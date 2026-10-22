#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import importlib.util
import math
import time
from datetime import datetime, timezone
from decimal import Decimal, ROUND_UP
from typing import Any, Dict, List
from dataclasses import dataclass, field

from analytics import hours_since_last_fill
from bingx_adapter import BingXBroker
from bybit_adapter import BybitBroker
from finam_adapter import FinamBroker
from schwab_adapter import SchwabBroker
from settings import SMART_EXECUTOR_PATH, env_float, env_int

# smart-inc: no fill within this window (or no history at all) = fresh start, multiplier 1.
SMART_INC_RESTART_HOURS = 36.0


def smart_inc_max_multiplier() -> int:
    """Potolok for the smart-inc multiplier from Settings (BingX section).

    Read per call so a Settings save (reload_env updates os.environ) applies
    without a server restart. Floor at 1: 0/invalid would mean "never trade".
    """
    return max(1, env_int('SMART_INC_MAX_MULTIPLIER', 36))


def smart_inc_multiplier(hours: float | None) -> int:
    """Multipler for smart-inc sizing from completed clock-hours since last fill.

    None (no fill history) or a gap >= SMART_INC_RESTART_HOURS means a fresh
    start → 1. Otherwise the hour count itself: 0 = another source already
    traded this ticker in the current clock hour → caller must skip the order.
    """
    if hours is None or hours >= SMART_INC_RESTART_HOURS:
        return 1
    return min(smart_inc_max_multiplier(), max(0, int(hours)))


def smart_inc_avg_tol(destination: Dict[str, Any]) -> float | None:
    """Price tolerance (absolute) for smart-inc add-to-position, from routing cell.

    Empty/missing → None = avg-add mode off (keep hours multiplier).
    """
    raw = destination.get('smartIncTol')
    if raw in (None, ''):
        return None
    try:
        v = float(str(raw).replace('%', '').strip())
    except Exception:
        return None
    return v if v > 0 else None


def smart_inc_required_add_qty(
    base_qty: float,
    avg_price: float,
    mark_price: float,
    direction: str,
    tol: float,
    max_hours: int | None = None,
) -> float | None:
    """Contracts to add so position avg is within `tol` of mark (internal math).

    LONG:  skip if A ≤ P+tol; else q ≥ Q*(A−P−tol)/tol  → new_avg ≤ P+tol
    SHORT: skip if A ≥ P−tol; else q ≥ Q*(P−tol−A)/tol  → new_avg ≥ P−tol
    Returns 0.0 if already in range (caller should skip), None on bad inputs.
    """
    if tol is None or tol <= 0:
        return None
    try:
        q0 = abs(float(base_qty))
        a = float(avg_price)
        p = float(mark_price)
    except (TypeError, ValueError):
        return None
    if q0 <= 0 or a <= 0 or p <= 0:
        return None
    d = str(direction or '').lower()
    if d == 'long':
        if a <= p + tol:
            return 0.0
        need = q0 * (a - p - tol) / tol
    elif d == 'short':
        if a >= p - tol:
            return 0.0
        need = q0 * (p - tol - a) / tol
    else:
        return None
    if need <= 0:
        return 0.0
    # Cap by hours-multiplier path if configured (same potolok).
    if max_hours is not None and max_hours > 0:
        need = min(need, float(q0) * float(max_hours))
    return need


def smart_inc_required_add_usdt(
    base_qty: float,
    avg_price: float,
    mark_price: float,
    direction: str,
    tol: float,
    max_hours: int | None = None,
) -> float | None:
    """USDT notional to add so avg is within `tol` of mark.

    N = M * (gap/tol − 1) where M = Q*P; same as contracts_need * P.
    Open/add orders are placed in USDT (quoteOrderQty) — this is the qty to send.
    """
    need_c = smart_inc_required_add_qty(base_qty, avg_price, mark_price, direction, tol, max_hours)
    if need_c is None:
        return None
    if need_c <= 0:
        return 0.0
    try:
        p = float(mark_price)
    except (TypeError, ValueError):
        return None
    if p <= 0:
        return None
    return need_c * p


def smart_inc_need_ok(need: float, destination: Dict[str, Any]) -> bool:
    """False when need is positive but below the instrument minQty."""
    if need is None or need <= 0:
        return False
    limits = (destination or {}).get('limits') or {}
    try:
        min_qty = float(limits.get('minQty') or 0)
    except Exception:
        min_qty = 0.0
    if min_qty > 0 and need < min_qty:
        return False
    return True


def smart_inc_apply_qty(quantity: Any, scaled: float) -> Any:
    """Normalize scaled qty back to int when the base was integer contracts."""
    if float(scaled).is_integer() and (isinstance(quantity, int) or (isinstance(quantity, float) and float(quantity).is_integer())):
        return int(scaled)
    return scaled


def smart_inc_open_qty(need_contracts: float, mark_price: float, open_qty_kind: str) -> float:
    """Convert avg_tol need (contracts) into the open/add order unit.

    BingX open path uses quoteOrderQty (USDT) when openQtyKind=usdt — passing
    the contract count as notional fills ~1/price of intended size and the
    average barely moves. Close/reduce stays in contracts (caller unchanged).
    """
    kind = str(open_qty_kind or '').lower()
    if kind in ('usdt', 'quote', 'quote_usdt', 'notional'):
        try:
            price = abs(float(mark_price or 0) or 0)
        except (TypeError, ValueError):
            price = 0.0
        if price <= 0:
            return float(need_contracts)
        return float(need_contracts) * price
    return float(need_contracts)


# Periodic BingX risk sync: correct margin only once terminal risk grew
# by this relative amount over the target (avoids micro-adjusting every cycle).
RISK_SYNC_GROWTH_RATIO = 1.30


def risk_sync_default_pct() -> float:
    """Target risk % for open BingX positions without a routing-cell riskPct.

    Read per call so a Settings save applies without restart. Missing key →
    field default 12; <=0 (explicit 0) → 0.0 = such positions are untouched.
    """
    value = env_float('RISK_SYNC_DEFAULT_PCT', 12.0)
    return value if value > 0 else 0.0


def risk_sync_should_adjust(current_risk_pct: float, target_pct: float) -> bool:
    """True when the terminal Risk field grew to target × RISK_SYNC_GROWTH_RATIO."""
    if target_pct <= 0 or current_risk_pct <= 0:
        return False
    # epsilon: 12.0 * 1.3 == 15.600000000000001, exact-threshold must still fire
    return current_risk_pct + 1e-9 >= target_pct * RISK_SYNC_GROWTH_RATIO


def risk_sync_target_isolated(current_risk_rate: float, current_isolated: float, notional: float, target_pct: float) -> float:
    """Isolated margin so terminal riskRate == target_pct% (maintenance conserved).

    maintenance ≈ current riskRate × current isolated (verified on live
    position); fallback 0.3% × notional when riskRate/isolated unavailable.
    """
    if target_pct <= 0:
        return 0.0
    maintenance = 0.0
    if current_risk_rate > 0 and current_isolated > 0:
        maintenance = current_risk_rate * current_isolated
    elif notional > 0:
        maintenance = 0.003 * abs(notional)
    if maintenance <= 0:
        return 0.0
    return maintenance / (float(target_pct) / 100.0)


def bingx_risk_pct_by_symbol(config: Dict[str, Any], broker: str = 'bingx') -> Dict[str, float]:
    """Routing-cell riskPct for one broker, keyed by normalized exchange symbol."""
    broker_l = str(broker or '').strip().lower()
    targets: Dict[str, float] = {}
    for route in (config or {}).get('routes') or []:
        if not isinstance(route, dict):
            continue
        for dest in route.get('destinations') or []:
            if not isinstance(dest, dict):
                continue
            if str(dest.get('broker') or '').strip().lower() != broker_l:
                continue
            symbol = str(dest.get('symbol') or '').strip()
            if not symbol:
                continue
            try:
                pct = float(str(dest.get('riskPct') or '').replace('%', '').strip())
            except Exception:
                continue
            if pct > 0:
                targets[symbol.replace('-', '').upper()] = pct
    return targets


@dataclass
class ExecutionResult:
    broker: str
    symbol: str
    status: str
    request: dict
    results: dict
    error: str | None = None
    fills: list = field(default_factory=list)
    category: str | None = None
    exchange: str | None = None
    qty: Any = None
    dry_run: bool = False

    def to_dict(self) -> dict:
        d: dict[str, Any] = {
            'broker': self.broker,
            'symbol': self.symbol,
            'request': self.request,
            'results': self.results,
        }
        if self.error:
            d['error'] = self.error
        if self.dry_run:
            d['dryRun'] = True
        if self.category:
            d['category'] = self.category
        if self.exchange:
            d['exchange'] = self.exchange
        if self.qty is not None:
            d['qty'] = self.qty
        return d


DESTINATION_TIMEOUT_SECONDS = 25

_smart_executor_cache = None
_smart_executor_mtime = 0.0


def _resolve_side(side_raw: str, signal_mode: str) -> tuple[str, str, str]:
    if side_raw in ('2long', 'long'):
        return 'buy', 'target-direction', 'long'
    elif side_raw in ('2short', 'short'):
        return 'sell', 'target-direction', 'short'
    return side_raw, signal_mode, ''


def _with_retry(fn, delays=(0.0, 0.7, 1.5, 3.0), predicate=None):
    last_error = None
    for delay in delays:
        if delay > 0:
            time.sleep(delay)
        try:
            result = fn()
            if predicate is None or predicate(result):
                return result
        except Exception as e:
            last_error = e
    return None


def load_smart_executor_module():
    global _smart_executor_cache, _smart_executor_mtime
    current_mtime = SMART_EXECUTOR_PATH.stat().st_mtime
    if _smart_executor_cache is not None and current_mtime == _smart_executor_mtime:
        return _smart_executor_cache
    spec = importlib.util.spec_from_file_location('smart_order_executor', SMART_EXECUTOR_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    _smart_executor_cache = module
    _smart_executor_mtime = current_mtime
    return module


async def _execute_via_workspace_executor(payload: Dict[str, Any], destination: Dict[str, Any]) -> Dict[str, Any]:
    module = load_smart_executor_module()

    broker = destination['broker']
    symbol = destination['symbol']
    exchange = destination['exchange']
    side_raw = str(destination.get('side', payload['side'])).strip().lower()
    signal_mode = str(destination.get('signalMode') or payload.get('signalMode') or '').strip().lower()
    target_direction = str(destination.get('targetDirection') or payload.get('targetDirection') or '').strip().lower()
    side, signal_mode, resolved_dir = _resolve_side(side_raw, signal_mode)
    if resolved_dir:
        target_direction = resolved_dir
    quantity = int(float(destination.get('qty', payload['qty'])))
    use_limit = str(destination.get('executionMode', 'maker')).lower() != 'market'
    request_payload = {
        'ticker': symbol,
        'side': side,
        'quantity': quantity,
        'exchange': exchange,
        'use_limit': use_limit,
        'signalMode': signal_mode,
        'targetDirection': target_direction,
    }

    try:
        results = await module.handle_tradingview_webhook(request_payload, broker=broker)
        return {
            'broker': broker,
            'symbol': symbol,
            'exchange': exchange,
            'qty': quantity,
            'request': request_payload,
            'results': results,
        }
    except Exception as e:
        return {
            'broker': broker,
            'symbol': symbol,
            'exchange': exchange,
            'qty': quantity,
            'request': request_payload,
            'error': str(e),
            'results': {'error': str(e)},
        }


async def _execute_bybit(payload: Dict[str, Any], destination: Dict[str, Any]) -> Dict[str, Any]:
    broker = destination['broker']
    symbol = destination['symbol']
    side_raw = str(destination.get('side', payload['side'])).strip().lower()
    signal_mode = str(destination.get('signalMode') or payload.get('signalMode') or 'step-side').strip().lower()
    target_direction = ''
    side, signal_mode, resolved_dir = _resolve_side(side_raw, signal_mode)
    if resolved_dir:
        target_direction = resolved_dir
    elif signal_mode == 'target-direction' and side_raw in ('long', 'short'):
        target_direction = side_raw
        side = 'buy' if target_direction == 'long' else 'sell'

    quantity = destination.get('qty', payload.get('qty'))
    qty_kind = str(destination.get('qtyKind') or payload.get('qtyKind') or 'contracts').lower()
    if signal_mode == 'target-direction':
        if quantity in (None, '') and payload.get('qty') not in (None, ''):
            quantity = payload.get('qty')
        qty_kind = str(destination.get('qtyKind') or payload.get('qtyKind') or 'usdt').lower()
    category = destination.get('category', 'linear')
    execution_mode = destination.get('executionMode', destination.get('mode', 'market'))
    reduce_only = bool(destination.get('reduceOnly', False))
    dry_run = bool(destination.get('dryRun', False) or payload.get('dryRun', False))

    # smart-inc: qty × hours since last fill (new open / reverse).
    # If smartIncTol is set and we are ADDING to an existing same-direction
    # position, switch to avg-price tolerance sizing (skip if already in range).
    smart_inc_applied = None
    smart_inc_skip = False
    smart_inc_tol = smart_inc_avg_tol(destination)
    if destination.get('smartInc') and signal_mode == 'target-direction':
        try:
            base_qty = float(quantity)
        except (TypeError, ValueError):
            base_qty = None
        if base_qty is not None:
            # Same-hour dedup on exchange symbol (NG1! + NATURAL… → one fill).
            hours = hours_since_last_fill(broker, symbol)
            multiplier = smart_inc_multiplier(hours)
            if multiplier == 0:
                smart_inc_skip = True
                smart_inc_applied = {
                    'mode': 'hours',
                    'baseQty': base_qty,
                    'hoursSinceLastFill': None if hours is None else round(hours, 3),
                    'multiplier': 0,
                    'qty': quantity,
                }
            # Detect add-to-position early when tol mode is on.
            # Same-hour skip already set → do not size further.
            add_mode = False
            pos_qty = 0.0
            pos_avg = 0.0
            pos_mark = 0.0
            if smart_inc_tol is not None and not smart_inc_skip:
                try:
                    _probe = BybitBroker(testnet=bool(destination.get('testnet', False)))
                    _prows = (((_probe.get_positions(category=category, symbol=symbol) or {}).get('result') or {}).get('list') or [])
                    for _row in _prows:
                        if not isinstance(_row, dict):
                            continue
                        if str(_row.get('symbol') or '').upper() != str(symbol).upper():
                            continue
                        _side = str(_row.get('side') or '').strip().lower()
                        _sz = abs(float(_row.get('size') or 0) or 0)
                        if _sz <= 0:
                            continue
                        _want = 'buy' if target_direction == 'long' else 'sell'
                        if _side == _want:
                            add_mode = True
                            pos_qty = _sz
                            for _k in ('avgPrice', 'entryPrice'):
                                try:
                                    pos_avg = abs(float(_row.get(_k) or 0) or 0)
                                    if pos_avg > 0:
                                        break
                                except Exception:
                                    pass
                            for _k in ('markPrice', 'markIV'):
                                try:
                                    pos_mark = abs(float(_row.get(_k) or 0) or 0)
                                    if pos_mark > 0:
                                        break
                                except Exception:
                                    pass
                            if not pos_mark:
                                pos_mark = pos_avg
                            break
                except Exception:
                    add_mode = False
            if add_mode and pos_qty > 0 and smart_inc_tol is not None and not smart_inc_skip:
                need = smart_inc_required_add_qty(pos_qty, pos_avg, pos_mark or pos_avg, target_direction, smart_inc_tol)
                if need is not None and need <= 1e-12:
                    # Avg already within tol: do not skip — fall through to hours (cell × hours).
                    add_mode = False
                elif need is not None:
                    # Bybit places CONTRACTS — no USDT round-trip (minQty=0.002).
                    # Snap need to qtyStep first (avg math → 0.00777 is illegal).
                    from decimal import Decimal as _D, ROUND_UP as _RU
                    try:
                        _need_q = _D(str(need)).quantize(_D('0.001'), rounding=_RU)
                        if _need_q > 0:
                            need = float(_need_q)
                    except Exception:
                        pass
                    limits = (destination or {}).get('limits') or {}
                    min_qty = 0.0
                    try:
                        min_qty = float(limits.get('minQty') or 0)
                    except (TypeError, ValueError):
                        min_qty = 0.0
                    if min_qty <= 0 and str(symbol).upper() == 'XAUTUSDT':
                        min_qty = 0.002
                    min_usdt = 0.0
                    try:
                        min_usdt = float(limits.get('minUsdt') or 0)
                    except (TypeError, ValueError):
                        min_usdt = 0.0
                    if min_usdt <= 0 and str(symbol).upper() == 'XAUTUSDT':
                        min_usdt = 5.0
                    mark_for_min = pos_mark or pos_avg
                    if min_qty > 0 and need < min_qty:
                        smart_inc_skip = True
                        smart_inc_applied = {
                            'mode': 'avg_tol',
                            'tol': smart_inc_tol,
                            'posQty': pos_qty,
                            'avgPrice': pos_avg,
                            'markPrice': pos_mark,
                            'requiredAddQty': need,
                            'reason': 'need_below_min_qty',
                            'qty': quantity,
                        }
                    elif min_usdt > 0 and mark_for_min > 0 and need * mark_for_min < min_usdt:
                        smart_inc_skip = True
                        smart_inc_applied = {
                            'mode': 'avg_tol',
                            'tol': smart_inc_tol,
                            'posQty': pos_qty,
                            'avgPrice': pos_avg,
                            'markPrice': pos_mark,
                            'requiredAddQty': need,
                            'reason': 'need_below_min_usdt',
                            'qty': quantity,
                        }
                    else:
                        quantity = need  # contracts — Bybit wire unit (already step-snapped)
                        smart_inc_applied = {
                            'mode': 'avg_tol',
                            'tol': smart_inc_tol,
                            'posQty': pos_qty,
                            'avgPrice': pos_avg,
                            'markPrice': pos_mark,
                            'requiredAddQty': need,
                            'qty': quantity,
                            'qtyKind': 'contracts',
                        }
                else:
                    # avg/mark unavailable → fall back to hours
                    add_mode = False
            if not smart_inc_skip and not (smart_inc_applied and smart_inc_applied.get('mode') == 'avg_tol'):
                scaled = base_qty * multiplier
                quantity = int(scaled) if float(scaled).is_integer() else scaled
                if smart_inc_applied is None:
                    smart_inc_applied = {}
                smart_inc_applied.update({
                    'mode': 'hours',
                    'baseQty': base_qty,
                    'hoursSinceLastFill': None if hours is None else round(hours, 3),
                    'multiplier': multiplier,
                    'qty': quantity,
                })

    # avg_tol add must fill at ~current mark; maker limit may fill late/partial/wrong price.
    if smart_inc_applied and smart_inc_applied.get('mode') == 'avg_tol':
        execution_mode = 'market'

    risk_pct_raw = destination.get('riskPct') if 'riskPct' in destination else payload.get('riskPct')
    risk_pct = None
    try:
        if risk_pct_raw not in (None, ''):
            risk_pct = float(str(risk_pct_raw).replace('%', '').strip())
    except Exception:
        risk_pct = None
    risk_control_enabled = bool(risk_pct is not None and risk_pct > 0)

    request_payload = {
        'broker': broker,
        'symbol': symbol,
        'side': side_raw,
        'qty': quantity,
        'qtyKind': qty_kind,
        'category': category,
        'executionMode': execution_mode,
        'reduceOnly': reduce_only,
        'signalMode': signal_mode,
        'targetDirection': target_direction,
    }
    if smart_inc_applied is not None:
        request_payload['smartInc'] = smart_inc_applied
    if risk_control_enabled:
        request_payload['riskPct'] = risk_pct

    if smart_inc_skip:
        _reason = 'smart_inc_avg_in_range'
        if smart_inc_applied and smart_inc_applied.get('reason') in ('need_below_min_qty', 'need_below_min_usdt'):
            _reason = 'smart_inc_need_below_min'
        elif smart_inc_applied and smart_inc_applied.get('mode') == 'hours':
            _reason = 'smart_inc_same_hour'
        return {
            'broker': broker,
            'symbol': symbol,
            'category': category,
            'qty': quantity,
            'request': request_payload,
            'skipped': True,
            'skipReason': _reason,
            'results': {
                'skipped': True,
                'reason': _reason,
                'smartInc': smart_inc_applied,
            },
        }

    if dry_run:
        return {
            'broker': broker,
            'symbol': symbol,
            'category': category,
            'dryRun': True,
            'request': request_payload,
            'wouldPlace': request_payload,
        }

    try:
        def _run() -> Dict[str, Any]:
            client = BybitBroker(testnet=bool(destination.get('testnet', False)))
            position_idx = destination.get('positionIdx')
            target_mode_close_then_open = False
            close_order_result: Dict[str, Any] | None = None

            # Nearest book level + IOC + repost remainder (same as BingX, all brokers).
            def _bybit_walk_ioc(side_str: str, qty_val: Any, kind: str, reduce_only_flag: bool, pos_idx: Any) -> Dict[str, Any]:
                remaining = qty_val
                kind_cur = kind
                order_result: Dict[str, Any] = {}
                attempts: List[Dict[str, Any]] = []
                request_payload['iocAttempts'] = attempts
                for attempt in range(6):
                    nearest = client.get_nearest_price(symbol, side_str, category)
                    if nearest in (None, '', '0', '0.0'):
                        # Fallback: market (immediate, no wait).
                        order_result = client.place_order(
                            symbol=symbol, side=side_str, qty=remaining, category=category,
                            order_type='Market', reduce_only=reduce_only_flag,
                            position_idx=pos_idx, qty_kind=kind_cur,
                        )
                        attempts.append({'attempt': attempt + 1, 'fallback': 'market', 'result': order_result})
                        break
                    order_result = client.place_order(
                        symbol=symbol, side=side_str, qty=remaining, category=category,
                        order_type='Limit', price=nearest, reduce_only=reduce_only_flag,
                        position_idx=pos_idx, qty_kind=kind_cur,
                    )
                    ret = (order_result or {}).get('retCode')
                    order_id = (((order_result or {}).get('result') or {}) or {}).get('orderId')
                    att = {
                        'attempt': attempt + 1,
                        'price': nearest,
                        'qty': remaining,
                        'qtyKind': kind_cur,
                        'orderId': order_id,
                        'result': order_result,
                    }
                    attempts.append(att)
                    if ret not in (None, 0, '0') or not order_id:
                        break
                    # Poll fill briefly.
                    status = 'NEW'
                    executed = Decimal('0')
                    for wait in (0.15, 0.3, 0.5):
                        time.sleep(wait)
                        st = client.get_order_status(symbol, str(order_id), category=category)
                        row = (((st or {}).get('result') or {}).get('list') or [None] or [None])[0] or {}
                        status = str(row.get('orderStatus') or row.get('status') or '').upper()
                        try:
                            executed = Decimal(str(row.get('cumExecQty') or row.get('executedQty') or '0'))
                        except Exception:
                            executed = Decimal('0')
                        if status in ('FILLED', 'CANCELLED', 'CANCELED', 'PARTIALLY_FILLED', 'DEALTH', 'REJECTED'):
                            break
                    att['finalStatus'] = status
                    att['executedQty'] = str(executed)
                    if status == 'FILLED':
                        att['remainingQty'] = '0'
                        break
                    # Remaining: qty minus executed (contracts). For usdt, convert executed→usdt.
                    try:
                        qty_dec = Decimal(str(remaining))
                    except Exception:
                        qty_dec = Decimal('0')
                    if str(kind_cur).lower() in ('usdt', 'quote', 'quote_usdt', 'notional'):
                        try:
                            px_dec = Decimal(str(nearest))
                            exec_usdt = executed * px_dec if px_dec > 0 else Decimal('0')
                        except Exception:
                            exec_usdt = Decimal('0')
                        rem = qty_dec - exec_usdt
                    else:
                        rem = qty_dec - executed
                    if rem <= 0:
                        att['remainingQty'] = '0'
                        break
                    # Round remaining up to instrument step for next IOC.
                    try:
                        rules = client.get_qty_rules(symbol, category)
                        step = Decimal(str(rules.get('qtyStep') or '0.001'))
                        if str(kind_cur).lower() not in ('usdt', 'quote', 'quote_usdt', 'notional'):
                            from decimal import ROUND_UP as _RU
                            rem = rem.quantize(step, rounding=_RU) if rem > 0 else Decimal('0')
                    except Exception:
                        pass
                    if rem <= 0:
                        att['remainingQty'] = '0'
                        break
                    remaining = float(rem) if str(kind_cur).lower() not in ('usdt', 'quote', 'quote_usdt', 'notional') else format(rem, 'f').rstrip('0').rstrip('.')
                    att['remainingQty'] = str(remaining)
                    att['repost'] = True
                return order_result


            if signal_mode == 'target-direction':
                positions = client.get_positions(category=category, symbol=symbol)
                request_payload['positionListRaw'] = positions
                rows = (((positions or {}).get('result') or {}).get('list') or [])
                long_size = 0.0
                short_size = 0.0
                long_idx = 0
                short_idx = 0
                observed_position_indexes = set()
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    if str(row.get('symbol') or '').upper() != str(symbol).upper():
                        continue
                    try:
                        size = abs(float(row.get('size') or 0))
                    except Exception:
                        size = 0.0
                    try:
                        row_idx = int(row.get('positionIdx') or 0)
                    except Exception:
                        row_idx = 0
                    observed_position_indexes.add(row_idx)
                    row_side = str(row.get('side') or '').strip().lower()
                    if row_side == 'buy' and size > 0:
                        long_size += size
                        if row_idx:
                            long_idx = row_idx
                    elif row_side == 'sell' and size > 0:
                        short_size += size
                        if row_idx:
                            short_idx = row_idx
                hedged_mode_detected = any(idx in (1, 2) for idx in observed_position_indexes)
                request_payload['positionSnapshot'] = {
                    'longSize': long_size,
                    'shortSize': short_size,
                    'longPositionIdx': long_idx,
                    'shortPositionIdx': short_idx,
                    'observedPositionIdxs': sorted(observed_position_indexes),
                    'positionMode': 'hedged' if hedged_mode_detected else 'one-way',
                }

                desired_side = 'buy' if target_direction == 'long' else 'sell'
                if target_direction == 'long' and short_size > 0:
                    target_mode_close_then_open = True
                    close_qty = short_size
                    close_side = 'buy'
                    close_position_idx = (short_idx or 2) if hedged_mode_detected else None
                elif target_direction == 'short' and long_size > 0:
                    target_mode_close_then_open = True
                    close_qty = long_size
                    close_side = 'sell'
                    close_position_idx = (long_idx or 1) if hedged_mode_detected else None
                else:
                    close_qty = 0.0
                    close_side = ''
                    close_position_idx = None

                if target_mode_close_then_open:
                    request_payload['nettingAction'] = 'target_direction_close_opposite_then_open_target'
                    request_payload['closeOpposite'] = {
                        'side': close_side,
                        'qty': close_qty,
                        'positionIdx': close_position_idx,
                    }
                    close_order_result = _bybit_walk_ioc(
                        close_side, close_qty, 'contracts', True, close_position_idx,
                    )
                    request_payload['closeOppositeResult'] = close_order_result
                    close_ret = (close_order_result or {}).get('retCode')
                    if close_ret not in (None, 0, '0'):
                        return close_order_result
                    time.sleep(0.35)
                    request_payload['postClosePositionListRaw'] = client.get_positions(category=category, symbol=symbol)
                else:
                    request_payload['nettingAction'] = 'target_direction_open_or_increase_target'

                if position_idx in (None, ''):
                    position_idx = (1 if desired_side == 'buy' else 2) if hedged_mode_detected else None
                request_payload['openPositionIdx'] = None if position_idx in (None, '') else int(position_idx)

            # Pre-trade risk: ensure isolated margin so riskRate ≈ riskPct%
            # (same intent as BingX; Bybit fields: positionMM / positionBalance).
            margin_idx = None if position_idx in (None, '') else int(position_idx)
            if risk_control_enabled and not reduce_only and str(side).lower() in ('buy', 'sell'):
                wallet = client.get_wallet_balance()
                equity = _bybit_account_equity(wallet)
                available = _bybit_account_available(wallet)
                positions_before = client.get_positions(category=category, symbol=symbol)
                before_pos = _bybit_pick_position(positions_before, symbol, target_direction or side)
                before_iso = _bybit_position_isolated(before_pos)
                before_rr = _bybit_position_risk_rate(before_pos)
                before_qty = _bybit_position_qty(before_pos)
                mark_price = 0.0
                for key in ('markPrice', 'entryPrice', 'avgPrice'):
                    try:
                        mark_price = abs(float(before_pos.get(key) or 0))
                        if mark_price > 0:
                            break
                    except Exception:
                        pass
                try:
                    incoming_qty = abs(float(quantity or 0))
                except Exception:
                    incoming_qty = 0.0
                expected_final_qty = max(0.0, before_qty + incoming_qty)
                expected_notional = mark_price * expected_final_qty
                unrealized = 0.0
                try:
                    unrealized = float(before_pos.get('unrealisedPnl') or before_pos.get('unrealisedPnl') or 0)
                except Exception:
                    unrealized = 0.0
                allowed_loss = max(0.0, equity * (risk_pct / 100.0))
                target_isolated_pre = max(0.0, allowed_loss + unrealized)
                target_margin_pre = min(expected_notional, target_isolated_pre) if expected_notional > 0 else target_isolated_pre
                request_payload['riskControl'] = {
                    'mode': 'bybit',
                    'equity': equity,
                    'available': available,
                    'allowedLoss': allowed_loss,
                    'beforeIsolated': before_iso,
                    'beforeRiskRate': before_rr,
                    'beforeQty': before_qty,
                    'incomingQty': incoming_qty,
                    'expectedFinalQty': expected_final_qty,
                    'expectedNotional': expected_notional,
                    'targetIsolatedPre': target_margin_pre,
                }
                add = target_margin_pre - before_iso
                if add > 1e-8:
                    if before_iso <= 1e-8 and before_qty <= 0:
                        # Opening NEW isolated position: only set isolated mode here.
                        # add_margin requires an open position (Bybit: "Margin cannot
                        # be set without open position") — top-up happens post-trade.
                        client.set_margin_mode(symbol, trade_mode=1, position_idx=margin_idx)
                    elif before_qty > 0:
                        # Existing position: top-up before placing.
                        add = min(add, max(0.0, available))
                        if add > 1e-8:
                            request_payload['riskControl']['preTradeAddMargin'] = add
                            add_resp = client.add_margin(symbol, add, category=category, position_idx=margin_idx)
                            request_payload['riskControl']['preTradeAddMarginResult'] = add_resp
                            ret = (add_resp or {}).get('retCode')
                            if ret not in (None, 0, '0'):
                                msg = str((add_resp or {}).get('retMsg') or '')
                                return {
                                    **{k: request_payload[k] for k in ('broker', 'symbol', 'side', 'qty') if k in request_payload},
                                    'error': f'pre_trade_margin_failed: {msg or ret}',
                                    'results': add_resp,
                                }

            # Bybit open/add: place CONTRACTS as sized (hours/avg_tol) — no
            # contracts→USDT→contracts round-trip (precision dies vs minQty=0.002).
            # Close/reduce is contracts too. NOTE: qty_kind is closed over from
            # _execute_bybit — do NOT assign it here (nested _run UnboundLocalError).
            order_qty_kind = qty_kind
            order_qty = quantity

            if str(execution_mode).lower() == 'limit':
                price = destination.get('price') or payload.get('price')
                request_payload['price'] = price
                if price not in (None, ''):
                    order_result = client.place_order(
                        symbol=symbol,
                        side=side,
                        qty=order_qty,
                        category=category,
                        order_type='Limit',
                        price=price,
                        reduce_only=reduce_only,
                        position_idx=margin_idx,
                        qty_kind=order_qty_kind,
                    )
                else:
                    order_result = _bybit_walk_ioc(side, order_qty, order_qty_kind, reduce_only, margin_idx)
            else:
                # Market path still walks nearest+IOC when possible (no GTC wait).
                order_result = _bybit_walk_ioc(side, order_qty, order_qty_kind, reduce_only, margin_idx)

            # Post-trade: if position already open and risk above target, top-up margin.
            if risk_control_enabled and not reduce_only:
                try:
                    time.sleep(0.25)
                    wallet2 = client.get_wallet_balance()
                    positions_after = client.get_positions(category=category, symbol=symbol)
                    after_pos = _bybit_pick_position(positions_after, symbol, target_direction or side)
                    after_iso = _bybit_position_isolated(after_pos)
                    after_rr = _bybit_position_risk_rate(after_pos)
                    after_mm = _bybit_position_maintenance(after_pos)
                    if after_rr > 0 and risk_pct > 0:
                        target_iso = risk_sync_target_isolated(after_rr, after_iso, _bybit_position_notional(after_pos), risk_pct)
                        delta = target_iso - after_iso
                        available2 = _bybit_account_available(wallet2)
                        request_payload['riskControl']['postTrade'] = {
                            'riskRate': after_rr,
                            'isolated': after_iso,
                            'targetIsolated': target_iso,
                            'delta': delta,
                        }
                        if delta > 1e-8 and available2 > 1e-8:
                            add2 = min(delta, available2)
                            post = client.add_margin(symbol, add2, category=category, position_idx=margin_idx)
                            request_payload['riskControl']['postTradeAddMargin'] = add2
                            request_payload['riskControl']['postTradeAddMarginResult'] = post
                            try:
                                verify = client.get_positions(category=category, symbol=symbol)
                                vpos = _bybit_pick_position(verify, symbol, target_direction or side)
                                request_payload['riskControl']['riskRateAfterAdjust'] = _bybit_position_risk_rate(vpos)
                            except Exception:
                                pass
                except Exception as exc:
                    request_payload['riskControl']['postTradeError'] = str(exc)

            return order_result

        result = await asyncio.to_thread(_run)

        return {
            'broker': broker,
            'symbol': symbol,
            'category': category,
            'qty': quantity,
            'request': request_payload,
            'results': result,
        }
    except Exception as e:
        return {
            'broker': broker,
            'symbol': symbol,
            'category': category,
            'qty': quantity,
            'request': request_payload,
            'error': str(e),
            'results': {'error': str(e)},
        }


def _bybit_account_equity(wallet: Dict[str, Any]) -> float:
    rows = ((wallet or {}).get('result') or {}).get('list') or []
    for row in rows:
        if not isinstance(row, dict):
            continue
        for key in ('totalEquity', 'walletBalance', 'totalWalletBalance'):
            value = row.get(key)
            if value not in (None, ''):
                try:
                    return abs(float(value))
                except Exception:
                    pass
    return 0.0


def _bybit_account_available(wallet: Dict[str, Any]) -> float:
    rows = ((wallet or {}).get('result') or {}).get('list') or []
    best = 0.0
    for row in rows:
        if not isinstance(row, dict):
            continue
        for key in ('totalAvailableBalance', 'availableBalance', 'availableToWithdraw'):
            value = row.get(key)
            if value not in (None, ''):
                try:
                    best = max(best, abs(float(value)))
                except Exception:
                    pass
        # UNIFIED: per-coin free balance (USDT first, then any positive)
        coins = row.get('coin') or []
        if isinstance(coins, list):
            usdt_free = 0.0
            any_free = 0.0
            for coin in coins:
                if not isinstance(coin, dict):
                    continue
                for key in ('availableToWithdraw', 'availableBalance', 'availableToTransfer', 'equity'):
                    value = coin.get(key)
                    if value in (None, ''):
                        continue
                    try:
                        v = abs(float(value))
                    except Exception:
                        continue
                    coin_name = str(coin.get('coin') or '').upper()
                    if coin_name == 'USDT':
                        usdt_free = max(usdt_free, v)
                    any_free = max(any_free, v)
                    break
            best = max(best, usdt_free, any_free)
    return best


def _bybit_position_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = ((payload or {}).get('result') or {}).get('list') or []
    return [r for r in rows if isinstance(r, dict)]


def _bybit_pick_position(payload: Dict[str, Any], symbol: str, prefer: str = '') -> Dict[str, Any]:
    """First open position for symbol; prefer long/short when given."""
    sym = str(symbol or '').upper()
    prefer_l = str(prefer or '').strip().lower()
    candidates = []
    for row in _bybit_position_rows(payload):
        if str(row.get('symbol') or '').upper() != sym:
            continue
        try:
            size = abs(float(row.get('size') or 0))
        except Exception:
            size = 0.0
        if size <= 0:
            continue
        candidates.append(row)
    if not candidates:
        # empty row with symbol (open new)
        for row in _bybit_position_rows(payload):
            if str(row.get('symbol') or '').upper() == sym:
                return row
        return {}
    want = 'buy' if prefer_l == 'long' else ('sell' if prefer_l == 'short' else '')
    if want:
        for row in candidates:
            if str(row.get('side') or '').strip().lower() == want:
                return row
    return candidates[0]


def _bybit_position_qty(row: Dict[str, Any]) -> float:
    try:
        return abs(float((row or {}).get('size') or 0))
    except Exception:
        return 0.0


def _bybit_position_isolated(row: Dict[str, Any]) -> float:
    for key in ('positionBalance', 'isolated', 'positionIM'):
        value = (row or {}).get(key)
        if value in (None, ''):
            continue
        try:
            v = abs(float(value))
            if v > 0:
                return v
        except Exception:
            pass
    return 0.0


def _bybit_position_maintenance(row: Dict[str, Any]) -> float:
    for key in ('positionMM', 'mm'):
        value = (row or {}).get(key)
        if value in (None, ''):
            continue
        try:
            v = abs(float(value))
            if v > 0:
                return v
        except Exception:
            pass
    return 0.0


def _bybit_position_notional(row: Dict[str, Any]) -> float:
    for key in ('positionValue', 'notional'):
        value = (row or {}).get(key)
        if value in (None, ''):
            continue
        try:
            v = abs(float(value))
            if v > 0:
                return v
        except Exception:
            pass
    try:
        return abs(float((row or {}).get('size') or 0)) * abs(float((row or {}).get('avgPrice') or 0))
    except Exception:
        return 0.0


def _bybit_position_risk_rate(row: Dict[str, Any]) -> float:
    """riskRate ≈ positionMM / isolated (positionBalance). 0 if unknown."""
    if not row:
        return 0.0
    # explicit field if present
    for key in ('riskRate', 'positionIM_rate', 'im_rate'):
        value = row.get(key)
        if value in (None, ''):
            continue
        try:
            v = float(value)
            if v > 0:
                # BingX-style riskRate is already a fraction (0.12 = 12%)
                return v if v < 1.0 else v / 100.0
        except Exception:
            pass
    mm = _bybit_position_maintenance(row)
    iso = _bybit_position_isolated(row)
    if mm > 0 and iso > 0:
        return mm / iso
    # fallback: 0.5% of notional / isolated (common linear MM rate ballpark)
    notional = _bybit_position_notional(row)
    if notional > 0 and iso > 0:
        return (0.005 * notional) / iso
    return 0.0


def _bingx_account_equity(balance_payload: Dict[str, Any]) -> float:
    rows = (balance_payload or {}).get('data') or []
    if isinstance(rows, dict):
        rows = [rows]
    for row in rows:
        if not isinstance(row, dict):
            continue
        asset = str(row.get('asset') or '').upper()
        if asset and asset != 'USDT':
            continue
        for key in ('equity', 'balance', 'availableMargin'):
            value = row.get(key)
            if value not in (None, ''):
                try:
                    return float(value)
                except Exception:
                    pass
    return 0.0


def _bingx_account_available(balance_payload: Dict[str, Any]) -> float:
    rows = (balance_payload or {}).get('data') or []
    if isinstance(rows, dict):
        rows = [rows]
    for row in rows:
        if not isinstance(row, dict):
            continue
        asset = str(row.get('asset') or '').upper()
        if asset and asset != 'USDT':
            continue
        for key in ('availableMargin', 'availableBalance', 'available'):
            value = row.get(key)
            if value not in (None, ''):
                try:
                    return float(value)
                except Exception:
                    pass
    return 0.0


def _bingx_account_unrealized(balance_payload: Dict[str, Any]) -> float:
    rows = (balance_payload or {}).get('data') or []
    if isinstance(rows, dict):
        rows = [rows]
    for row in rows:
        if not isinstance(row, dict):
            continue
        asset = str(row.get('asset') or '').upper()
        if asset and asset != 'USDT':
            continue
        for key in ('unrealizedProfit', 'unrealizedPnl', 'unrealizedProfitLoss'):
            value = row.get(key)
            if value not in (None, ''):
                try:
                    return float(value)
                except Exception:
                    pass
    return 0.0


def _bingx_api_ok(resp: Any) -> bool:
    if not isinstance(resp, dict):
        return False
    code = resp.get('code')
    return code in (None, 0, '0', '000000')


def _bingx_position_isolated(pos: Dict[str, Any]) -> float:
    for key in ('margin', 'positionMargin', 'isolatedMargin'):
        if pos and pos.get(key) not in (None, ''):
            try:
                return abs(float(pos.get(key)))
            except Exception:
                pass
    return 0.0


def _bingx_target_isolated_for_notional(notional: float, risk_pct: float) -> float:
    """Isolated margin so terminal riskRate = riskPct% (riskRate ≈ 0.3%×notional / isolated)."""
    if notional <= 0 or risk_pct is None or risk_pct <= 0:
        return 0.0
    maintenance = 0.003 * abs(notional)
    return maintenance / (float(risk_pct) / 100.0)


def _bingx_prepared_notional(prepared: Dict[str, Any]) -> float:
    """USDT notional of a prepared order: quoteOrderQty is already USDT (no mark×)."""
    quote = prepared.get('quoteOrderQty')
    if quote not in (None, ''):
        try:
            return abs(float(quote))
        except (TypeError, ValueError):
            return 0.0
    try:
        qty = abs(float(prepared.get('quantity') or 0))
        price = abs(float(prepared.get('price') or 0))
    except (TypeError, ValueError):
        return 0.0
    if str(prepared.get('qtyKind') or '').lower() in ('usdt', 'quote', 'quote_usdt', 'notional'):
        return qty
    return qty * price


def _bingx_extract_position(positions_payload: Dict[str, Any], symbol: str, fallback_side: str = '') -> Dict[str, Any]:
    rows = (positions_payload or {}).get('data') or []
    if isinstance(rows, dict):
        rows = [rows]
    symbol_norm = str(symbol or '').replace('-', '').upper()
    fallback_side = str(fallback_side or '').upper()
    best = {}
    best_qty = -1.0
    for row in rows:
        if not isinstance(row, dict):
            continue
        row_symbol = str(row.get('symbol') or '').replace('-', '').upper()
        if row_symbol != symbol_norm:
            continue
        row_side = str(row.get('positionSide') or row.get('side') or '').upper()
        qty_value = None
        for key in ('positionAmt', 'positionQty', 'availableAmt', 'quantity', 'positionSize'):
            value = row.get(key)
            if value not in (None, ''):
                try:
                    qty_value = abs(float(value))
                    break
                except Exception:
                    pass
        if qty_value is None:
            continue
        score = qty_value
        if fallback_side and row_side == fallback_side:
            score += 10**9
        if score > best_qty:
            best_qty = score
            best = row
    return best


def _bingx_position_qty(position: Dict[str, Any]) -> float:
    for key in ('positionAmt', 'positionQty', 'availableAmt', 'quantity', 'positionSize'):
        value = (position or {}).get(key)
        if value not in (None, ''):
            try:
                return abs(float(value))
            except Exception:
                pass
    return 0.0


def _bingx_position_rows(positions_payload: Dict[str, Any], symbol: str) -> List[Dict[str, Any]]:
    rows = (positions_payload or {}).get('data') or []
    if isinstance(rows, dict):
        rows = [rows]
    symbol_norm = str(symbol or '').replace('-', '').upper()
    matched: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        row_symbol = str(row.get('symbol') or '').replace('-', '').upper()
        if row_symbol != symbol_norm:
            continue
        matched.append(row)
    return matched


def _bingx_position_buckets(positions_payload: Dict[str, Any], symbol: str) -> Dict[str, Decimal]:
    rows = _bingx_position_rows(positions_payload, symbol)
    buckets = {'LONG': Decimal('0'), 'SHORT': Decimal('0')}
    for row in rows:
        row_side = str(row.get('positionSide') or row.get('side') or '').upper()
        if row_side not in ('LONG', 'SHORT'):
            continue
        try:
            buckets[row_side] += Decimal(str(_bingx_position_qty(row)))
        except Exception:
            pass
    return buckets


def _bingx_extract_order_row(payload: Dict[str, Any]) -> Dict[str, Any]:
    data = (payload or {}).get('data') or {}
    if isinstance(data, dict):
        if isinstance(data.get('order'), dict):
            return data.get('order') or {}
        return data
    return {}


def _bingx_order_id(order_row: Dict[str, Any]) -> str:
    return str(order_row.get('orderID') or order_row.get('orderId') or '').strip()


def _bingx_order_status(order_row: Dict[str, Any]) -> str:
    status = str(order_row.get('status') or '').strip().upper()
    # BingX uses British CANCELLED; older checks used CANCELED.
    if status == 'CANCELLED':
        return 'CANCELED'
    return status


def _safe_decimal(value: Any, default: str = '0') -> Decimal:
    try:
        text = str(value).strip()
        if not text:
            return Decimal(default)
        return Decimal(text)
    except Exception:
        return Decimal(default)


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')


def _bingx_order_executed_qty(order_row: Dict[str, Any]) -> Decimal:
    for key in ('executedQty', 'cumExecQty', 'filledQty'):
        value = order_row.get(key)
        if value not in (None, ''):
            try:
                text = str(value).strip()
                if not text:
                    continue
                return Decimal(text)
            except Exception:
                continue
    return Decimal('0')


def _bingx_order_avg_price(order_row: Dict[str, Any], fallback_price: Any = None) -> Decimal:
    for key in ('avgPrice', 'avgFilledPrice', 'priceAvg', 'fillPrice', 'executedPrice', 'price'):
        value = order_row.get(key)
        if value not in (None, '', '0'):
            dec = _safe_decimal(value)
            if dec > 0:
                return dec
    fallback = _safe_decimal(fallback_price)
    if fallback > 0:
        return fallback
    return Decimal('0')


def _bingx_order_commission(order_row: Dict[str, Any]) -> Decimal:
    for key in ('commission', 'fee', 'cumCommission', 'cumFee'):
        value = order_row.get(key)
        if value not in (None, ''):
            dec = _safe_decimal(value)
            if dec != 0:
                return abs(dec)
    return Decimal('0')


def _bingx_order_commission_currency(order_row: Dict[str, Any]) -> str:
    for key in ('commissionAsset', 'feeAsset', 'commissionCurrency', 'feeCurrency'):
        value = str(order_row.get(key) or '').strip()
        if value:
            return value
    return ''


def _bingx_order_snapshot(order_row: Dict[str, Any], fallback_price: Any = None, observed_at: str = '') -> Dict[str, Any]:
    return {
        'observedAt': observed_at or _utcnow_iso(),
        'status': _bingx_order_status(order_row),
        'executedQty': str(_bingx_order_executed_qty(order_row)),
        'avgPrice': str(_bingx_order_avg_price(order_row, fallback_price)),
        'commission': str(_bingx_order_commission(order_row)),
        'commissionCurrency': _bingx_order_commission_currency(order_row),
    }


def _bingx_build_fill_parts(snapshots: List[Dict[str, Any]], fallback_price: Any = None) -> List[Dict[str, Any]]:
    fills: List[Dict[str, Any]] = []
    prev_qty = Decimal('0')
    prev_commission = Decimal('0')
    seq = 0
    for snapshot in snapshots:
        if not isinstance(snapshot, dict):
            continue
        current_qty = _safe_decimal(snapshot.get('executedQty'))
        current_commission = abs(_safe_decimal(snapshot.get('commission')))
        qty_delta = current_qty - prev_qty
        commission_delta = current_commission - prev_commission
        if qty_delta > 0:
            seq += 1
            price = _safe_decimal(snapshot.get('avgPrice'))
            if price <= 0:
                price = _safe_decimal(fallback_price)
            fills.append({
                'seq': seq,
                'observedAt': str(snapshot.get('observedAt') or _utcnow_iso()),
                'qty': str(qty_delta),
                'price': str(price),
                'commission': str(commission_delta if commission_delta > 0 else Decimal('0')),
                'commissionCurrency': str(snapshot.get('commissionCurrency') or ''),
                'sourceType': 'order_poll_delta',
            })
        if current_qty > prev_qty:
            prev_qty = current_qty
        if current_commission > prev_commission:
            prev_commission = current_commission
    return fills


def _iso_to_epoch_ms(value: Any) -> int:
    text = str(value or '').strip()
    if not text:
        return 0
    try:
        normalized = text.replace('Z', '+00:00')
        return int(datetime.fromisoformat(normalized).timestamp() * 1000)
    except Exception:
        return 0


def _epoch_ms_to_iso(value: Any) -> str:
    try:
        return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc).isoformat().replace('+00:00', 'Z')
    except Exception:
        return ''


def _bingx_extract_fill_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    data = (payload or {}).get('data') or {}
    rows = []
    if isinstance(data, dict):
        rows = data.get('fill_orders') or data.get('fillOrders') or data.get('fills') or []
    elif isinstance(data, list):
        rows = data
    return [row for row in rows if isinstance(row, dict)]


def _bingx_extract_history_order_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    data = (payload or {}).get('data') or {}
    rows = []
    if isinstance(data, dict):
        rows = data.get('orders') or data.get('orderList') or []
    elif isinstance(data, list):
        rows = data
    return [row for row in rows if isinstance(row, dict)]


def _bingx_fill_parts_from_api_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    ordered_rows = sorted(
        rows,
        key=lambda row: (
            str(row.get('filledTime') or row.get('filledTm') or ''),
            str(row.get('orderId') or ''),
        ),
    )
    fills: List[Dict[str, Any]] = []
    seq = 0
    for row in ordered_rows:
        qty = _safe_decimal(row.get('volume') or row.get('qty') or row.get('executedQty'))
        if qty <= 0:
            continue
        price = _safe_decimal(row.get('price') or row.get('avgPrice'))
        seq += 1
        fills.append({
            'seq': seq,
            'observedAt': str(row.get('filledTime') or row.get('filledTm') or ''),
            'qty': str(qty),
            'price': str(price),
            'commission': str(abs(_safe_decimal(row.get('commission') or row.get('fee')))),
            'commissionCurrency': str(row.get('currency') or row.get('commissionCurrency') or row.get('feeAsset') or ''),
            'sourceType': 'allFillOrders',
            'raw': row,
        })
    return fills


def _bingx_fill_parts_from_history_order(order_row: Dict[str, Any]) -> List[Dict[str, Any]]:
    qty = _bingx_order_executed_qty(order_row)
    if qty <= 0:
        return []
    return [{
        'seq': 1,
        'observedAt': _epoch_ms_to_iso(order_row.get('updateTime') or order_row.get('time')) or _utcnow_iso(),
        'qty': str(qty),
        'price': str(_bingx_order_avg_price(order_row)),
        'commission': str(_bingx_order_commission(order_row)),
        'commissionCurrency': _bingx_order_commission_currency(order_row),
        'sourceType': 'allOrders_fallback',
        'raw': order_row,
    }]


def _bingx_extract_income_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = (payload or {}).get('data') or []
    if isinstance(rows, dict):
        rows = [rows]
    return [row for row in rows if isinstance(row, dict)]


def _bingx_apply_income_fee_fallback(
    fills: List[Dict[str, Any]],
    income_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    fill_rows = [fill for fill in fills if isinstance(fill, dict)]
    if not fill_rows:
        return {'fills': fills, 'feeTotal': Decimal('0'), 'feeCurrency': ''}

    selected_income_rows = [
        row for row in income_rows
        if str(row.get('incomeType') or '').strip().upper() == 'TRADING_FEE'
        and abs(_safe_decimal(row.get('income'))) > 0
    ]
    if not selected_income_rows:
        return {'fills': fills, 'feeTotal': Decimal('0'), 'feeCurrency': ''}

    total_fee = sum((abs(_safe_decimal(row.get('income'))) for row in selected_income_rows), Decimal('0'))
    if total_fee <= 0:
        return {'fills': fills, 'feeTotal': Decimal('0'), 'feeCurrency': ''}

    fee_currency = next((str(row.get('asset') or '').strip() for row in selected_income_rows if str(row.get('asset') or '').strip()), '')
    weighted = []
    total_weight = Decimal('0')
    for fill in fill_rows:
        weight = abs(_safe_decimal(fill.get('qty')) * _safe_decimal(fill.get('price')))
        if weight <= 0:
            weight = abs(_safe_decimal(fill.get('qty')))
        weighted.append(weight)
        total_weight += weight

    if total_weight <= 0:
        weighted = [Decimal('1') for _ in fill_rows]
        total_weight = Decimal(len(fill_rows))

    allocated_total = Decimal('0')
    new_fills: List[Dict[str, Any]] = []
    for idx, fill in enumerate(fill_rows):
        fill_copy = dict(fill)
        if idx == len(fill_rows) - 1:
            fee_part = total_fee - allocated_total
        else:
            fee_part = (total_fee * weighted[idx] / total_weight) if total_weight > 0 else Decimal('0')
            fee_part = fee_part.quantize(Decimal('0.00000001'))
            allocated_total += fee_part
        fill_copy['commission'] = str(abs(fee_part))
        if fee_currency:
            fill_copy['commissionCurrency'] = fee_currency
        fill_copy['sourceType'] = f"{str(fill_copy.get('sourceType') or 'unknown')}+income"
        new_fills.append(fill_copy)

    return {
        'fills': new_fills,
        'feeTotal': total_fee,
        'feeCurrency': fee_currency,
        'incomeRows': selected_income_rows,
    }


def _bingx_income_fee_fallback(
    client: BingXBroker,
    symbol: str,
    fills: List[Dict[str, Any]],
    started_ms: int,
    completed_ms: int,
) -> Dict[str, Any]:
    if not fills:
        return {'fills': fills, 'feeTotal': Decimal('0'), 'feeCurrency': ''}

    start_ms = max(0, started_ms - 2 * 60 * 1000)
    end_ms = max(start_ms + 1000, completed_ms + 2 * 60 * 1000)
    payload = client.get_income(
        symbol=symbol,
        income_type='TRADING_FEE',
        start_time=start_ms,
        end_time=end_ms,
        limit=200,
    )
    rows = _bingx_extract_income_rows(payload)
    candidate_rows: List[Dict[str, Any]] = []
    symbol_norm = str(symbol or '').replace('_', '-').upper()
    fill_trade_ids = {
        str(((fill.get('raw') or {}) if isinstance(fill.get('raw'), dict) else {}).get('tradeId') or '').strip()
        for fill in fills
        if isinstance(fill, dict)
    }
    fill_trade_ids.discard('')

    for row in rows:
        row_symbol = str(row.get('symbol') or '').replace('_', '-').upper()
        if row_symbol and row_symbol != symbol_norm:
            continue
        row_time = int(row.get('time') or 0)
        if row_time and (row_time < start_ms or row_time > end_ms):
            continue
        row_trade_id = str(row.get('tradeId') or '').strip()
        if fill_trade_ids and row_trade_id and row_trade_id not in fill_trade_ids:
            continue
        candidate_rows.append(row)

    applied = _bingx_apply_income_fee_fallback(fills, candidate_rows)
    applied['payload'] = payload
    applied['matchedRowCount'] = len(candidate_rows)
    return applied


def _bingx_fetch_actual_fills(
    client: BingXBroker,
    symbol: str,
    order_id: str,
    observed_started_at: Any,
    observed_completed_at: Any,
) -> Dict[str, Any]:
    if not symbol or not order_id:
        return {'fills': []}

    started_ms = _iso_to_epoch_ms(observed_started_at)
    completed_ms = _iso_to_epoch_ms(observed_completed_at)
    if completed_ms <= 0:
        completed_ms = int(time.time() * 1000)
    if started_ms <= 0:
        started_ms = max(0, completed_ms - 30 * 60 * 1000)

    fill_error = None
    last_payload = None
    retry_delays_sec = [0.0, 0.7, 1.5, 3.0]
    for delay_sec in retry_delays_sec:
        if delay_sec > 0:
            time.sleep(delay_sec)
        now_ms = int(time.time() * 1000)
        start_ms = max(0, started_ms - 30 * 60 * 1000)
        end_ms = max(start_ms + 1000, min(now_ms + 5 * 60 * 1000, completed_ms + 30 * 60 * 1000))

        try:
            payload = client.get_all_fill_orders(
                symbol=symbol,
                order_id=order_id,
                start_ts=start_ms,
                end_ts=end_ms,
                trading_unit='COIN',
            )
            last_payload = payload
            rows = [row for row in _bingx_extract_fill_rows(payload) if str(row.get('orderId') or '') == str(order_id)]
            fills = _bingx_fill_parts_from_api_rows(rows)
            if fills:
                has_nonzero_fee = any(abs(_safe_decimal(fill.get('commission'))) > 0 for fill in fills if isinstance(fill, dict))
                if not has_nonzero_fee:
                    try:
                        income_result = _bingx_income_fee_fallback(client, symbol, fills, started_ms, completed_ms)
                        income_fills = income_result.get('fills') or fills
                        income_fee_total = abs(_safe_decimal(income_result.get('feeTotal')))
                        if income_fee_total > 0:
                            return {
                                'fills': income_fills,
                                'source': 'allFillOrders+income',
                                'payload': payload,
                                'incomePayload': income_result.get('payload'),
                                'incomeMatchedRowCount': income_result.get('matchedRowCount'),
                                'retryDelaySec': delay_sec,
                            }
                    except Exception as exc:
                        fill_error = str(exc) if fill_error is None else f'{fill_error}; income fallback: {exc}'
                if has_nonzero_fee or delay_sec >= retry_delays_sec[-1]:
                    return {
                        'fills': fills,
                        'source': 'allFillOrders',
                        'payload': payload,
                        'retryDelaySec': delay_sec,
                    }
        except Exception as exc:
            fill_error = str(exc)

        try:
            history = client.get_all_orders(
                symbol=symbol,
                order_id=order_id,
                start_time=max(0, end_ms - 7 * 24 * 60 * 60 * 1000),
                end_time=end_ms,
                limit=100,
            )
            history_rows = _bingx_extract_history_order_rows(history)
            for row in history_rows:
                if str(row.get('orderId') or '') != str(order_id):
                    continue
                fills = _bingx_fill_parts_from_history_order(row)
                if fills:
                    has_nonzero_fee = any(abs(_safe_decimal(fill.get('commission'))) > 0 for fill in fills if isinstance(fill, dict))
                    if not has_nonzero_fee:
                        try:
                            income_result = _bingx_income_fee_fallback(client, symbol, fills, started_ms, completed_ms)
                            income_fills = income_result.get('fills') or fills
                            income_fee_total = abs(_safe_decimal(income_result.get('feeTotal')))
                            if income_fee_total > 0:
                                return {
                                    'fills': income_fills,
                                    'source': 'allOrders+income',
                                    'payload': history,
                                    'incomePayload': income_result.get('payload'),
                                    'incomeMatchedRowCount': income_result.get('matchedRowCount'),
                                    'previousError': fill_error,
                                    'retryDelaySec': delay_sec,
                                }
                        except Exception as exc:
                            fill_error = str(exc) if fill_error is None else f'{fill_error}; income fallback: {exc}'
                    if has_nonzero_fee or delay_sec >= retry_delays_sec[-1]:
                        return {
                            'fills': fills,
                            'source': 'allOrders_fallback',
                            'payload': history,
                            'previousError': fill_error,
                            'retryDelaySec': delay_sec,
                        }
        except Exception as exc:
            if fill_error is None:
                fill_error = str(exc)
            else:
                fill_error = f'{fill_error}; history fallback: {exc}'

    return {
        'fills': [],
        'source': 'unavailable',
        'error': fill_error,
        'payload': last_payload,
    }


def _bingx_remaining_qty(total_qty: Any, executed_qty: Any, quantity_precision: int) -> str:
    try:
        total_dec = Decimal(str(total_qty).strip())
    except Exception:
        return '0'
    try:
        executed_dec = Decimal(str(executed_qty).strip())
    except Exception:
        executed_dec = Decimal('0')
    remaining = total_dec - executed_dec
    if remaining <= 0:
        return '0'
    quantum = Decimal('1').scaleb(-max(0, int(quantity_precision)))
    remaining = remaining.quantize(quantum, rounding=ROUND_UP)
    if remaining <= 0:
        return '0'
    text = format(remaining, 'f').rstrip('0').rstrip('.')
    return text or '0'


def _bingx_quantize_contract_qty_up(value: Any, quantity_precision: int) -> str:
    try:
        dec = Decimal(str(value).strip())
    except Exception:
        return '0'
    if dec <= 0:
        return '0'
    quantum = Decimal('1').scaleb(-max(0, int(quantity_precision)))
    dec = dec.quantize(quantum, rounding=ROUND_UP)
    if dec <= 0:
        return '0'
    text = format(dec, 'f').rstrip('0').rstrip('.')
    return text or '0'


async def _execute_bingx(payload: Dict[str, Any], destination: Dict[str, Any]) -> Dict[str, Any]:
    broker = destination['broker']
    symbol = destination['symbol']
    side_raw = str(destination.get('side', payload['side'])).strip().lower()
    signal_mode = str(destination.get('signalMode') or payload.get('signalMode') or 'step-side').strip().lower()
    target_direction = ''
    side, signal_mode, resolved_dir = _resolve_side(side_raw, signal_mode)
    if resolved_dir:
        target_direction = resolved_dir
    elif signal_mode == 'target-direction' and side_raw in ('long', 'short'):
        target_direction = side_raw
        side = 'buy' if target_direction == 'long' else 'sell'
    quantity = destination.get('qty', payload.get('qty'))
    qty_kind = str(destination.get('qtyKind') or payload.get('qtyKind') or 'contracts').lower()
    if signal_mode == 'target-direction':
        if quantity in (None, '') and payload.get('qty') not in (None, ''):
            quantity = payload.get('qty')
        qty_kind = str(destination.get('qtyKind') or payload.get('qtyKind') or 'usdt').lower()
    open_qty_kind = str(destination.get('openQtyKind') or qty_kind or 'usdt').lower()
    smart_inc_applied = None
    smart_inc_skip = False
    smart_inc_tol = smart_inc_avg_tol(destination)
    if destination.get('smartInc') and signal_mode == 'target-direction':
        try:
            base_qty = float(quantity)
        except (TypeError, ValueError):
            base_qty = None
        if base_qty is not None:
            # Same-hour dedup on exchange symbol (NG1! + NATURAL… → one fill).
            hours = hours_since_last_fill(broker, symbol)
            multiplier = smart_inc_multiplier(hours)
            if multiplier == 0:
                smart_inc_skip = True
                smart_inc_applied = {
                    'mode': 'hours',
                    'baseQty': base_qty,
                    'hoursSinceLastFill': None if hours is None else round(hours, 3),
                    'multiplier': 0,
                    'qty': quantity,
                }
            # Detect add-to-position when avg-tol mode is on (same as Bybit).
            add_mode = False
            pos_qty = 0.0
            pos_avg = 0.0
            pos_mark = 0.0
            if smart_inc_tol is not None and not smart_inc_skip:
                try:
                    _probe = BingXBroker()
                    _prows = _bingx_position_rows(_probe.get_positions(symbol), symbol)
                    _want = 'LONG' if target_direction == 'long' else 'SHORT'
                    for _row in _prows:
                        _rs = str(_row.get('positionSide') or _row.get('side') or '').upper()
                        _sz = _bingx_position_qty(_row)
                        if _sz <= 0:
                            continue
                        if _rs == _want:
                            add_mode = True
                            pos_qty = _sz
                            for _k in ('avgPrice', 'entryPrice', 'priceAvg', 'positionX'):
                                try:
                                    pos_avg = abs(float(_row.get(_k) or 0) or 0)
                                    if pos_avg > 0:
                                        break
                                except Exception:
                                    pass
                            for _k in ('markPrice', 'mark'):
                                try:
                                    pos_mark = abs(float(_row.get(_k) or 0) or 0)
                                    if pos_mark > 0:
                                        break
                                except Exception:
                                    pass
                            if not pos_mark:
                                pos_mark = pos_avg
                            break
                except Exception:
                    add_mode = False
            if add_mode and pos_qty > 0 and smart_inc_tol is not None and not smart_inc_skip:
                need = smart_inc_required_add_qty(pos_qty, pos_avg, pos_mark or pos_avg, target_direction, smart_inc_tol)
                if need is not None and need <= 1e-12:
                    # Avg already within tol: do not skip — fall through to hours (cell × hours).
                    add_mode = False
                elif need is not None:
                    # Open/add is placed in USDT — compute notional directly, no contract qty on the wire.
                    need_usdt = smart_inc_required_add_usdt(pos_qty, pos_avg, pos_mark or pos_avg, target_direction, smart_inc_tol)
                    if need_usdt is None:
                        add_mode = False
                    else:
                        # min check: minUsdt (preferred) or minQty*price; skip tiny residuals.
                        limits = (destination or {}).get('limits') or {}
                        min_usdt = 0.0
                        try:
                            min_usdt = float(limits.get('minUsdt') or 0)
                        except (TypeError, ValueError):
                            min_usdt = 0.0
                        if min_usdt <= 0 and limits.get('minQty'):
                            try:
                                min_usdt = float(limits['minQty']) * (pos_mark or pos_avg)
                            except (TypeError, ValueError):
                                min_usdt = 0.0
                        if need_usdt <= 0:
                            # Numerically in range after usdt conversion — fall through to hours.
                            add_mode = False
                        elif min_usdt > 0 and need_usdt < min_usdt:
                            smart_inc_skip = True
                            smart_inc_applied = {
                                'mode': 'avg_tol',
                                'tol': smart_inc_tol,
                                'posQty': pos_qty,
                                'avgPrice': pos_avg,
                                'markPrice': pos_mark,
                                'requiredAddUsdt': need_usdt,
                                'reason': 'need_below_min_usdt',
                                'qty': quantity,
                            }
                        else:
                            quantity = need_usdt
                            smart_inc_applied = {
                                'mode': 'avg_tol',
                                'tol': smart_inc_tol,
                                'posQty': pos_qty,
                                'avgPrice': pos_avg,
                                'markPrice': pos_mark,
                                'requiredAddUsdt': need_usdt,
                                'qty': quantity,
                                'qtyKind': 'usdt',
                            }
                            open_qty_kind = 'usdt'
                else:
                    add_mode = False
            if not smart_inc_skip and not (smart_inc_applied and smart_inc_applied.get('mode') == 'avg_tol'):
                scaled = base_qty * multiplier
                quantity = int(scaled) if float(scaled).is_integer() else scaled
                if smart_inc_applied is None:
                    smart_inc_applied = {}
                smart_inc_applied.update({
                    'mode': 'hours',
                    'baseQty': base_qty,
                    'hoursSinceLastFill': None if hours is None else round(hours, 3),
                    'multiplier': multiplier,
                    'qty': quantity,
                })
    category = destination.get('category', 'swap')
    execution_mode = str(destination.get('executionMode', destination.get('mode', 'maker'))).lower()
    reduce_only = destination.get('reduceOnly') if 'reduceOnly' in destination else payload.get('reduceOnly')
    position_side = str(destination.get('positionSide', payload.get('positionSide', 'BOTH')) or 'BOTH').upper()
    bingx_hedged_mode_raw = destination.get('hedgedMode', payload.get('hedgedMode'))
    bingx_hedged_mode = None if bingx_hedged_mode_raw in (None, '') else bool(bingx_hedged_mode_raw)
    dry_run = bool(destination.get('dryRun', False) or payload.get('dryRun', False))
    testnet = bool(destination.get('testnet', False))
    price = destination.get('price') or payload.get('price')
    client_order_id = destination.get('clientOrderId') or payload.get('clientOrderId')
    risk_pct_raw = destination.get('riskPct') if 'riskPct' in destination else payload.get('riskPct')
    risk_pct = None
    try:
        if risk_pct_raw not in (None, ''):
            risk_pct = float(str(risk_pct_raw).replace('%', '').strip())
    except Exception:
        risk_pct = None
    # avg_tol add on BingX: keep cell execution mode (maker/limit) and leave price
    # empty so prepare_limit_order uses smart depth placement (get_best_depth_price).
    # Do NOT pin to mark — that bypasses the book and causes cancel/repost churn.
    # avg_tol / open path orders are placed in USDT when openQtyKind=usdt.
    request_qty_kind = open_qty_kind if (smart_inc_applied and smart_inc_applied.get('mode') == 'avg_tol') else qty_kind
    request_payload = {
        'broker': broker,
        'symbol': symbol,
        'side': side,
        'qty': quantity,
        'qtyKind': request_qty_kind,
        'category': category,
        'executionMode': execution_mode,
        'positionSide': position_side,
        'signalMode': signal_mode,
        'targetDirection': target_direction,
        'stage': 'init',
        'stageTrace': ['init'],
    }
    if smart_inc_applied is not None:
        request_payload['smartInc'] = smart_inc_applied
    if bingx_hedged_mode is not None:
        request_payload['hedgedMode'] = bingx_hedged_mode
    if reduce_only is not None:
        request_payload['reduceOnly'] = bool(reduce_only)
    if client_order_id:
        request_payload['clientOrderId'] = client_order_id
    if risk_pct is not None:
        request_payload['riskPct'] = risk_pct
        request_payload['marginType'] = 'ISOLATED'
    if price not in (None, ''):
        request_payload['price'] = price

    def _set_stage(name: str) -> None:
        request_payload['stage'] = name
        trace = request_payload.setdefault('stageTrace', [])
        if not trace or trace[-1] != name:
            trace.append(name)

    if smart_inc_skip:
        _reason = 'smart_inc_avg_in_range'
        if smart_inc_applied and smart_inc_applied.get('reason') == 'need_below_min_qty':
            _reason = 'smart_inc_need_below_min_qty'
        elif smart_inc_applied and smart_inc_applied.get('mode') == 'hours':
            _reason = 'smart_inc_same_hour'
        _set_stage('smart_inc_skip')
        return {
            'broker': broker,
            'symbol': symbol,
            'category': category,
            'qty': quantity,
            'request': request_payload,
            'skipped': True,
            'results': {
                'skipped': True,
                'reason': _reason,
                'smartInc': smart_inc_applied,
            },
        }

    if execution_mode == 'market':
        error = 'Only limit orders are supported by BingX broker'
        return {
            'broker': broker,
            'symbol': symbol,
            'category': category,
            'qty': quantity,
            'request': request_payload,
            'error': error,
            'results': {'error': error},
        }

    try:
        def _run() -> Dict[str, Any]:
            client = BingXBroker(testnet=testnet)
            _set_stage('prepare_limit_order')
            prepared = client.prepare_limit_order(symbol=symbol, side=side, qty=quantity, price=price, qty_kind=open_qty_kind)
            request_payload['price'] = prepared['price']
            request_payload['symbol'] = prepared['symbol']
            request_payload['qty'] = prepared.get('quantity', prepared.get('quoteOrderQty', quantity))
            # Journal truth: USDT open path uses quoteOrderQty — report openQtyKind as qtyKind.
            if str(open_qty_kind).lower() in ('usdt', 'quote', 'quote_usdt', 'notional'):
                request_payload['qtyKind'] = 'usdt'
            else:
                request_payload['qtyKind'] = qty_kind
            request_payload['openQtyKind'] = open_qty_kind
            request_payload['positionSide'] = position_side
            request_payload['bookTicker'] = prepared.get('bookTicker') or {}
            request_payload['depthPlan'] = prepared.get('depthPlan') or {}
            request_payload['contract'] = {
                'symbol': (prepared.get('contract') or {}).get('symbol'),
                'pricePrecision': (prepared.get('contract') or {}).get('pricePrecision'),
                'quantityPrecision': (prepared.get('contract') or {}).get('quantityPrecision'),
                'asset': (prepared.get('contract') or {}).get('asset'),
                'displayName': (prepared.get('contract') or {}).get('displayName'),
            }
            contract_meta = prepared.get('contract') or {}
            contract_asset = str(contract_meta.get('asset') or '').upper()
            contract_display = str(contract_meta.get('displayName') or '').upper()
            contract_symbol = str((prepared.get('contract') or {}).get('symbol') or prepared.get('symbol') or '').upper()
            is_non_crypto_index = bool(
                contract_asset.startswith('NCS')
                or contract_symbol.startswith('NCS')
                or 'NASDAQ' in contract_display
                or 'SPY' in contract_display
                or 'DXY' in contract_display
            )
            api_position_side = position_side

            target_direction_mode = (signal_mode == 'target-direction')
            current_position_mode = None
            if target_direction_mode or is_non_crypto_index or bingx_hedged_mode is not None:
                _set_stage('get_position_mode')
                position_mode_info = client.get_position_mode()
                request_payload['positionModeInfo'] = position_mode_info
                dual_side = str(((position_mode_info.get('data') or {}).get('dualSidePosition')) or '').lower()
                if dual_side in ('true', 'false'):
                    current_position_mode = (dual_side == 'true')
                    request_payload['hedgedModeCurrent'] = current_position_mode

            desired_hedged_mode = bingx_hedged_mode
            if desired_hedged_mode is None and target_direction_mode:
                desired_hedged_mode = True
                request_payload['hedgedModeDesired'] = True

            if desired_hedged_mode is not None and current_position_mode is not None and desired_hedged_mode != current_position_mode:
                _set_stage('set_position_mode')
                mode_change = client.set_position_mode(desired_hedged_mode)
                request_payload['positionModeChange'] = mode_change
                refreshed_dual_side = str((((mode_change.get('data') or {}).get('dualSidePosition')) or '')).lower()
                if refreshed_dual_side in ('true', 'false'):
                    current_position_mode = (refreshed_dual_side == 'true')
                    request_payload['hedgedModeCurrent'] = current_position_mode

            if current_position_mode is True:
                api_position_side = 'LONG' if side == 'buy' else 'SHORT'
                request_payload['positionSideMode'] = 'hedge'
            else:
                api_position_side = 'BOTH'
                request_payload['positionSideMode'] = 'one_way'
            if dry_run:
                _set_stage('dry_run_ready')
                return {
                    'dryRun': True,
                    'wouldPlace': {
                        'symbol': prepared['symbol'],
                        'side': prepared['side'],
                        'type': 'LIMIT',
                        'quantity': prepared.get('quantity') or prepared.get('quoteOrderQty') or quantity,
                        'price': prepared['price'],
                        'timeInForce': 'GTC',
                        'positionSide': api_position_side,
                        'reduceOnly': None if reduce_only is None else bool(reduce_only),
                        'clientOrderId': client_order_id or '',
                    },
                    'bookTicker': prepared.get('bookTicker') or {},
                    'contract': prepared.get('contract') or {},
                }

            margin_ops: Dict[str, Any] = {}
            pretrade_aborted = False
            requested_position_side = position_side
            if requested_position_side == 'BOTH':
                requested_position_side = 'LONG' if side == 'buy' else 'SHORT'
            if api_position_side in (None, 'LONG', 'SHORT'):
                requested_position_side = 'LONG' if side == 'buy' else 'SHORT'

            close_position_side = None
            close_position_qty = Decimal('0')
            effective_reduce_only = bool(reduce_only) if reduce_only is not None else None
            target_mode_close_then_open = False
            if current_position_mode is True:
                _set_stage('get_positions_before')
                positions_before_for_netting = client.get_positions(prepared['symbol'])
                request_payload['positionPayloadBeforeRaw'] = positions_before_for_netting
                position_rows_before = _bingx_position_rows(positions_before_for_netting, prepared['symbol'])
                buckets_before = _bingx_position_buckets(positions_before_for_netting, prepared['symbol'])
                request_payload['positionRowsBeforeRaw'] = position_rows_before
                request_payload['positionBucketsBefore'] = {k: float(v) for k, v in buckets_before.items()}
                long_qty = buckets_before.get('LONG', Decimal('0')) or Decimal('0')
                short_qty = buckets_before.get('SHORT', Decimal('0')) or Decimal('0')
                net_qty = long_qty - short_qty
                request_payload['positionNetBefore'] = float(net_qty)
                opposite_qty = short_qty if side == 'buy' else long_qty
                same_side_qty = long_qty if side == 'buy' else short_qty
                request_payload['positionOppositeBefore'] = float(opposite_qty)
                request_payload['positionSameSideBefore'] = float(same_side_qty)
                if signal_mode == 'target-direction':
                    desired_side = 'LONG' if target_direction == 'long' else 'SHORT'
                    opposite_side = 'SHORT' if desired_side == 'LONG' else 'LONG'
                    opposite_qty_target = buckets_before.get(opposite_side, Decimal('0')) or Decimal('0')
                    same_qty_target = buckets_before.get(desired_side, Decimal('0')) or Decimal('0')
                    request_payload['targetDirectionOppositeQty'] = float(opposite_qty_target)
                    request_payload['targetDirectionSameQty'] = float(same_qty_target)
                    if opposite_qty_target > 0:
                        close_position_side = opposite_side
                        close_position_qty = opposite_qty_target
                        api_position_side = opposite_side
                        effective_reduce_only = None
                        request_payload['positionSideNetting'] = opposite_side
                        request_payload['nettingAction'] = 'target_direction_close_opposite_then_open_target'
                        request_payload['hedgeCloseUsesReduceOnly'] = False
                        request_payload['targetDirectionCloseInitialQtyRaw'] = str(opposite_qty_target)
                        target_mode_close_then_open = True
                    else:
                        requested_position_side = desired_side
                        api_position_side = desired_side
                        effective_reduce_only = None
                        request_payload['positionSideNetting'] = desired_side
                        request_payload['nettingAction'] = 'target_direction_open_or_increase_target'
                        request_payload['hedgeOpenUsesReduceOnly'] = False
                elif opposite_qty > 0:
                    close_position_side = 'SHORT' if side == 'buy' else 'LONG'
                    api_position_side = close_position_side
                    effective_reduce_only = None
                    request_payload['positionSideNetting'] = close_position_side
                    request_payload['nettingAction'] = 'close_opposite_leg_only'
                    request_payload['hedgeCloseUsesReduceOnly'] = False
                else:
                    requested_position_side = 'LONG' if side == 'buy' else 'SHORT'
                    api_position_side = requested_position_side
                    effective_reduce_only = None
                    request_payload['positionSideNetting'] = requested_position_side
                    request_payload['nettingAction'] = 'open_same_side_leg'
                    request_payload['hedgeOpenUsesReduceOnly'] = False

            risk_control_enabled = bool(risk_pct is not None and risk_pct > 0)
            if is_non_crypto_index and risk_pct is not None:
                request_payload['riskControlMode'] = 'enabled_for_non_crypto'

            if risk_control_enabled:
                _set_stage('get_balance_before')
                balance = client.get_balance()
                equity = _bingx_account_equity(balance)
                margin_ops['balance'] = balance
                if current_position_mode is True:
                    positions_before = positions_before_for_netting
                else:
                    _set_stage('get_positions_before')
                    positions_before = client.get_positions(prepared['symbol'])
                before_position = _bingx_extract_position(positions_before, prepared['symbol'], requested_position_side)
                before_qty = _bingx_position_qty(before_position)
                # Target leg only: extract may fall back to the opposite (flip) leg
                # when the open side is still empty.
                before_side = str((before_position or {}).get('positionSide') or (before_position or {}).get('side') or '').upper()
                if str(requested_position_side or '').upper() and before_side and before_side != str(requested_position_side).upper():
                    before_qty = 0.0
                mark_price = float(prepared['price'])
                incoming_notional = _bingx_prepared_notional(prepared)
                try:
                    incoming_raw = abs(float(prepared.get('quoteOrderQty') or prepared.get('quantity') or 0))
                except (TypeError, ValueError):
                    incoming_raw = 0.0
                expected_final_qty = max(0.0, before_qty + (0.0 if prepared.get('quoteOrderQty') not in (None, '') else incoming_raw))
                unrealized_before = _bingx_account_unrealized(balance)
                # Terminal Risk = riskRate = maintenance / isolated.
                # Target isolated so riskRate == riskPct% (e.g.12 → 0.12).
                allowed_loss = max(0.0, equity * (risk_pct / 100.0))
                target_isolated_pre = allowed_loss + unrealized_before
                if target_isolated_pre < 0:
                    target_isolated_pre = 0.0
                # Final notional = target-side contracts×mark + this order (USDT already USDT).
                expected_notional = abs(mark_price * before_qty) + incoming_notional
                target_margin_pre = min(expected_notional, target_isolated_pre) if target_isolated_pre > 0 else 0.0
                # Do not change leverage — user sets it in BingX terminal.
                actual_leverage = 0
                try:
                    actual_leverage = int(float(before_position.get('leverage') or 0))
                except Exception:
                    actual_leverage = 0
                request_payload['riskControl'] = {
                    'mode': 'non_crypto' if is_non_crypto_index else 'standard',
                    'equity': equity,
                    'unrealizedProfit': unrealized_before,
                    'allowedLoss': allowed_loss,
                    'targetIsolatedPre': target_isolated_pre,
                    'targetIsolatedRiskRate': _bingx_target_isolated_for_notional(expected_notional, risk_pct),
                    'beforeQty': before_qty,
                    'incomingQty': incoming_raw,
                    'incomingNotional': incoming_notional,
                    'expectedFinalQty': expected_final_qty,
                    'expectedNotional': expected_notional,
                    'preTradeLeverage': actual_leverage,
                    'leverageSource': 'position_or_terminal',
                }
                _set_stage('set_margin_type')
                margin_ops['setMarginType'] = client.set_margin_type(prepared['symbol'], 'ISOLATED')

            effective_position_side = api_position_side if api_position_side != 'BOTH' else requested_position_side

            def _ensure_pretrade_margin(position_side: str) -> Dict[str, Any] | None:
                """Add isolated to riskPct target before opening/adding.

                Returns None if OK; error dict → abort without placing order.
                """
                if dry_run or not risk_control_enabled:
                    return None
                rc = request_payload.setdefault('riskControl', {})

                def _pretrade_abort(msg: str) -> Dict[str, Any]:
                    _set_stage('pre_trade_margin_aborted')
                    rc['preTradeMarginAborted'] = True
                    return {
                        'code': -1,
                        'msg': msg,
                        'stage': request_payload.get('stage'),
                        'stageTrace': request_payload.get('stageTrace', []),
                        'riskControl': rc,
                        '_riskControl': margin_ops,
                    }

                # Recompute from THIS order + live target-side position:
                # flip close→open invalidates the pre-close snapshot, and
                # quoteOrderQty is already USDT (must not × mark).
                _set_stage('pre_trade_get_position')
                try:
                    pos_rows = client.get_positions(prepared['symbol'])
                    pos = _bingx_extract_position(pos_rows, prepared['symbol'], position_side)
                except Exception as e:
                    return _pretrade_abort(f'pre-trade margin: cannot read position: {e} — order not placed')
                same_side_qty = _bingx_position_qty(pos)
                pos_side = str((pos or {}).get('positionSide') or (pos or {}).get('side') or '').upper()
                if str(position_side or '').upper() and pos_side and pos_side != str(position_side).upper():
                    same_side_qty = 0.0
                try:
                    mark_now = abs(float(prepared.get('price') or 0))
                except (TypeError, ValueError):
                    mark_now = 0.0
                order_notional = _bingx_prepared_notional(prepared)
                expected_notional = abs(mark_now * same_side_qty) + order_notional
                rc['expectedNotional'] = expected_notional
                rc['expectedNotionalSource'] = 'prepared+live_position'
                target_iso = _bingx_target_isolated_for_notional(expected_notional, risk_pct)
                rc['preTradeTargetIsolated'] = target_iso
                if target_iso <= 0:
                    return None
                current_iso = _bingx_position_isolated(pos)
                rc['preTradeIsolated'] = current_iso
                need_add = max(0.0, target_iso - current_iso)
                rc['preTradeNeedAdd'] = need_add
                if need_add <= 0.00000001:
                    rc['preTradeAddMargin'] = 0.0
                    return None

                _set_stage('pre_trade_get_balance')
                try:
                    pre_bal = client.get_balance()
                except Exception as e:
                    return _pretrade_abort(f'pre-trade margin: cannot read balance: {e} — order not placed')
                available = _bingx_account_available(pre_bal)
                rc['preTradeAvailable'] = available

                # New position: cannot call positionMargin yet — require free balance
                # for post-fill top-up to target (initial margin comes out of available too).
                if current_iso <= 0:
                    if available + 1e-8 < target_iso:
                        # Flip close→open: isolated free-up may lag the balance snapshot.
                        # One short retry with a fresh balance before aborting.
                        rc['preTradeBalanceRetry'] = True
                        time.sleep(0.4)
                        try:
                            pre_bal = client.get_balance()
                            available = _bingx_account_available(pre_bal)
                            rc['preTradeAvailableRetry'] = available
                        except Exception as retry_exc:
                            rc['preTradeBalanceRetryError'] = str(retry_exc)
                    if available + 1e-8 < target_iso:
                        rc['preTradeAddMarginSkipped'] = 'insufficient_balance_for_new_position'
                        return _pretrade_abort(
                            f'pre-trade margin insufficient for new position: '
                            f'need {target_iso:.4f} USDT free, available {available:.4f} — order not placed'
                        )
                    rc['preTradeAddMargin'] = 0.0
                    rc['preTradeAddMarginNote'] = 'new_position_reserved_via_available'
                    return None

                if available + 1e-8 < need_add:
                    rc['preTradeAddMarginSkipped'] = 'insufficient_balance'
                    rc['preTradeAddMarginShortfall'] = need_add - available
                    return _pretrade_abort(
                        f'pre-trade margin insufficient: need add {need_add:.4f} USDT, '
                        f'available {available:.4f} — order not placed'
                    )

                _set_stage('pre_trade_add_margin')
                try:
                    add_resp = client.adjust_isolated_margin(
                        prepared['symbol'], position_side, need_add, direction_type=1
                    )
                except Exception as e:
                    rc['preTradeAddMarginError'] = str(e)
                    return _pretrade_abort(f'pre-trade add_margin failed: {e} — order not placed')
                margin_ops['preTradeAddMargin'] = add_resp
                rc['preTradeAddMargin'] = need_add
                if not _bingx_api_ok(add_resp):
                    rc['preTradeAddMarginFailed'] = True
                    rc['preTradeAddMarginApiCode'] = add_resp.get('code') if isinstance(add_resp, dict) else None
                    rc['preTradeAddMarginApiMsg'] = str((add_resp or {}).get('msg') or '') if isinstance(add_resp, dict) else str(add_resp)
                    return _pretrade_abort(
                        f'pre-trade add_margin rejected: code={rc.get("preTradeAddMarginApiCode")} '
                        f'msg={rc.get("preTradeAddMarginApiMsg")} — order not placed'
                    )

                _set_stage('pre_trade_verify_margin')
                try:
                    pos2_rows = client.get_positions(prepared['symbol'])
                    pos2 = _bingx_extract_position(pos2_rows, prepared['symbol'], position_side)
                    iso_after = _bingx_position_isolated(pos2)
                    rc['preTradeIsolatedAfterAdd'] = iso_after
                    # Allow small slack (mark/uPnL drift)
                    if iso_after + 0.05 < target_iso * 0.5:
                        rc['preTradeAddMarginNoEffect'] = True
                        return _pretrade_abort(
                            f'pre-trade margin still low after add: isolated={iso_after:.4f} '
                            f'target={target_iso:.4f} — order not placed'
                        )
                except Exception as e:
                    rc['preTradeVerifyError'] = str(e)
                return None

            # Simple open/add: top up risk margin BEFORE placing.
            # close-then-open: skip here (frees margin first); ensure before target open below.
            pretrade_aborted = False
            if not target_mode_close_then_open and risk_control_enabled and not dry_run:
                pre_abort = _ensure_pretrade_margin(
                    api_position_side if api_position_side != 'BOTH' else requested_position_side
                )
                if pre_abort:
                    pretrade_aborted = True
                    if margin_ops:
                        pre_abort['_riskControl'] = margin_ops
                    return pre_abort

            def _run_limit_repost_loop(loop_prepared: Dict[str, Any], loop_position_side: str, loop_reduce_only: Any, stage_prefix: str = ''):
                order_attempts = []
                poll_plan_ms = [350, 700, 1200]
                max_reposts = 5
                current_prepared = dict(loop_prepared)
                # USDT orders carry quoteOrderQty; contracts carry quantity.
                is_quote_order = str(current_prepared.get('qtyKind') or '').lower() in ('usdt', 'quote', 'quote_usdt', 'notional') or current_prepared.get('quoteOrderQty') not in (None, '')
                remaining_qty = str(current_prepared.get('quoteOrderQty') if is_quote_order else current_prepared.get('quantity'))
                loop_result = None
                loop_final_order_row = {}
                loop_effective_position_side = loop_position_side

                for attempt_index in range(max_reposts + 1):
                    if is_quote_order:
                        current_prepared['quoteOrderQty'] = remaining_qty
                    else:
                        current_prepared['quantity'] = remaining_qty
                    # Open AND close: IOC vs nearest book level — take what's there
                    # now; remainder reposts to the next level. No GTC waiting.
                    open_tif = 'IOC'
                    _set_stage(f"{stage_prefix}place_limit_order" if attempt_index == 0 else f"{stage_prefix}place_limit_order_repost")
                    attempt_result = client.place_limit_order(
                        current_prepared,
                        position_side=loop_position_side,
                        reduce_only=loop_reduce_only,
                        client_order_id=client_order_id,
                        time_in_force=open_tif,
                    )

                    message = str((attempt_result or {}).get('msg') or '')
                    if loop_position_side == 'BOTH' and 'Hedge mode' in message:
                        retry_position_side = 'LONG' if side == 'buy' else 'SHORT'
                        request_payload['positionSideRetry'] = retry_position_side
                        loop_effective_position_side = retry_position_side
                        _set_stage(f'{stage_prefix}place_limit_order_retry_hedge')
                        attempt_result = client.place_limit_order(
                            current_prepared,
                            position_side=retry_position_side,
                            reduce_only=loop_reduce_only,
                            client_order_id=client_order_id,
                            time_in_force=open_tif,
                        )

                    loop_result = attempt_result
                    order_row = _bingx_extract_order_row(attempt_result)
                    order_id = _bingx_order_id(order_row)
                    attempt_entry = {
                        'attempt': attempt_index + 1,
                        'placedQty': current_prepared.get('quantity'),
                        'placedPrice': current_prepared.get('price'),
                        'result': attempt_result,
                        'orderId': order_id,
                        'observedStartedAt': _utcnow_iso(),
                        'snapshots': [_bingx_order_snapshot(order_row, current_prepared.get('price'))],
                    }
                    order_attempts.append(attempt_entry)

                    if not isinstance(attempt_result, dict) or str(attempt_result.get('code')) not in ('None', '0', ''):
                        break

                    latest_order = order_row
                    for wait_ms in poll_plan_ms:
                        time.sleep(wait_ms / 1000.0)
                        if not order_id:
                            break
                        _set_stage(f"{stage_prefix}poll_open_order" if attempt_index == 0 else f"{stage_prefix}poll_open_order_repost")
                        polled = client.get_order(current_prepared['symbol'], order_id=order_id)
                        polled_row = _bingx_extract_order_row(polled)
                        if polled_row:
                            latest_order = polled_row
                            attempt_entry.setdefault('polls', []).append(polled)
                            attempt_entry.setdefault('snapshots', []).append(_bingx_order_snapshot(polled_row, current_prepared.get('price')))
                        status = _bingx_order_status(latest_order)
                        if status in ('FILLED', 'CANCELED', 'EXPIRED', 'PENDING'):
                            break

                    loop_final_order_row = latest_order or order_row
                    final_status = _bingx_order_status(loop_final_order_row)
                    total_qty = current_prepared.get('quoteOrderQty') if is_quote_order else (current_prepared.get('quantity') or '0')
                    quantity_precision = int((prepared.get('contract') or {}).get('quantityPrecision') or 0)
                    attempt_entry['observedCompletedAt'] = _utcnow_iso()

                    if final_status == 'PENDING':
                        if not order_id:
                            break
                        _set_stage(f"{stage_prefix}cancel_remainder" if attempt_index == 0 else f"{stage_prefix}cancel_remainder_repost")
                        cancel_result = client.cancel_order(current_prepared['symbol'], order_id=order_id)
                        attempt_entry['cancelResult'] = cancel_result
                        _set_stage(f"{stage_prefix}confirm_cancel" if attempt_index == 0 else f"{stage_prefix}confirm_cancel_repost")
                        confirmed_order = client.get_order(current_prepared['symbol'], order_id=order_id)
                        confirmed_order_row = _bingx_extract_order_row(confirmed_order)
                        if confirmed_order_row:
                            loop_final_order_row = confirmed_order_row
                            final_status = _bingx_order_status(confirmed_order_row)
                            attempt_entry['confirmedStatus'] = final_status
                            attempt_entry.setdefault('snapshots', []).append(_bingx_order_snapshot(confirmed_order_row, current_prepared.get('price')))
                        if final_status not in ('CANCELED', 'EXPIRED', 'FILLED'):
                            loop_result = {
                                'code': -1,
                                'msg': f'order {order_id} remained {final_status or "PENDING"} after cancel attempt'
                            }
                            break

                    executed_qty = _bingx_order_executed_qty(loop_final_order_row)
                    if is_quote_order:
                        # remaining in USDT: total quote − filled notional (executed contracts × avg price)
                        avg_fill = _bingx_order_avg_price(loop_final_order_row, current_prepared.get('price'))
                        filled_notional = executed_qty * avg_fill
                        rem = _safe_decimal(total_qty) - filled_notional
                        if rem < 0:
                            rem = Decimal('0')
                        # price precision for USDT remainder
                        remaining_qty = format(rem, 'f').rstrip('0').rstrip('.') or '0'
                    else:
                        remaining_qty = _bingx_remaining_qty(total_qty, executed_qty, quantity_precision)
                    # Fully filled order: quoteOrderQty rounding must not look like an unfilled remainder.
                    if final_status == 'FILLED':
                        remaining_qty = '0'
                    attempt_entry['finalStatus'] = final_status
                    attempt_entry['executedQty'] = str(executed_qty)
                    attempt_entry['remainingQty'] = remaining_qty

                    actual_fill_result = _bingx_fetch_actual_fills(
                        client,
                        current_prepared['symbol'],
                        order_id,
                        attempt_entry.get('observedStartedAt'),
                        attempt_entry.get('observedCompletedAt'),
                    ) if order_id and executed_qty > 0 else {'fills': []}
                    actual_fills = actual_fill_result.get('fills') or []
                    if actual_fills:
                        attempt_entry['fills'] = actual_fills
                        attempt_entry['fillSource'] = actual_fill_result.get('source') or 'allFillOrders'
                        if actual_fill_result.get('incomeMatchedRowCount') not in (None, ''):
                            attempt_entry['incomeMatchedRowCount'] = actual_fill_result.get('incomeMatchedRowCount')
                    else:
                        attempt_entry['fills'] = _bingx_build_fill_parts(attempt_entry.get('snapshots', []), current_prepared.get('price'))
                        attempt_entry['fillSource'] = 'order_poll_delta'
                        if actual_fill_result.get('error'):
                            attempt_entry['fillFetchError'] = actual_fill_result.get('error')
                    fill_rows = [fill for fill in attempt_entry.get('fills', []) if isinstance(fill, dict)]
                    total_fill_qty = sum((_safe_decimal(fill.get('qty')) for fill in fill_rows), Decimal('0'))
                    total_fill_notional = sum((_safe_decimal(fill.get('qty')) * _safe_decimal(fill.get('price')) for fill in fill_rows), Decimal('0'))
                    attempt_entry['avgFillPrice'] = str((total_fill_notional / total_fill_qty) if total_fill_qty > 0 else _bingx_order_avg_price(loop_final_order_row, current_prepared.get('price')))
                    attempt_entry['commissionTotal'] = str(sum((abs(_safe_decimal(fill.get('commission'))) for fill in fill_rows), Decimal('0')))
                    attempt_entry['commissionCurrency'] = next((str(fill.get('commissionCurrency') or '') for fill in fill_rows if str(fill.get('commissionCurrency') or '').strip()), _bingx_order_commission_currency(loop_final_order_row))

                    if final_status == 'FILLED' or Decimal(remaining_qty) <= 0:
                        break

                    # IOC (open/add): exchange already cancelled the unfilled part —
                    # remaining qty → next book level via repost, don't treat as error.
                    if open_tif == 'IOC' and final_status in ('CANCELED', 'EXPIRED', 'PARTIALLY_FILLED'):
                        if attempt_index >= max_reposts:
                            break
                        if Decimal(remaining_qty) <= 0:
                            break
                        _set_stage(f'{stage_prefix}prepare_repost_price')
                        repost_kind = 'usdt' if is_quote_order else 'contracts'
                        repost_prepared = client.prepare_limit_order(
                            symbol=symbol, side=side, qty=remaining_qty, price=None, qty_kind=repost_kind,
                        )
                        current_prepared = repost_prepared
                        is_quote_order = str(current_prepared.get('qtyKind') or '').lower() in ('usdt', 'quote', 'quote_usdt', 'notional') or current_prepared.get('quoteOrderQty') not in (None, '')
                        request_payload['repostCount'] = request_payload.get('repostCount', 0) + 1
                        continue

                    if final_status not in ('NEW', 'PARTIALLY_FILLED'):
                        break

                    if not order_id:
                        break

                    _set_stage(f"{stage_prefix}cancel_remainder" if attempt_index == 0 else f"{stage_prefix}cancel_remainder_repost")
                    cancel_result = client.cancel_order(current_prepared['symbol'], order_id=order_id)
                    attempt_entry['cancelResult'] = cancel_result

                    if attempt_index >= max_reposts:
                        break

                    _set_stage(f'{stage_prefix}prepare_repost_price')
                    repost_kind = 'usdt' if is_quote_order else 'contracts'
                    repost_prepared = client.prepare_limit_order(symbol=symbol, side=side, qty=remaining_qty, price=None, qty_kind=repost_kind)
                    current_prepared = repost_prepared
                    is_quote_order = str(current_prepared.get('qtyKind') or '').lower() in ('usdt', 'quote', 'quote_usdt', 'notional') or current_prepared.get('quoteOrderQty') not in (None, '')
                    request_payload['repostCount'] = request_payload.get('repostCount', 0) + 1

                return loop_result, loop_final_order_row, remaining_qty, order_attempts, loop_effective_position_side

            if target_mode_close_then_open:
                quantity_precision = int((prepared.get('contract') or {}).get('quantityPrecision') or 0)
                close_initial_qty = _bingx_quantize_contract_qty_up(close_position_qty, quantity_precision)
                request_payload['targetDirectionCloseInitialQty'] = close_initial_qty
                if close_initial_qty == '0':
                    result = {
                        'code': -1,
                        'msg': f'target-direction close qty is below executable precision: {close_position_qty}'
                    }
                    final_order_row = {}
                    remaining_qty = '0'
                    order_attempts = []
                else:
                    close_initial_side = 'buy' if str(close_position_side or '').upper() == 'SHORT' else 'sell'
                    _set_stage('target_direction_prepare_initial_close')
                    close_initial_prepared = client.prepare_limit_order(
                        symbol=symbol,
                        side=close_initial_side,
                        qty=close_initial_qty,
                        price=None,
                        qty_kind='contracts',
                    )
                    result, final_order_row, remaining_qty, order_attempts, effective_position_side = _run_limit_repost_loop(
                        close_initial_prepared,
                        close_position_side,
                        effective_reduce_only,
                        stage_prefix='target_close_initial_',
                    )
                    order_attempts = [{**item, 'phase': 'target-close-initial'} for item in order_attempts]
            else:
                result, final_order_row, remaining_qty, order_attempts, effective_position_side = _run_limit_repost_loop(
                    prepared,
                    api_position_side,
                    effective_reduce_only,
                    stage_prefix='',
                )

            if target_mode_close_then_open and isinstance(result, dict) and result.get('code') in (None, 0, '0'):
                close_phase_attempts = []
                close_side_key = str(close_position_side or '').upper()
                quantity_precision = int((prepared.get('contract') or {}).get('quantityPrecision') or 0)
                max_close_passes = 8
                close_verify_error = ''
                close_still_open_qty = Decimal('0')

                for close_pass in range(max_close_passes):
                    try:
                        _set_stage('target_direction_verify_close_positions')
                        positions_after_close = client.get_positions(prepared['symbol'])
                        request_payload['positionPayloadAfterCloseRaw'] = positions_after_close
                        position_rows_after_close = _bingx_position_rows(positions_after_close, prepared['symbol'])
                        verified_buckets = _bingx_position_buckets(positions_after_close, prepared['symbol'])
                        request_payload['positionRowsAfterCloseRaw'] = position_rows_after_close
                        request_payload['positionBucketsAfterClose'] = {k: float(v) for k, v in verified_buckets.items()}
                        if close_side_key in ('LONG', 'SHORT'):
                            close_still_open_qty = verified_buckets.get(close_side_key, Decimal('0')) or Decimal('0')
                        else:
                            close_still_open_qty = Decimal('0')
                        close_phase_attempts.append({
                            'pass': close_pass + 1,
                            'remainingPositionQty': str(close_still_open_qty),
                            'buckets': {k: float(v) for k, v in verified_buckets.items()},
                        })
                        request_payload['targetDirectionCloseStillOpenQty'] = str(close_still_open_qty)
                    except Exception as verify_error:
                        close_verify_error = str(verify_error)
                        request_payload['targetDirectionCloseVerifyError'] = close_verify_error
                        break

                    if close_still_open_qty <= 0:
                        break

                    if close_pass >= max_close_passes - 1:
                        break

                    _set_stage('target_direction_prepare_close_retry')
                    close_retry_qty = _bingx_quantize_contract_qty_up(close_still_open_qty, quantity_precision)
                    if close_retry_qty == '0':
                        close_verify_error = f'remaining opposite leg {close_still_open_qty} is below executable precision'
                        request_payload['targetDirectionCloseVerifyError'] = close_verify_error
                        break
                    close_retry_side = 'buy' if close_side_key == 'SHORT' else 'sell'
                    close_retry_prepared = client.prepare_limit_order(
                        symbol=symbol,
                        side=close_retry_side,
                        qty=close_retry_qty,
                        price=None,
                        qty_kind='contracts',
                    )
                    close_result, close_final_order_row, close_remaining_qty, close_order_attempts, effective_position_side = _run_limit_repost_loop(
                        close_retry_prepared,
                        close_side_key,
                        effective_reduce_only,
                        stage_prefix=f'target_close_retry_{close_pass + 1}_',
                    )
                    close_phase_attempts[-1]['closeOrderRemainingQty'] = close_remaining_qty
                    close_phase_attempts[-1]['closeOrderAttempts'] = [
                        {
                            'attempt': item.get('attempt'),
                            'placedQty': item.get('placedQty'),
                            'placedPrice': item.get('placedPrice'),
                            'orderId': item.get('orderId'),
                            'finalStatus': item.get('finalStatus'),
                            'executedQty': item.get('executedQty'),
                            'remainingQty': item.get('remainingQty'),
                        }
                        for item in close_order_attempts
                    ]
                    result = close_result
                    final_order_row = close_final_order_row
                    remaining_qty = close_remaining_qty
                    order_attempts.extend([{**item, 'phase': f'target-close-retry-{close_pass + 1}'} for item in close_order_attempts])
                    if not isinstance(result, dict) or result.get('code') not in (None, 0, '0'):
                        break

                request_payload['targetDirectionCloseAttempts'] = close_phase_attempts

                if close_still_open_qty <= 0 and not close_verify_error and isinstance(result, dict) and result.get('code') in (None, 0, '0'):
                    _set_stage('target_direction_verify_flat_before_open')
                    positions_before_open = client.get_positions(prepared['symbol'])
                    request_payload['positionPayloadBeforeTargetOpenRaw'] = positions_before_open
                    position_rows_before_open = _bingx_position_rows(positions_before_open, prepared['symbol'])
                    buckets_before_open = _bingx_position_buckets(positions_before_open, prepared['symbol'])
                    request_payload['positionRowsBeforeTargetOpenRaw'] = position_rows_before_open
                    request_payload['positionBucketsBeforeTargetOpen'] = {k: float(v) for k, v in buckets_before_open.items()}
                    if (buckets_before_open.get(close_side_key, Decimal('0')) or Decimal('0')) > 0:
                        close_verify_error = 'opposite leg still exists before target open'
                        request_payload['targetDirectionCloseVerifyError'] = close_verify_error

                if close_still_open_qty <= 0 and not close_verify_error and isinstance(result, dict) and result.get('code') in (None, 0, '0'):
                    open_side = 'buy' if target_direction == 'long' else 'sell'
                    open_position_side = 'LONG' if target_direction == 'long' else 'SHORT'
                    _set_stage('target_direction_prepare_open')
                    # Flip: close just freed isolated margin — refresh balance before
                    # the open pre-trade check (BingX available can lag a beat).
                    time.sleep(0.35)
                    try:
                        fresh_bal = client.get_balance()
                        margin_ops['balance'] = fresh_bal
                        request_payload['riskControl']['balanceRefreshAfterClose'] = True
                        request_payload['riskControl']['availableAfterClose'] = _bingx_account_available(fresh_bal)
                    except Exception as refresh_exc:
                        request_payload['riskControl']['balanceRefreshAfterCloseError'] = str(refresh_exc)
                    target_open_qty_kind = 'usdt'
                    open_prepared = client.prepare_limit_order(symbol=symbol, side=open_side, qty=quantity, price=None, qty_kind=target_open_qty_kind)
                    request_payload['targetOpenQtyKind'] = target_open_qty_kind
                    pre_abort = None
                    if risk_control_enabled and not dry_run:
                        pre_abort = _ensure_pretrade_margin(open_position_side)
                    if pre_abort:
                        pretrade_aborted = True
                        result = pre_abort
                        final_order_row = {}
                        remaining_qty = '0'
                    else:
                        open_result, open_final_order_row, open_remaining_qty, open_attempts, open_effective_position_side = _run_limit_repost_loop(
                            open_prepared,
                            open_position_side,
                            None,
                            stage_prefix='target_open_',
                        )
                        effective_position_side = open_effective_position_side or open_position_side
                        request_payload['targetOpenAttempts'] = [
                            {
                                'attempt': item.get('attempt'),
                                'placedQty': item.get('placedQty'),
                                'placedPrice': item.get('placedPrice'),
                                'orderId': item.get('orderId'),
                                'finalStatus': item.get('finalStatus'),
                                'executedQty': item.get('executedQty'),
                                'remainingQty': item.get('remainingQty'),
                            }
                            for item in open_attempts
                        ]
                        request_payload['targetOpenFinalRemainingQty'] = open_remaining_qty
                        result = open_result
                        final_order_row = open_final_order_row
                        remaining_qty = open_remaining_qty
                        order_attempts.extend([{**item, 'phase': 'target-open'} for item in open_attempts])

                        if isinstance(result, dict) and result.get('code') in (None, 0, '0'):
                            _set_stage('target_direction_verify_final_positions')
                            final_positions = client.get_positions(prepared['symbol'])
                            request_payload['positionPayloadAfterTargetOpenRaw'] = final_positions
                            final_position_rows = _bingx_position_rows(final_positions, prepared['symbol'])
                            final_buckets = _bingx_position_buckets(final_positions, prepared['symbol'])
                            request_payload['positionRowsAfterTargetOpenRaw'] = final_position_rows
                            request_payload['positionBucketsAfterTargetOpen'] = {k: float(v) for k, v in final_buckets.items()}
                            expected_side = 'LONG' if target_direction == 'long' else 'SHORT'
                            opposite_side = 'SHORT' if expected_side == 'LONG' else 'LONG'
                            final_expected_qty = final_buckets.get(expected_side, Decimal('0')) or Decimal('0')
                            final_opposite_qty = final_buckets.get(opposite_side, Decimal('0')) or Decimal('0')
                            if final_expected_qty <= 0 or final_opposite_qty > 0:
                                result = {
                                    'code': -1,
                                    'msg': f'target-direction final reconcile failed: expected {expected_side} > 0 and {opposite_side} == 0, got {final_buckets}'
                                }

            request_payload['orderAttempts'] = [
                {
                    'attempt': item.get('attempt'),
                    'phase': item.get('phase', 'primary'),
                    'placedQty': item.get('placedQty'),
                    'placedPrice': item.get('placedPrice'),
                    'orderId': item.get('orderId'),
                    'finalStatus': item.get('finalStatus'),
                    'executedQty': item.get('executedQty'),
                    'remainingQty': item.get('remainingQty'),
                    'observedStartedAt': item.get('observedStartedAt'),
                    'observedCompletedAt': item.get('observedCompletedAt'),
                    'avgFillPrice': item.get('avgFillPrice'),
                    'commissionTotal': item.get('commissionTotal'),
                    'commissionCurrency': item.get('commissionCurrency'),
                    'fillSource': item.get('fillSource'),
                    'fillFetchError': item.get('fillFetchError'),
                    'fills': item.get('fills', []),
                }
                for item in order_attempts
            ]
            if final_order_row:
                request_payload['finalOrderStatus'] = _bingx_order_status(final_order_row)
                request_payload['finalExecutedQty'] = str(_bingx_order_executed_qty(final_order_row))
                request_payload['finalRemainingQty'] = remaining_qty

            # Post-trade risk adjust only when an order was actually attempted
            # (skip after pre-trade abort — position did not change from our fill).
            if risk_control_enabled and not pretrade_aborted:
                _set_stage('get_balance_after')
                balance = margin_ops.get('balance') or client.get_balance()
                equity = _bingx_account_equity(balance)
                _set_stage('get_positions_after')
                positions_after = client.get_positions(prepared['symbol'])
                after_position = _bingx_extract_position(positions_after, prepared['symbol'], effective_position_side)
                final_qty = _bingx_position_qty(after_position)
                liquidation_price = after_position.get('liquidationPrice') or after_position.get('liquidPrice')
                position_margin = after_position.get('positionMargin') or after_position.get('isolatedMargin') or after_position.get('margin')
                mark_price = float(prepared['price'])
                final_notional = abs(mark_price * final_qty)
                unrealized_after = _bingx_account_unrealized(balance)
                # Terminal "Risk" field = riskRate = maintenanceMargin / isolated.
                # maintenance ≈ 0.3% × notional (verified on live position).
                # Target isolated so riskRate == riskPct% after adjust.
                current_rr = 0.0
                try:
                    current_rr = float(after_position.get('riskRate') or 0)
                except Exception:
                    current_rr = 0.0
                current_iso_for_maint = 0.0
                try:
                    if position_margin not in (None, ''):
                        current_iso_for_maint = abs(float(position_margin))
                except Exception:
                    current_iso_for_maint = 0.0
                if current_rr > 0 and current_iso_for_maint > 0:
                    maintenance_margin = current_rr * current_iso_for_maint
                else:
                    maintenance_margin = 0.003 * final_notional
                target_risk_rate = max(0.0, risk_pct) / 100.0
                if target_risk_rate > 0 and maintenance_margin > 0:
                    target_margin = maintenance_margin / target_risk_rate
                else:
                    target_margin = 0.0
                target_used = 0.0  # legacy; terminal risk is riskRate
                request_payload['riskControl'].update({
                    'effectivePositionSide': effective_position_side,
                    'finalQty': final_qty,
                    'finalNotional': final_notional,
                    'unrealizedProfit': unrealized_after,
                    'currentRiskRate': current_rr,
                    'maintenanceMargin': maintenance_margin,
                    'targetRiskRate': target_risk_rate,
                    'targetUsedMargin': target_used,
                    'targetMargin': target_margin,
                    'liquidationPrice': liquidation_price,
                    'currentPositionMargin': position_margin,
                })
                current_margin = 0.0
                try:
                    if position_margin not in (None, ''):
                        current_margin = abs(float(position_margin))
                except Exception:
                    current_margin = 0.0
                margin_pct_of_equity = None
                target_margin_pct_of_equity = None
                add_margin_pct_of_equity = None
                current_used = None
                if current_rr > 0:
                    margin_pct_of_equity = current_rr * 100.0
                target_margin_pct_of_equity = risk_pct
                request_payload['riskControl'].update({
                    'currentMarginValue': current_margin,
                    'currentUsedMargin': current_used,
                    'currentMarginPctOfEquity': margin_pct_of_equity,
                    'targetMarginPctOfEquity': target_margin_pct_of_equity,
                })
                margin_delta = target_margin - current_margin
                margin_delta_abs = abs(margin_delta)
                if equity > 0:
                    add_margin_pct_of_equity = (margin_delta_abs / equity) * 100.0
                pre_leverage = 0
                try:
                    pre_leverage = int(float((request_payload.get('riskControl') or {}).get('preTradeLeverage') or 0))
                except Exception:
                    pre_leverage = 0
                # Exchange will not allow isolated margin below ~notional/leverage.
                estimated_min_margin = 0.0
                if final_notional > 0 and pre_leverage > 0:
                    estimated_min_margin = final_notional / float(pre_leverage)
                if final_qty > 0 and margin_delta_abs > 0.00000001:
                    if margin_delta > 0:
                        _set_stage('add_margin')
                        try:
                            available = _bingx_account_available(margin_ops.get('balance') or {})
                            request_payload['riskControl']['availableBalance'] = available
                            actual_add = min(margin_delta_abs, available) if available > 0 else 0.0
                            shortfall = margin_delta_abs - actual_add
                            if shortfall > 0.00000001:
                                request_payload['riskControl']['addMarginShortfall'] = shortfall
                            if actual_add > 0.00000001:
                                add_resp = client.adjust_isolated_margin(prepared['symbol'], effective_position_side, actual_add, direction_type=1)
                                margin_ops['addMargin'] = add_resp
                                request_payload['riskControl']['addMargin'] = actual_add
                                if not _bingx_api_ok(add_resp):
                                    request_payload['riskControl']['addMarginFailed'] = True
                                    request_payload['riskControl']['addMarginApiCode'] = add_resp.get('code') if isinstance(add_resp, dict) else None
                                    request_payload['riskControl']['addMarginApiMsg'] = str((add_resp or {}).get('msg') or '') if isinstance(add_resp, dict) else str(add_resp)
                            else:
                                request_payload['riskControl']['addMarginSkipped'] = 'insufficient_balance'
                        except Exception as add_err:
                            margin_ops['addMarginError'] = str(add_err)
                            request_payload['riskControl']['addMarginError'] = str(add_err)
                    else:
                        _set_stage('reduce_margin')
                        effective_target = target_margin
                        if estimated_min_margin > target_margin + 0.00000001:
                            effective_target = estimated_min_margin
                            request_payload['riskControl']['targetMarginFallback'] = 'below_exchange_min'
                            request_payload['riskControl']['targetMarginRequested'] = target_margin
                            request_payload['riskControl']['targetMarginFloor'] = estimated_min_margin
                        reduce_amount = max(0.0, current_margin - effective_target)
                        request_payload['riskControl']['reduceMarginTarget'] = effective_target
                        if reduce_amount <= 0.00000001:
                            request_payload['riskControl']['reduceMarginSkipped'] = 'already_at_or_below_floor'
                        else:
                            reduce_resp = None
                            reduce_err_text = ''
                            try:
                                reduce_resp = client.adjust_isolated_margin(prepared['symbol'], effective_position_side, reduce_amount, direction_type=2)
                                margin_ops['reduceMargin'] = reduce_resp
                                request_payload['riskControl']['reduceMargin'] = reduce_amount
                                request_payload['riskControl']['reduceMarginApiCode'] = reduce_resp.get('code') if isinstance(reduce_resp, dict) else None
                                if not _bingx_api_ok(reduce_resp):
                                    request_payload['riskControl']['reduceMarginFailed'] = True
                                    request_payload['riskControl']['reduceMarginApiMsg'] = str(reduce_resp.get('msg') or '') if isinstance(reduce_resp, dict) else str(reduce_resp)
                                    # Fallback: retry a smaller chunk (half), still above floor.
                                    retry_amount = reduce_amount / 2.0
                                    if retry_amount > 0.00000001 and current_margin - retry_amount >= effective_target - 0.00000001:
                                        try:
                                            retry_resp = client.adjust_isolated_margin(prepared['symbol'], effective_position_side, retry_amount, direction_type=2)
                                            margin_ops['reduceMarginRetry'] = retry_resp
                                            request_payload['riskControl']['reduceMarginRetry'] = retry_amount
                                            request_payload['riskControl']['reduceMarginRetryApiCode'] = retry_resp.get('code') if isinstance(retry_resp, dict) else None
                                            if _bingx_api_ok(retry_resp):
                                                request_payload['riskControl']['reduceMarginFailed'] = False
                                                request_payload['riskControl']['reduceMargin'] = retry_amount
                                        except Exception as retry_err:
                                            margin_ops['reduceMarginRetryError'] = str(retry_err)
                            except Exception as reduce_err:
                                reduce_err_text = str(reduce_err)
                                margin_ops['reduceMarginError'] = reduce_err_text
                                request_payload['riskControl']['reduceMarginError'] = reduce_err_text
                request_payload['riskControl'].update({
                    'marginDeltaPctOfEquity': add_margin_pct_of_equity,
                    'marginDirection': 'add' if margin_delta > 0 else ('reduce' if margin_delta < 0 else 'none'),
                })
                if margin_delta_abs > 0.00000001:
                    _set_stage('verify_margin_after_adjust')
                    try:
                        positions_after_margin = client.get_positions(prepared['symbol'])
                        pos_after_margin = _bingx_extract_position(positions_after_margin, prepared['symbol'], effective_position_side)
                        request_payload['riskControl']['liquidationPriceAfterAdjust'] = pos_after_margin.get('liquidationPrice') or pos_after_margin.get('liquidPrice')
                        adjusted_margin = pos_after_margin.get('positionMargin') or pos_after_margin.get('isolatedMargin') or pos_after_margin.get('margin')
                        request_payload['riskControl']['marginAfterAdjust'] = adjusted_margin
                        try:
                            rr_after = float(pos_after_margin.get('riskRate') or 0)
                            request_payload['riskControl']['riskRateAfterAdjust'] = rr_after
                            request_payload['riskControl']['marginAfterAdjustPctOfEquity'] = rr_after * 100.0
                        except Exception:
                            pass
                        balance_verify = client.get_balance()
                        equity_verify = _bingx_account_equity(balance_verify) or equity
                        unrealized_verify = _bingx_account_unrealized(balance_verify)
                        if adjusted_margin not in (None, ''):
                            try:
                                after_isolated = abs(float(adjusted_margin))
                                after_used = after_isolated - unrealized_verify
                                request_payload['riskControl']['unrealizedProfitAfter'] = unrealized_verify
                                request_payload['riskControl']['usedMarginAfterAdjust'] = after_used
                                if margin_delta < 0 and current_margin > 0 and after_isolated >= current_margin - 0.00000001:
                                    request_payload['riskControl']['reduceMarginNoEffect'] = True
                            except Exception:
                                pass
                    except Exception as verify_err:
                        request_payload['riskControl']['verifyMarginError'] = str(verify_err)
                request_payload['riskControl']['ops'] = margin_ops

            _set_stage('done')
            if margin_ops and isinstance(result, dict):
                result['_riskControl'] = margin_ops
            return result

        result = await asyncio.to_thread(_run)
        business_error = ''
        if not dry_run and isinstance(result, dict) and result.get('code') not in (None, 0, '0'):
            business_error = str(result.get('msg') or result.get('code') or 'bingx_error')
        elif request_payload.get('finalRemainingQty') not in (None, '', '0'):
            try:
                rem = Decimal(str(request_payload.get('finalRemainingQty')))
                if rem > 0:
                    # Dust below instrument min → not an error (quote rounding / partial level).
                    limits = (destination or {}).get('limits') or {}
                    unit = str(request_payload.get('openQtyKind') or request_payload.get('qtyKind') or '').lower()
                    if unit in ('usdt', 'quote', 'quote_usdt', 'notional'):
                        min_rem = Decimal(str(limits.get('minUsdt') or 1))
                    else:
                        min_rem = Decimal(str(limits.get('minQty') or 0.001))
                    if min_rem <= 0:
                        min_rem = Decimal('0.001')
                    if rem >= min_rem:
                        business_error = f"unfilled_remainder:{request_payload.get('finalRemainingQty')}"
            except Exception:
                pass

        response_payload = {
            'broker': broker,
            'symbol': request_payload.get('symbol', symbol),
            'category': category,
            'qty': request_payload.get('qty', quantity),
            'request': request_payload,
            'results': result,
        }
        if dry_run:
            response_payload['dryRun'] = True
        if business_error:
            response_payload['error'] = business_error
        return response_payload
    except Exception as e:
        return {
            'broker': broker,
            'symbol': symbol,
            'category': category,
            'qty': quantity,
            'request': request_payload,
            'error': str(e),
            'results': {'error': str(e), 'stage': request_payload.get('stage'), 'stageTrace': request_payload.get('stageTrace', [])},
        }


async def _execute_finam(payload: Dict[str, Any], destination: Dict[str, Any]) -> Dict[str, Any]:
    side_raw = str(destination.get('side', payload['side'])).strip().lower()
    signal_mode = str(destination.get('signalMode') or payload.get('signalMode') or '').strip().lower()
    if side_raw in ('2long', '2short', 'long', 'short') or signal_mode == 'target-direction':
        return await _execute_via_workspace_executor(payload, destination)

    broker = destination['broker']
    symbol = destination['symbol']
    side = side_raw
    quantity = destination.get('qty', payload['qty'])
    exchange = destination.get('exchange', 'MOEX')
    price = destination.get('price') or payload.get('price')
    dry_run = bool(destination.get('dryRun', False) or payload.get('dryRun', False))
    request_payload = {
        'symbol': symbol,
        'side': side,
        'qty': quantity,
        'exchange': exchange,
        'price': price,
        'dryRun': dry_run,
    }

    try:
        client = FinamBroker()
        result = await client.place_order(symbol=symbol, side=side, qty=quantity, exchange=exchange, price=price, dry_run=dry_run)
        return {
            'broker': broker,
            'symbol': symbol,
            'exchange': exchange,
            'qty': quantity,
            'request': request_payload,
            'results': result,
        }
    except Exception as e:
        return {
            'broker': broker,
            'symbol': symbol,
            'exchange': exchange,
            'qty': quantity,
            'request': request_payload,
            'error': str(e),
            'results': {'error': str(e)},
        }


async def _execute_schwab(payload: Dict[str, Any], destination: Dict[str, Any]) -> Dict[str, Any]:
    broker = destination['broker']
    symbol = destination['symbol']
    side = str(destination.get('side', payload['side'])).lower()
    quantity = destination.get('qty', payload['qty'])
    account_id = destination.get('account', 'primary')
    dry_run = bool(destination.get('dryRun', False) or payload.get('dryRun', False))
    request_payload = {
        'account': account_id,
        'symbol': symbol,
        'side': side,
        'qty': quantity,
        'dryRun': dry_run,
    }

    try:
        def _run() -> Dict[str, Any]:
            client = SchwabBroker()
            return client.place_equity_order(account_id=account_id, symbol=symbol, side=side, qty=quantity, dry_run=dry_run)

        result = await asyncio.to_thread(_run)
        return {
            'broker': broker,
            'symbol': symbol,
            'account': account_id,
            'qty': quantity,
            'request': request_payload,
            'results': result,
        }
    except Exception as e:
        return {
            'broker': broker,
            'symbol': symbol,
            'account': account_id,
            'qty': quantity,
            'request': request_payload,
            'error': str(e),
            'results': {'error': str(e)},
        }


async def _execute_destination(payload: Dict[str, Any], destination: Dict[str, Any]) -> Dict[str, Any]:
    broker = destination['broker']
    if broker == 'bybit':
        return await _execute_bybit(payload, destination)
    if broker == 'bingx':
        return await _execute_bingx(payload, destination)
    if broker == 'finam':
        return await _execute_finam(payload, destination)
    if broker == 'schwab':
        return await _execute_schwab(payload, destination)
    return await _execute_via_workspace_executor(payload, destination)


async def _execute_destination_with_timeout(payload: Dict[str, Any], destination: Dict[str, Any]) -> Dict[str, Any]:
    try:
        return await asyncio.wait_for(_execute_destination(payload, destination), timeout=DESTINATION_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        broker = destination.get('broker')
        symbol = destination.get('symbol')
        venue = destination.get('category') or destination.get('exchange') or destination.get('account')
        request_payload = destination.get('request') or {}
        return {
            'broker': broker,
            'symbol': symbol,
            'category': destination.get('category'),
            'exchange': destination.get('exchange'),
            'account': destination.get('account'),
            'qty': destination.get('qty', payload.get('qty')),
            'request': {
                'symbol': symbol,
                'side': destination.get('side', payload.get('side')),
                'qty': destination.get('qty', payload.get('qty')),
                'venue': venue,
                'stage': request_payload.get('stage'),
                'stageTrace': request_payload.get('stageTrace', []),
            },
            'error': f"timeout after {DESTINATION_TIMEOUT_SECONDS}s",
            'results': {
                'error': f"timeout after {DESTINATION_TIMEOUT_SECONDS}s",
                'stage': request_payload.get('stage'),
                'stageTrace': request_payload.get('stageTrace', []),
            },
        }


async def execute_route(payload: Dict[str, Any], route: Dict[str, Any]) -> Dict[str, Any]:
    destinations: List[Dict[str, Any]] = route.get('destinations', [])
    results: List[Dict[str, Any]] = []
    for destination in destinations:
        results.append(await _execute_destination_with_timeout(payload, destination))

    return {
        'routeId': route.get('id'),
        'routeName': route.get('name'),
        'destinations': results,
    }


def execute_route_sync(payload: Dict[str, Any], route: Dict[str, Any]) -> Dict[str, Any]:
    return asyncio.run(execute_route(payload, route))
