#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import importlib.util
import math
import time
from datetime import datetime, timezone
from decimal import Decimal, ROUND_UP
from typing import Any, Dict, List

from bingx_adapter import BingXBroker
from bybit_adapter import BybitBroker
from finam_adapter import FinamBroker
from schwab_adapter import SchwabBroker
from settings import SMART_EXECUTOR_PATH


DESTINATION_TIMEOUT_SECONDS = 25


def load_smart_executor_module():
    spec = importlib.util.spec_from_file_location('smart_order_executor', SMART_EXECUTOR_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


async def _execute_via_workspace_executor(payload: Dict[str, Any], destination: Dict[str, Any]) -> Dict[str, Any]:
    module = load_smart_executor_module()

    broker = destination['broker']
    symbol = destination['symbol']
    exchange = destination['exchange']
    side = str(destination.get('side', payload['side'])).lower()
    quantity = int(float(destination.get('qty', payload['qty'])))
    use_limit = str(destination.get('executionMode', 'maker')).lower() != 'market'
    request_payload = {
        'ticker': symbol,
        'side': side,
        'quantity': quantity,
        'exchange': exchange,
        'use_limit': use_limit,
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
    side = side_raw
    if side_raw in ('2long', 'long'):
        signal_mode = 'target-direction'
        target_direction = 'long'
        side = 'buy'
    elif side_raw in ('2short', 'short'):
        signal_mode = 'target-direction'
        target_direction = 'short'
        side = 'sell'
    elif signal_mode == 'target-direction' and side_raw in ('long', 'short'):
        target_direction = side_raw
        side = 'buy' if target_direction == 'long' else 'sell'

    quantity = destination.get('qty', payload.get('qty'))
    qty_kind = str(destination.get('qtyKind') or payload.get('qtyKind') or 'contracts').lower()
    category = destination.get('category', 'linear')
    execution_mode = destination.get('executionMode', destination.get('mode', 'market'))
    reduce_only = bool(destination.get('reduceOnly', False))
    dry_run = bool(destination.get('dryRun', False) or payload.get('dryRun', False))
    request_payload = {
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

            if signal_mode == 'target-direction':
                positions = client.get_positions(category=category, symbol=symbol)
                request_payload['positionListRaw'] = positions
                rows = (((positions or {}).get('result') or {}).get('list') or [])
                long_size = 0.0
                short_size = 0.0
                long_idx = 0
                short_idx = 0
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
                    row_side = str(row.get('side') or '').strip().lower()
                    if row_side == 'buy' and size > 0:
                        long_size += size
                        if row_idx:
                            long_idx = row_idx
                    elif row_side == 'sell' and size > 0:
                        short_size += size
                        if row_idx:
                            short_idx = row_idx
                request_payload['positionSnapshot'] = {
                    'longSize': long_size,
                    'shortSize': short_size,
                    'longPositionIdx': long_idx,
                    'shortPositionIdx': short_idx,
                }

                desired_side = 'buy' if target_direction == 'long' else 'sell'
                if target_direction == 'long' and short_size > 0:
                    target_mode_close_then_open = True
                    close_qty = short_size
                    close_side = 'buy'
                    close_position_idx = short_idx or 2
                elif target_direction == 'short' and long_size > 0:
                    target_mode_close_then_open = True
                    close_qty = long_size
                    close_side = 'sell'
                    close_position_idx = long_idx or 1
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
                    close_order_result = client.place_order(
                        symbol=symbol,
                        side=close_side,
                        qty=close_qty,
                        category=category,
                        order_type='Market',
                        reduce_only=True,
                        position_idx=close_position_idx,
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
                    position_idx = 1 if desired_side == 'buy' else 2
                request_payload['openPositionIdx'] = int(position_idx)

            if str(execution_mode).lower() == 'limit':
                price = destination.get('price') or payload.get('price')
                request_payload['price'] = price
                return client.place_order(
                    symbol=symbol,
                    side=side,
                    qty=quantity,
                    category=category,
                    order_type='Limit',
                    price=price,
                    reduce_only=reduce_only,
                    position_idx=None if position_idx in (None, '') else int(position_idx),
                )
            return client.place_order(
                symbol=symbol,
                side=side,
                qty=quantity,
                category=category,
                order_type='Market',
                reduce_only=reduce_only,
                position_idx=None if position_idx in (None, '') else int(position_idx),
            )

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
    return str(order_row.get('status') or '').strip().upper()


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
        started_ms = max(0, completed_ms - 6 * 60 * 60 * 1000)

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
    side = side_raw
    if side_raw in ('2long', 'long'):
        signal_mode = 'target-direction'
        target_direction = 'long'
        side = 'buy'
    elif side_raw in ('2short', 'short'):
        signal_mode = 'target-direction'
        target_direction = 'short'
        side = 'sell'
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
    request_payload = {
        'symbol': symbol,
        'side': side,
        'qty': quantity,
        'qtyKind': qty_kind,
        'category': category,
        'executionMode': execution_mode,
        'positionSide': position_side,
        'signalMode': signal_mode,
        'targetDirection': target_direction,
        'stage': 'init',
        'stageTrace': ['init'],
    }
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
                        'quantity': prepared['quantity'],
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
                mark_price = float(prepared['price'])
                incoming_qty = abs(float(prepared['quantity']))
                expected_final_qty = max(0.0, before_qty + incoming_qty)
                allowed_loss = max(0.0, equity * (risk_pct / 100.0))
                expected_notional = abs(mark_price * expected_final_qty)
                target_margin_pre = min(expected_notional, allowed_loss) if allowed_loss > 0 else 0.0
                raw_leverage = (expected_notional / target_margin_pre) if target_margin_pre > 0 else 1.0
                leverage_cap = 125
                if is_non_crypto_index:
                    try:
                        contract_leverage = int(float(contract_meta.get('maxLongLeverage') or contract_meta.get('maxShortLeverage') or contract_meta.get('maxLeverage') or 0))
                        if contract_leverage > 0:
                            leverage_cap = min(leverage_cap, contract_leverage)
                    except Exception:
                        leverage_cap = 125
                leverage = max(1, min(leverage_cap, int(math.ceil(raw_leverage))))
                request_payload['riskControl'] = {
                    'mode': 'non_crypto' if is_non_crypto_index else 'standard',
                    'equity': equity,
                    'allowedLoss': allowed_loss,
                    'beforeQty': before_qty,
                    'incomingQty': incoming_qty,
                    'expectedFinalQty': expected_final_qty,
                    'expectedNotional': expected_notional,
                    'preTradeLeverage': leverage,
                }
                _set_stage('set_margin_type')
                margin_ops['setMarginType'] = client.set_margin_type(prepared['symbol'], 'ISOLATED')
                _set_stage('set_leverage')
                leverage_side = requested_position_side
                if leverage_side not in ('LONG', 'SHORT'):
                    leverage_side = 'LONG' if side == 'buy' else 'SHORT'
                margin_ops['setLeverage'] = client.set_leverage(prepared['symbol'], leverage_side, leverage)

            effective_position_side = api_position_side if api_position_side != 'BOTH' else requested_position_side
            def _run_limit_repost_loop(loop_prepared: Dict[str, Any], loop_position_side: str, loop_reduce_only: Any, stage_prefix: str = ''):
                order_attempts = []
                poll_plan_ms = [350, 700, 1200]
                max_reposts = 5
                current_prepared = dict(loop_prepared)
                remaining_qty = str(current_prepared.get('quantity'))
                loop_result = None
                loop_final_order_row = {}
                loop_effective_position_side = loop_position_side

                for attempt_index in range(max_reposts + 1):
                    current_prepared['quantity'] = remaining_qty
                    _set_stage(f"{stage_prefix}place_limit_order" if attempt_index == 0 else f"{stage_prefix}place_limit_order_repost")
                    attempt_result = client.place_limit_order(
                        current_prepared,
                        position_side=loop_position_side,
                        reduce_only=loop_reduce_only,
                        client_order_id=client_order_id,
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

                    if not isinstance(attempt_result, dict) or attempt_result.get('code') not in (None, 0, '0'):
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
                    total_qty = current_prepared.get('quantity') or '0'
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
                    remaining_qty = _bingx_remaining_qty(total_qty, executed_qty, quantity_precision)
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
                    repost_prepared = client.prepare_limit_order(symbol=symbol, side=side, qty=remaining_qty, price=None, qty_kind='contracts')
                    current_prepared = repost_prepared
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
                    target_open_qty_kind = 'usdt'
                    open_prepared = client.prepare_limit_order(symbol=symbol, side=open_side, qty=quantity, price=None, qty_kind=target_open_qty_kind)
                    request_payload['targetOpenQtyKind'] = target_open_qty_kind
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

            if risk_control_enabled:
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
                allowed_loss = max(0.0, equity * (risk_pct / 100.0))
                target_margin = min(final_notional, allowed_loss) if allowed_loss > 0 else 0.0
                request_payload['riskControl'].update({
                    'effectivePositionSide': effective_position_side,
                    'finalQty': final_qty,
                    'finalNotional': final_notional,
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
                if equity > 0:
                    margin_pct_of_equity = (current_margin / equity) * 100.0
                    target_margin_pct_of_equity = (target_margin / equity) * 100.0
                request_payload['riskControl'].update({
                    'currentMarginValue': current_margin,
                    'currentMarginPctOfEquity': margin_pct_of_equity,
                    'targetMarginPctOfEquity': target_margin_pct_of_equity,
                })
                add_margin_amount = max(0.0, target_margin - current_margin)
                if equity > 0:
                    add_margin_pct_of_equity = (add_margin_amount / equity) * 100.0
                if final_qty > 0 and add_margin_amount > 0.00000001:
                    _set_stage('add_margin')
                    margin_ops['addMargin'] = client.adjust_isolated_margin(prepared['symbol'], effective_position_side, add_margin_amount, direction_type=1)
                    request_payload['riskControl']['addMargin'] = add_margin_amount
                request_payload['riskControl'].update({
                    'addMarginPctOfEquity': add_margin_pct_of_equity,
                })
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
                if Decimal(str(request_payload.get('finalRemainingQty'))) > 0:
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
    broker = destination['broker']
    symbol = destination['symbol']
    side = str(destination.get('side', payload['side'])).lower()
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
