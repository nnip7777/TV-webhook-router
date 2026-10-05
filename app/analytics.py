#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Tuple
from zoneinfo import ZoneInfo

from settings import ANALYTICS_DB_PATH

LOCAL_TZ = ZoneInfo('Europe/Moscow')

_DB_LOCK = threading.Lock()

SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS signals (
    signal_id TEXT PRIMARY KEY,
    received_at TEXT NOT NULL,
    origin TEXT,
    route_id TEXT,
    route_name TEXT,
    source_ticker TEXT,
    side TEXT,
    qty_text TEXT,
    payload_json TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS executions (
    execution_id TEXT PRIMARY KEY,
    signal_id TEXT NOT NULL REFERENCES signals(signal_id) ON DELETE CASCADE,
    destination_index INTEGER NOT NULL,
    received_at TEXT NOT NULL,
    broker TEXT,
    symbol TEXT,
    venue TEXT,
    account TEXT,
    side TEXT,
    requested_qty_text TEXT,
    execution_mode TEXT,
    signal_mode TEXT,
    status TEXT,
    error_text TEXT,
    dry_run INTEGER NOT NULL DEFAULT 0,
    request_json TEXT,
    result_json TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(signal_id, destination_index)
);

CREATE TABLE IF NOT EXISTS orders (
    order_local_id TEXT PRIMARY KEY,
    execution_id TEXT NOT NULL REFERENCES executions(execution_id) ON DELETE CASCADE,
    signal_id TEXT NOT NULL REFERENCES signals(signal_id) ON DELETE CASCADE,
    broker_order_id TEXT,
    client_order_id TEXT,
    phase TEXT,
    attempt_no INTEGER,
    side TEXT,
    symbol TEXT,
    venue TEXT,
    placed_price REAL,
    requested_qty REAL,
    executed_qty REAL,
    remaining_qty REAL,
    avg_fill_price REAL,
    status TEXT,
    commission_total REAL,
    commission_currency TEXT,
    fill_count INTEGER NOT NULL DEFAULT 0,
    is_reduce_only INTEGER,
    observed_started_at TEXT,
    observed_completed_at TEXT,
    raw_json TEXT
);

CREATE TABLE IF NOT EXISTS fills (
    fill_id TEXT PRIMARY KEY,
    execution_id TEXT NOT NULL REFERENCES executions(execution_id) ON DELETE CASCADE,
    signal_id TEXT NOT NULL REFERENCES signals(signal_id) ON DELETE CASCADE,
    order_local_id TEXT REFERENCES orders(order_local_id) ON DELETE SET NULL,
    broker_order_id TEXT,
    fill_seq INTEGER,
    phase TEXT,
    observed_at TEXT,
    broker TEXT,
    symbol TEXT,
    venue TEXT,
    side TEXT,
    qty REAL,
    price REAL,
    notional REAL,
    commission REAL,
    commission_currency TEXT,
    liquidity_flag TEXT,
    position_effect TEXT,
    source_type TEXT,
    raw_json TEXT
);

CREATE TABLE IF NOT EXISTS open_lots (
    lot_id TEXT PRIMARY KEY,
    broker TEXT NOT NULL,
    symbol TEXT NOT NULL,
    venue TEXT NOT NULL,
    side TEXT NOT NULL,
    opened_at TEXT NOT NULL,
    remaining_qty REAL NOT NULL,
    remaining_commission REAL NOT NULL DEFAULT 0,
    open_price REAL NOT NULL,
    open_fill_id TEXT NOT NULL UNIQUE REFERENCES fills(fill_id) ON DELETE CASCADE,
    open_order_local_id TEXT REFERENCES orders(order_local_id) ON DELETE SET NULL,
    open_execution_id TEXT REFERENCES executions(execution_id) ON DELETE SET NULL,
    open_signal_id TEXT REFERENCES signals(signal_id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS round_trips (
    round_trip_id TEXT PRIMARY KEY,
    broker TEXT NOT NULL,
    symbol TEXT NOT NULL,
    venue TEXT NOT NULL,
    direction TEXT NOT NULL,
    opened_at TEXT NOT NULL,
    closed_at TEXT NOT NULL,
    holding_time_sec REAL,
    entry_qty REAL NOT NULL,
    exit_qty REAL NOT NULL,
    entry_avg_price REAL NOT NULL,
    exit_avg_price REAL NOT NULL,
    gross_pnl REAL NOT NULL,
    entry_commission REAL NOT NULL DEFAULT 0,
    exit_commission REAL NOT NULL DEFAULT 0,
    commission_total REAL NOT NULL DEFAULT 0,
    net_pnl REAL NOT NULL,
    entry_fill_count INTEGER NOT NULL DEFAULT 1,
    exit_fill_count INTEGER NOT NULL DEFAULT 1,
    entry_order_count INTEGER NOT NULL DEFAULT 1,
    exit_order_count INTEGER NOT NULL DEFAULT 1,
    opening_fill_id TEXT REFERENCES fills(fill_id) ON DELETE SET NULL,
    closing_fill_id TEXT REFERENCES fills(fill_id) ON DELETE SET NULL,
    opening_order_local_id TEXT REFERENCES orders(order_local_id) ON DELETE SET NULL,
    closing_order_local_id TEXT REFERENCES orders(order_local_id) ON DELETE SET NULL,
    opening_signal_id TEXT REFERENCES signals(signal_id) ON DELETE SET NULL,
    closing_signal_id TEXT REFERENCES signals(signal_id) ON DELETE SET NULL,
    opening_execution_id TEXT REFERENCES executions(execution_id) ON DELETE SET NULL,
    closing_execution_id TEXT REFERENCES executions(execution_id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS round_trip_fills (
    link_id TEXT PRIMARY KEY,
    round_trip_id TEXT NOT NULL REFERENCES round_trips(round_trip_id) ON DELETE CASCADE,
    fill_id TEXT NOT NULL REFERENCES fills(fill_id) ON DELETE CASCADE,
    leg TEXT NOT NULL,
    matched_qty REAL NOT NULL,
    price REAL,
    commission_alloc REAL NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS daily_trade_stats (
    trade_day TEXT NOT NULL,
    broker TEXT NOT NULL,
    symbol TEXT NOT NULL,
    venue TEXT NOT NULL,
    lot_bucket TEXT NOT NULL,
    trades_count INTEGER NOT NULL DEFAULT 0,
    gross_pnl_sum REAL NOT NULL DEFAULT 0,
    commission_sum REAL NOT NULL DEFAULT 0,
    net_pnl_sum REAL NOT NULL DEFAULT 0,
    entry_qty_sum REAL NOT NULL DEFAULT 0,
    PRIMARY KEY (trade_day, broker, symbol, venue, lot_bucket)
);

CREATE TABLE IF NOT EXISTS analytics_counters (
    counter_key TEXT PRIMARY KEY,
    counter_value INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS exchange_income (
    income_id TEXT PRIMARY KEY,
    broker TEXT NOT NULL,
    symbol TEXT NOT NULL,
    income_type TEXT NOT NULL,
    income REAL NOT NULL DEFAULT 0,
    asset TEXT,
    trade_id TEXT,
    order_id TEXT,
    observed_at TEXT,
    raw_json TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS broker_sync_state (
    broker TEXT PRIMARY KEY,
    last_synced_at TEXT NOT NULL,
    cursor TEXT,
    last_imported INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS instrument_meta (
    symbol TEXT PRIMARY KEY,
    shortname TEXT,
    cash_unit TEXT NOT NULL,
    price_step REAL NOT NULL DEFAULT 1.0,
    minstep REAL NOT NULL DEFAULT 1.0,
    facevalue REAL NOT NULL DEFAULT 1.0,
    underlying_currency TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_executions_signal_id ON executions(signal_id);
CREATE INDEX IF NOT EXISTS idx_orders_execution_id ON orders(execution_id);
CREATE INDEX IF NOT EXISTS idx_orders_broker_order_id ON orders(broker_order_id);
CREATE INDEX IF NOT EXISTS idx_fills_execution_id ON fills(execution_id);
CREATE INDEX IF NOT EXISTS idx_fills_order_local_id ON fills(order_local_id);
CREATE INDEX IF NOT EXISTS idx_fills_symbol_time ON fills(broker, symbol, venue, observed_at);
CREATE INDEX IF NOT EXISTS idx_open_lots_lookup ON open_lots(broker, symbol, venue, opened_at);
CREATE INDEX IF NOT EXISTS idx_round_trips_symbol_time ON round_trips(broker, symbol, venue, closed_at);
CREATE INDEX IF NOT EXISTS idx_round_trip_fills_round_trip_id ON round_trip_fills(round_trip_id);
CREATE INDEX IF NOT EXISTS idx_exchange_income_symbol ON exchange_income(broker, symbol, observed_at);
CREATE INDEX IF NOT EXISTS idx_exchange_income_type ON exchange_income(broker, symbol, income_type);
"""


def init_analytics_db() -> Dict[str, Any]:
    path = Path(ANALYTICS_DB_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    with _DB_LOCK:
        with _connect() as conn:
            conn.executescript(SCHEMA_SQL)
            _ensure_counter_keys(conn)
    return {'ok': True, 'path': str(path)}


def _symbol_units(symbol: str) -> Tuple[str, str]:
    raw = str(symbol or '').strip().upper()
    if '-' in raw:
        base, quote = raw.split('-', 1)
        return base or 'ASSET', quote or 'QUOTE'
    if raw.endswith('USDT') and len(raw) > 4:
        return raw[:-4] or 'ASSET', 'USDT'
    return raw or 'ASSET', 'QUOTE'


def _fill_qty_unit(row: Dict[str, Any]) -> str:
    broker = str(row.get('broker') or '').strip().lower()
    venue = str(row.get('venue') or '').strip().lower()
    if broker == 'bingx' and venue == 'swap':
        return 'cts'
    base_unit, _ = _symbol_units(str(row.get('symbol') or ''))
    return base_unit


def _fill_phase_label(phase: str, position_effect: str = '') -> str:
    effect = str(position_effect or '').strip().lower()
    if effect == 'add_long':
        return 'add long'
    if effect == 'add_short':
        return 'add short'
    if effect == 'reduce_long':
        return 'reduce long'
    if effect == 'reduce_short':
        return 'reduce short'
    value = str(phase or '').strip().lower()
    if value == 'target-open':
        return 'open'
    if value.startswith('target-close'):
        return 'close'
    return value or 'primary'


def _fill_sizing_basis(phase: str, request_json: str, position_effect: str = '') -> str:
    phase_label = _fill_phase_label(phase, position_effect)
    request: Dict[str, Any] = {}
    try:
        loaded = json.loads(request_json or '{}')
        if isinstance(loaded, dict):
            request = loaded
    except Exception:
        request = {}
    qty_kind = str(request.get('qtyKind') or '').strip().upper()
    open_qty_kind = str(request.get('openQtyKind') or '').strip().upper()
    if phase_label == 'close':
        return 'close: contracts'
    if phase_label == 'open':
        return f"open: {open_qty_kind or qty_kind or 'unknown'}"
    if open_qty_kind or qty_kind:
        return f"{phase_label}: {open_qty_kind or qty_kind}"
    return phase_label


def _fill_request_unit(phase: str, request_json: str, position_effect: str = '') -> str:
    phase_label = _fill_phase_label(phase, position_effect)
    request: Dict[str, Any] = {}
    try:
        loaded = json.loads(request_json or '{}')
        if isinstance(loaded, dict):
            request = loaded
    except Exception:
        request = {}
    qty_kind = str(request.get('qtyKind') or '').strip().upper()
    open_qty_kind = str(request.get('openQtyKind') or '').strip().upper()
    if phase_label == 'close':
        return 'contracts'
    if phase_label == 'open':
        return (open_qty_kind or qty_kind or 'unknown').lower()
    return (open_qty_kind or qty_kind or 'unknown').lower()


def _fill_request_size(requested_qty_text: str, phase: str, request_json: str, position_effect: str = '') -> str:
    qty_text = str(requested_qty_text or '').strip()
    if not qty_text:
        return ''
    return f"{qty_text} {_fill_request_unit(phase, request_json, position_effect)}"


def _channel_label(row: Dict[str, Any]) -> str:
    signal_id = str(row.get('closing_signal_id') or row.get('opening_signal_id') or '')
    origin = str(row.get('origin') or '').strip().lower()
    if signal_id.startswith('exchange-sync:') or origin == 'exchange_sync':
        return 'manual'
    if origin == 'quick-order':
        return 'quick'
    if origin == 'webhook' or origin == 'tv':
        return 'tv'
    return origin or 'unknown'


def _strategy_label(row: Dict[str, Any]) -> str:
    """Final P&L attribution = traded instrument (broker symbol), not the channel.

    manual / TV / quick on the same contract roll up into one instrument.
    """
    symbol = str(row.get('symbol') or '').strip()
    if symbol:
        return symbol
    ticker = str(row.get('source_ticker') or '').strip()
    if ticker:
        return ticker
    route = str(row.get('route_name') or '').strip()
    if route:
        return route
    return _channel_label(row) or 'unknown'


# BingX perps are quoted in USDT (price*qty already money).
# Alor/Finam MOEX futures P&L from price deltas is in price points;
# cash = points * price_step (RUB per 1 point per 1 contract). Commission is RUB.
BROKER_CASH_UNIT = {
    'bingx': 'USDT',
    'bybit': 'USDT',
    'alor': 'RUB',
    'finam': 'RUB',
    'schwab': 'USD',
}


def _base_code(symbol: str) -> str:
    text = str(symbol or '').strip()
    if '@' in text:
        text = text.split('@', 1)[0]
    return text


def _load_instrument_meta_map(conn: sqlite3.Connection) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    # Compatibility: older DBs may lack minstep.
    cols = {r[1] for r in conn.execute('PRAGMA table_info(instrument_meta)')}
    minstep_col = 'minstep' if 'minstep' in cols else '1.0 AS minstep'
    for row in conn.execute(f'SELECT symbol, shortname, cash_unit, price_step, {minstep_col}, facevalue, underlying_currency FROM instrument_meta'):
        step = float(row['price_step'] or 1.0) or 1.0
        minstep = float(row['minstep'] or 1.0) or 1.0
        # Alor pricestep = RUB per 1 minstep; cash per 1.0 price point = pricestep / minstep.
        cash_step = step / minstep if minstep else step
        out[str(row['symbol'])] = {
            'symbol': str(row['symbol']),
            'shortname': str(row['shortname'] or ''),
            'cash_unit': str(row['cash_unit'] or ''),
            'price_step': cash_step,
            'raw_pricestep': step,
            'minstep': minstep,
            'facevalue': float(row['facevalue'] or 1.0),
            'underlying_currency': str(row['underlying_currency'] or ''),
        }
    return out


def ensure_instrument_meta() -> Dict[str, Any]:
    """Fill price_step (cash per 1 price point) for Alor/Finam futures from Alor securities."""
    init_analytics_db()
    try:
        with _connect() as conn:
            _ensure_counter_keys(conn)
            existing = _load_instrument_meta_map(conn)
            symbols = [str(r['symbol']) for r in conn.execute(
                "SELECT DISTINCT symbol FROM fills WHERE broker IN ('alor','finam') AND symbol IS NOT NULL AND symbol != ''"
            )]
    except Exception:
        return {'ok': False, 'error': 'db_read_failed', 'updated': 0}

    need = []
    for sym in symbols:
        meta = existing.get(sym) or existing.get(_base_code(sym))
        if not meta or float(meta.get('price_step') or 1.0) == 1.0 and meta.get('cash_unit') != 'USDT':
            need.append(sym)
    if not need:
        return {'ok': True, 'updated': 0, 'cached': len(existing)}

    updated = 0
    try:
        from alor_adapter import load_workspace_module
        import asyncio
        import httpx
        from settings import ALOR_CONFIG_PATH
        cfg = json.loads(Path(ALOR_CONFIG_PATH).read_text())
        module = load_workspace_module()
        client = module.AlorClient(
            refresh_token=str(cfg.get('refresh_token') or ''),
            portfolio=str(cfg.get('portfolio') or ''),
            client_id=str(cfg.get('client_id') or ''),
        )

        async def _fetch(sym: str):
            token = await client.get_access_token()
            headers = {'Authorization': f'Bearer {token}'}
            code = sym.split('@', 1)[0]
            async with httpx.AsyncClient(timeout=30) as session:
                for path_sym in (sym, code):
                    url = f'https://api.alor.ru/md/v2/Securities/MOEX/{path_sym}'
                    resp = await session.get(url, headers=headers)
                    if resp.status_code != 200:
                        continue
                    data = resp.json()
                    if isinstance(data, list):
                        data = data[0] if data else {}
                    if isinstance(data, dict) and data:
                        return data
            return {}

        async def _batch():
            out = {}
            for sym in need:
                try:
                    out[sym] = await _fetch(sym)
                except Exception:
                    out[sym] = {}
            return out

        fetched = asyncio.run(_batch())
    except Exception as e:
        return {'ok': False, 'error': str(e), 'updated': 0}

    with _DB_LOCK:
        with _connect() as conn:
            cols = {r[1] for r in conn.execute('PRAGMA table_info(instrument_meta)')}
            if 'minstep' not in cols:
                conn.execute('ALTER TABLE instrument_meta ADD COLUMN minstep REAL NOT NULL DEFAULT 1.0')
            for sym, data in fetched.items():
                if not data:
                    continue
                step = _to_float(data.get('pricestep') or data.get('priceStep') or 1.0) or 1.0
                minstep = _to_float(data.get('minstep') or data.get('minStep') or 1.0) or 1.0
                face = _to_float(data.get('facevalue') or data.get('lotsize') or 1.0) or 1.0
                shortname = str(data.get('shortname') or data.get('shortName') or _base_code(sym))
                und = str(data.get('currency') or '')
                for key in (sym, shortname):
                    if not key:
                        continue
                    conn.execute(
                        '''
                        INSERT INTO instrument_meta(symbol, shortname, cash_unit, price_step, minstep, facevalue, underlying_currency, updated_at)
                        VALUES(?,?,?,?,?,?,?,CURRENT_TIMESTAMP)
                        ON CONFLICT(symbol) DO UPDATE SET
                            shortname=excluded.shortname,
                            cash_unit=excluded.cash_unit,
                            price_step=excluded.price_step,
                            minstep=excluded.minstep,
                            facevalue=excluded.facevalue,
                            underlying_currency=excluded.underlying_currency,
                            updated_at=CURRENT_TIMESTAMP
                        ''',
                        (key, shortname, 'RUB', step, minstep, face, und),
                    )
                updated += 1
    return {'ok': True, 'updated': updated}


def _is_option_symbol(symbol: str) -> bool:
    code = _base_code(symbol)
    # MOEX option shortcodes: strike + C/P/B + month/year, e.g. GD2800BO5, NG2.6BS6, NG3.15BR6C
    if any(token in code for token in ('BO', 'BN', 'BR', 'BS', 'BF', 'BP', 'BU', 'BV', 'BD', 'BQ')):
        return True
    return False


def _instrument_cash_meta(broker: str, symbol: str, meta_map: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    broker = str(broker or '').strip().lower()
    if broker in ('bingx', 'bybit', 'schwab'):
        return {
            'cash_unit': BROKER_CASH_UNIT.get(broker, 'USDT'),
            'price_step': 1.0,
            'points_unit': 'price',
        }
    meta = meta_map.get(symbol) or meta_map.get(_base_code(symbol)) or {}
    # price_step in map is already cash per 1.0 quote point (loader divides pricestep/minstep).
    step = float(meta.get('price_step') or 1.0) or 1.0
    unit = str(meta.get('cash_unit') or 'RUB')
    if not meta:
        return {'cash_unit': 'POINTS', 'price_step': 1.0, 'points_unit': 'pts'}
    # Option premium multiplies differently; keep points unless calibrated.
    if _is_option_symbol(symbol) and not meta.get('calibrated'):
        return {'cash_unit': 'POINTS', 'price_step': 1.0, 'points_unit': 'opt_pts'}
    return {'cash_unit': unit, 'price_step': step, 'points_unit': 'pts'}


def calibrate_price_step_from_trades() -> Dict[str, Any]:
    """cash_step = Alor volume / (price * qty) — ground truth from trade notional."""
    init_analytics_db()
    updated = 0
    try:
        with _DB_LOCK:
            with _connect() as conn:
                cols = {r[1] for r in conn.execute('PRAGMA table_info(instrument_meta)')}
                if 'calibrated' not in cols:
                    conn.execute('ALTER TABLE instrument_meta ADD COLUMN calibrated INTEGER NOT NULL DEFAULT 0')
                rows = conn.execute(
                    '''
                    SELECT symbol, price, qty, raw_json FROM fills
                    WHERE broker='alor' AND price > 0 AND qty > 0
                    '''
                ).fetchall()
                ratios: Dict[str, List[float]] = {}
                for row in rows:
                    try:
                        raw = json.loads(row['raw_json'] or '{}')
                        vol = float(raw.get('volume') or 0)
                        pq = float(row['price']) * float(row['qty'])
                        if vol > 0 and pq > 0:
                            ratios.setdefault(str(row['symbol']), []).append(vol / pq)
                    except Exception:
                        continue
                for symbol, vals in ratios.items():
                    if not vals:
                        continue
                    cash_step = sum(vals) / len(vals)
                    if cash_step <= 0:
                        continue
                    shortname = ''
                    for key in (symbol, _base_code(symbol)):
                        existing = conn.execute('SELECT shortname FROM instrument_meta WHERE symbol=?', (key,)).fetchone()
                        if existing and existing['shortname']:
                            shortname = str(existing['shortname'])
                            break
                    for key in {symbol, _base_code(symbol), shortname}:
                        if not key:
                            continue
                        conn.execute(
                            '''
                            INSERT INTO instrument_meta(symbol, shortname, cash_unit, price_step, minstep, facevalue, underlying_currency, updated_at, calibrated)
                            VALUES(?,?,?,?,1.0,1.0,?,CURRENT_TIMESTAMP,1)
                            ON CONFLICT(symbol) DO UPDATE SET
                                cash_unit=excluded.cash_unit,
                                price_step=excluded.price_step,
                                minstep=1.0,
                                calibrated=1,
                                updated_at=CURRENT_TIMESTAMP
                            ''',
                            (key, shortname or _base_code(symbol), 'RUB', cash_step, ''),
                        )
                    updated += 1
    except Exception as e:
        return {'ok': False, 'error': str(e), 'updated': 0}
    return {'ok': True, 'updated': updated}


def _cash_from_points(points_pnl: float, price_step: float) -> float:
    return float(points_pnl or 0.0) * float(price_step or 1.0)


def _metrics_block(rows: List[Dict[str, Any]], cash_unit: str = '') -> Dict[str, Any]:
    trades = len(rows)
    unit = cash_unit or 'MIXED'
    if not trades:
        return {
            'cashUnit': unit,
            'trades': 0, 'wins': 0, 'losses': 0, 'breakeven': 0,
            'winRate': 0.0, 'profitFactor': 0.0,
            'netCash': 0.0, 'grossCash': 0.0, 'commissionCash': 0.0,
            'netPoints': 0.0, 'grossPoints': 0.0,
            'avgWinCash': 0.0, 'avgLossCash': 0.0, 'avgTradeCash': 0.0,
            'expectancyCash': 0.0, 'bestTradeCash': 0.0, 'worstTradeCash': 0.0,
            'avgHoldSec': 0.0, 'totalQty': 0.0,
        }
    # Prefer cash fields; fall back to converting points.
    def _cash(r: Dict[str, Any], kind: str) -> float:
        if r.get(f'{kind}_cash') is not None:
            return float(r.get(f'{kind}_cash') or 0.0)
        step = float(r.get('price_step') or 1.0) or 1.0
        return _cash_from_points(float(r.get(f'{kind}_pnl') or 0.0) if kind != 'commission' else float(r.get('commission_total') or 0.0), step)

    net_cash = [float(r.get('net_cash') if r.get('net_cash') is not None else _cash(r, 'net')) for r in rows]
    wins_list = [p for p in net_cash if p > 0]
    losses_list = [p for p in net_cash if p < 0]
    flat = [p for p in net_cash if p == 0]
    wins = len(wins_list)
    losses = len(losses_list)
    net = sum(net_cash)
    gross = sum(float(r.get('gross_cash') if r.get('gross_cash') is not None else _cash(r, 'gross')) for r in rows)
    commission = sum(float(r.get('commission_cash') if r.get('commission_cash') is not None else _cash(r, 'commission')) for r in rows)
    # Points are always the raw price-delta P&L (gross), never mixed with cash.
    gross_points = sum(float(r.get('gross_pnl') or 0.0) for r in rows)
    net_points = gross_points
    gross_profit = sum(wins_list)
    gross_loss = abs(sum(losses_list))
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (999.0 if gross_profit > 0 else 0.0)
    holds = [float(r.get('holding_time_sec') or 0.0) for r in rows if r.get('holding_time_sec') is not None]
    qty = sum(abs(float(r.get('exit_qty') or r.get('entry_qty') or 0.0)) for r in rows)
    return {
        'cashUnit': unit,
        'trades': trades,
        'wins': wins,
        'losses': losses,
        'breakeven': len(flat),
        'winRate': round(100.0 * wins / trades, 2) if trades else 0.0,
        'profitFactor': round(profit_factor, 3) if profit_factor < 999 else 999.0,
        'netCash': round(net, 4),
        'grossCash': round(gross, 4),
        'commissionCash': round(commission, 4),
        'netPoints': round(net_points, 4),
        'grossPoints': round(gross_points, 4),
        'avgWinCash': round(gross_profit / wins, 4) if wins else 0.0,
        'avgLossCash': round(-gross_loss / losses, 4) if losses else 0.0,
        'avgTradeCash': round(net / trades, 4) if trades else 0.0,
        'expectancyCash': round(net / trades, 4) if trades else 0.0,
        'bestTradeCash': round(max(net_cash), 4) if net_cash else 0.0,
        'worstTradeCash': round(min(net_cash), 4) if net_cash else 0.0,
        'avgHoldSec': round(sum(holds) / len(holds), 1) if holds else 0.0,
        'totalQty': round(qty, 4),
    }


def _load_round_trips_joined(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    return [dict(row) for row in conn.execute(
        '''
        SELECT
            rt.*,
            COALESCE(sc.origin, so.origin) AS origin,
            COALESCE(sc.source_ticker, so.source_ticker) AS source_ticker,
            COALESCE(sc.route_name, so.route_name) AS route_name,
            COALESCE(sc.route_id, so.route_id) AS route_id
        FROM round_trips rt
        LEFT JOIN signals sc ON sc.signal_id = rt.closing_signal_id
        LEFT JOIN signals so ON so.signal_id = rt.opening_signal_id
        '''
    ).fetchall()]


def _group_metrics(rows: List[Dict[str, Any]], key_fn) -> List[Dict[str, Any]]:
    buckets: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        key = key_fn(row)
        buckets.setdefault(key, []).append(row)
    out = []
    for key, bucket in buckets.items():
        # Never mix cash units inside one block.
        units = {str(r.get('cash_unit') or 'MIXED') for r in bucket}
        unit = next(iter(units)) if len(units) == 1 else 'MIXED'
        block = _metrics_block(bucket, cash_unit=unit)
        block['name'] = key
        out.append(block)
    out.sort(key=lambda r: r.get('netCash') or 0.0, reverse=True)
    return out


def performance_stats(brokers: List[str] | None = None, years: List[str] | None = None) -> Dict[str, Any]:
    """Full closed-trade stats in cash units (RUB / USDT / USD), split by unit.

    Alor/Finam futures: price-delta P&L is in points → cash = points × price_step (RUB).
    BingX perps: already USDT. Units are never summed together.
    brokers: optional allowlist (default = all).
    years: optional year allowlist like ['2025','2026'] (default = all).
    """
    allow = None
    if brokers is not None:
        allow = {str(b).strip().lower() for b in brokers if str(b or '').strip()}
    year_allow = None
    if years is not None:
        year_allow = {str(y).strip()[:4] for y in years if str(y or '').strip()}
    meta_note = ensure_instrument_meta()
    calib_note = calibrate_price_step_from_trades()
    with _DB_LOCK:
        with _connect() as conn:
            _ensure_counter_keys(conn)
            all_rows = _load_round_trips_joined(conn)
            meta_map = _load_instrument_meta_map(conn)
            available_years = sorted({
                str(r.get('closed_at') or r.get('opened_at') or '')[:4]
                for r in all_rows
                if str(r.get('closed_at') or r.get('opened_at') or '')[:4]
            })
            income_by_asset: Dict[str, float] = {}
            income_where = ' WHERE 1=1'
            income_params: List[Any] = []
            if allow:
                marks = ','.join('?' for _ in allow)
                income_where += f' AND broker IN ({marks})'
                income_params.extend(sorted(allow))
            if year_allow:
                ymarks = ','.join('?' for _ in year_allow)
                income_where += f" AND substr(observed_at,1,4) IN ({ymarks})"
                income_params.extend(sorted(year_allow))
            for row in conn.execute(f'SELECT asset, SUM(income) AS s FROM exchange_income{income_where} GROUP BY asset', income_params):
                income_by_asset[str(row['asset'] or '')] = float(row['s'] or 0.0)
            finam_vm = 0.0
            finam_fee = 0.0
            finam_account_by_year: Dict[str, Dict[str, float]] = {}
            finam_allowed = allow is None or 'finam' in allow
            if finam_allowed:
                finam_where = " WHERE broker='finam'"
                finam_params: List[Any] = []
                if year_allow:
                    ymarks = ','.join('?' for _ in year_allow)
                    finam_where += f" AND substr(observed_at,1,4) IN ({ymarks})"
                    finam_params.extend(sorted(year_allow))
                for row in conn.execute(f"SELECT income_type, SUM(income) AS s FROM exchange_income{finam_where} GROUP BY income_type", finam_params):
                    t = str(row['income_type'] or '')
                    if t in ('INCOME', 'OUTCOMES'):
                        finam_vm += float(row['s'] or 0.0)
                    elif t == 'COMMISSION':
                        finam_fee += float(row['s'] or 0.0)
                for row in conn.execute(
                    f"""
                    SELECT substr(observed_at,1,4) AS y, income_type, SUM(income) AS s
                    FROM exchange_income{finam_where} AND observed_at IS NOT NULL AND observed_at != ''
                    GROUP BY y, income_type
                    """,
                    finam_params,
                ):
                    y = str(row['y'] or '')
                    if not y:
                        continue
                    if year_allow and y not in year_allow:
                        continue
                    bucket = finam_account_by_year.setdefault(y, {'variationMarginRUB': 0.0, 'commissionRUB': 0.0, 'netRUB': 0.0})
                    t = str(row['income_type'] or '')
                    val = float(row['s'] or 0.0)
                    if t in ('INCOME', 'OUTCOMES'):
                        bucket['variationMarginRUB'] += val
                    elif t == 'COMMISSION':
                        bucket['commissionRUB'] += val
                for bucket in finam_account_by_year.values():
                    bucket['netRUB'] = bucket['variationMarginRUB'] + bucket['commissionRUB']
                    for k in bucket:
                        bucket[k] = round(bucket[k], 2)

    rows = all_rows
    if allow is not None:
        rows = [r for r in rows if str(r.get('broker') or '').strip().lower() in allow]

    for row in rows:
        row['strategy'] = _strategy_label(row)
        row['channel'] = _channel_label(row)
        row['instrument'] = str(row.get('symbol') or '')
        row['broker_name'] = str(row.get('broker') or '')
        day = str(row.get('closed_at') or row.get('opened_at') or '')[:10]
        row['trade_day'] = day or 'unknown'

    if year_allow is not None:
        rows = [r for r in rows if str(r.get('trade_day') or '')[:4] in year_allow]

    for row in rows:
        cash_meta = _instrument_cash_meta(row['broker_name'], row['instrument'], meta_map)
        step = float(cash_meta.get('price_step') or 1.0) or 1.0
        row['price_step'] = step
        row['cash_unit'] = cash_meta['cash_unit']
        gross_points = float(row.get('gross_pnl') or 0.0)
        commission_points = float(row.get('commission_total') or 0.0)
        # Alor/Finam: commission is already in RUB cash; do not treat it as points.
        if row['cash_unit'] == 'USDT':
            row['gross_cash'] = gross_points
            row['commission_cash'] = commission_points
        else:
            row['gross_cash'] = _cash_from_points(gross_points, step)
            row['commission_cash'] = commission_points if commission_points else 0.0
        row['net_cash'] = float(row['gross_cash']) - float(row['commission_cash'])
        row['net_pnl'] = float(row.get('net_pnl') or 0.0)
        row['gross_pnl'] = gross_points

    by_unit: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        by_unit.setdefault(row['cash_unit'], []).append(row)

    overall_by_unit = {
        unit: _metrics_block(bucket, cash_unit=unit)
        for unit, bucket in sorted(by_unit.items(), key=lambda kv: -sum(float(r.get('net_cash') or 0.0) for r in kv[1]))
    }

    current_year = datetime.now(LOCAL_TZ).year
    current_year_s = str(current_year)

    def _period_slice(pred) -> Dict[str, Any]:
        subset = [r for r in rows if pred(r)]
        by_u: Dict[str, List[Dict[str, Any]]] = {}
        for r in subset:
            by_u.setdefault(r['cash_unit'], []).append(r)
        return {
            unit: _metrics_block(bucket, cash_unit=unit)
            for unit, bucket in sorted(by_u.items(), key=lambda kv: -sum(float(r.get('net_cash') or 0.0) for r in kv[1]))
        }

    by_year: Dict[str, Dict[str, Any]] = {}
    years = sorted({str(r.get('trade_day') or '')[:4] for r in rows if str(r.get('trade_day') or '')[:4]})
    for y in years:
        by_year[y] = _period_slice(lambda r, y=y: str(r.get('trade_day') or '').startswith(y))

    ytd = _period_slice(lambda r: str(r.get('trade_day') or '').startswith(current_year_s))
    trailing_365 = _period_slice(
        lambda r: str(r.get('trade_day') or '') >= (datetime.now(LOCAL_TZ) - timedelta(days=365)).date().isoformat()
    )

    def _unit_aware(rows_in: List[Dict[str, Any]], key_fn) -> List[Dict[str, Any]]:
        return _group_metrics(rows_in, lambda r: f"{key_fn(r)}|{r.get('cash_unit')}")

    def _split_name(block: Dict[str, Any]) -> Dict[str, Any]:
        name = str(block.get('name') or '')
        if '|' in name:
            base, unit = name.rsplit('|', 1)
            block['name'] = base
            block['cashUnit'] = unit
        return block

    by_strategy = [_split_name(b) for b in _unit_aware(rows, lambda r: r['strategy'])]
    by_instrument = [_split_name(b) for b in _unit_aware(rows, lambda r: r['instrument'])]
    by_channel = [_split_name(b) for b in _unit_aware(rows, lambda r: r['channel'])]
    by_broker = [_split_name(b) for b in _unit_aware(rows, lambda r: r['broker_name'])]
    by_direction = [_split_name(b) for b in _unit_aware(rows, lambda r: str(r.get('direction') or ''))]
    by_day = [_split_name(b) for b in _unit_aware(rows, lambda r: r['trade_day'])]
    by_day.sort(key=lambda r: r['name'])

    open_lots_count = 0
    open_qty = 0.0
    try:
        with _connect() as conn:
            row = conn.execute('SELECT COUNT(*) AS c, COALESCE(SUM(remaining_qty),0) AS q FROM open_lots').fetchone()
            open_lots_count = int(row['c'] or 0)
            open_qty = float(row['q'] or 0.0)
    except Exception:
        pass

    return {
        'generatedAt': datetime.now(LOCAL_TZ).isoformat(),
        'brokerFilter': sorted(allow) if allow is not None else 'all',
        'yearFilter': sorted(year_allow) if year_allow is not None else 'all',
        'availableYears': available_years,
        'period': {
            'from': min((r.get('opened_at') or '') for r in rows) if rows else '',
            'to': max((r.get('closed_at') or '') for r in rows) if rows else '',
        },
        'cashUnits': {
            'bingx': 'USDT — уже деньги (price×qty)',
            'alor': 'RUB — пункты × cash_step; cash_step калиброван по volume/(price×qty)',
            'finam': 'RUB — пункты × cash_step для фьючерсов; опционы в POINTS (без надёжного множителя)',
            'finamVmCash': f'VM income FINAM = {finam_vm:.2f} RUB (фактический cash P&L по счёту)',
            'finamFeeCash': f'Комиссии FINAM income = {finam_fee:.2f} RUB',
            'note': 'Суммы в разных валютах НЕ складываются. POINTS — сырой P&L в пунктах/премии.',
        },
        'incomeByAsset': {k: round(v, 4) for k, v in income_by_asset.items()},
        'finamAccountCash': {'variationMarginRUB': round(finam_vm, 2), 'commissionRUB': round(finam_fee, 2), 'netRUB': round(finam_vm + finam_fee, 2)},
        'finamAccountCashByYear': finam_account_by_year,
        'overallByUnit': overall_by_unit,
        'byYear': by_year,
        'ytd': {'year': current_year_s, 'byUnit': ytd},
        'trailing365d': trailing_365,
        'openLots': {'count': open_lots_count, 'qty': round(open_qty, 4)},
        'byStrategy': by_strategy,
        'byInstrument': by_instrument,
        'byChannel': by_channel,
        'byBroker': by_broker,
        'byDirection': by_direction,
        'byDay': by_day,
        'instrumentMeta': {'fetch': meta_note, 'calibrate': calib_note},
        'definitions': {
            'netCash': 'денежный P&L. BingX=USDT. Alor/Finam futures: пункты×cash_step − комиссия (RUB).',
            'netPoints': 'сырой P&L в пунктах цены / пунктах премии — НЕ деньги',
            'cash_step': 'RUB за 1.0 пункта: Alor volume/(price×qty); для фьючерсов также pricestep/minstep',
            'POINTS': 'инструмент без надёжного множителя (часто опционы) — считаем только в пунктах',
            'winRate': 'доля закрытых сделок с netCash (или netPoints для POINTS) > 0, %',
            'profitFactor': 'gross profit / |gross loss| (999 = без убытков)',
            'finamAccountCash': 'VM (INCOME+OUTCOMES) и комиссии из Finam transactions — фактические RUB по счёту',
            'strategy': 'итоговая атрибуция = биржевой инструмент (symbol); TV/quick/manual по одному контракту сгруппированы вместе',
            'channel': 'manual = терминал брокера; tv = сигнал TradingView; quick = быстрый ордер WHR',
        },
    }


def _filter_sql(alias: str, brokers: List[str] | None, years: List[str] | None, date_col: str) -> Tuple[str, List[Any]]:
    """Build WHERE fragment for broker/year allowlists on a table alias."""
    where = ''
    params: List[Any] = []
    if brokers is not None:
        allow = [str(b).strip().lower() for b in brokers if str(b or '').strip()]
        if not allow:
            return ' AND 1=0', params
        marks = ','.join('?' for _ in allow)
        where += f" AND {alias}.broker IN ({marks})"
        params.extend(allow)
    if years is not None:
        allow_y = [str(y).strip()[:4] for y in years if str(y or '').strip()]
        if not allow_y:
            return ' AND 1=0', params
        marks = ','.join('?' for _ in allow_y)
        where += f" AND substr({date_col},1,4) IN ({marks})"
        params.extend(allow_y)
    return where, params


def analytics_overview(limit: int = 20, brokers: List[str] | None = None, years: List[str] | None = None) -> Dict[str, Any]:
    path = Path(ANALYTICS_DB_PATH)
    lim = max(1, min(int(limit), 100))
    f_where, f_params = _filter_sql('f', brokers, years, 'f.observed_at')
    rt_where, rt_params = _filter_sql('rt', brokers, years, 'rt.closed_at')
    d_where, d_params = _filter_sql('d', brokers, years, 'd.trade_day')
    inc_where, inc_params = _filter_sql('i', brokers, years, 'i.observed_at')
    with _DB_LOCK:
        with _connect() as conn:
            _ensure_counter_keys(conn)
            counters = {
                str(row['counter_key']): int(row['counter_value'] or 0)
                for row in conn.execute('SELECT counter_key, counter_value FROM analytics_counters').fetchall()
            }
            latest_fills = [dict(row) for row in conn.execute(
                f'''
                SELECT
                    f.fill_id, f.observed_at, f.broker, f.symbol, f.venue, f.side, f.qty, f.price, f.notional,
                    f.commission, f.commission_currency, f.order_local_id, f.execution_id, f.signal_id,
                    f.phase, f.position_effect, f.source_type, e.signal_mode, e.requested_qty_text, e.request_json
                FROM fills f
                LEFT JOIN executions e ON e.execution_id = f.execution_id
                WHERE 1=1{f_where}
                ORDER BY f.observed_at DESC, f.fill_id DESC
                LIMIT ?
                ''',
                (*f_params, lim),
            ).fetchall()]
            for row in latest_fills:
                phase = str(row.get('phase') or '')
                request_json = str(row.get('request_json') or '')
                base_unit, quote_unit = _symbol_units(str(row.get('symbol') or ''))
                position_effect = str(row.get('position_effect') or '')
                row['phaseLabel'] = _fill_phase_label(phase, position_effect)
                row['sizingBasis'] = _fill_sizing_basis(phase, request_json, position_effect)
                row['requestSize'] = _fill_request_size(str(row.get('requested_qty_text') or ''), phase, request_json, position_effect)
                row['fillQtyUnit'] = _fill_qty_unit(row)
                row['priceUnit'] = quote_unit
                row['notionalUnit'] = quote_unit
                row['feeUnit'] = str(row.get('commission_currency') or quote_unit)
                row['feeBasis'] = f"qty ({row['fillQtyUnit']}) × price = notional in {quote_unit}"
            latest_round_trips = [dict(row) for row in conn.execute(
                f'''
                SELECT rt.round_trip_id, rt.closed_at, rt.broker, rt.symbol, rt.venue, rt.direction, rt.entry_qty, rt.entry_avg_price, rt.exit_avg_price, rt.gross_pnl, rt.commission_total, rt.net_pnl, rt.opening_signal_id, rt.closing_signal_id
                FROM round_trips rt
                WHERE 1=1{rt_where}
                ORDER BY rt.closed_at DESC, rt.round_trip_id DESC
                LIMIT ?
                ''',
                (*rt_params, lim),
            ).fetchall()]
            for row in latest_round_trips:
                base_unit, quote_unit = _symbol_units(str(row.get('symbol') or ''))
                row['qtyUnit'] = base_unit
                row['priceUnit'] = quote_unit
                row['pnlUnit'] = quote_unit
            latest_close_events = [dict(row) for row in conn.execute(
                f'''
                SELECT
                    rt.closing_signal_id,
                    rt.closed_at,
                    rt.broker,
                    rt.symbol,
                    rt.venue,
                    rt.direction,
                    COUNT(*) AS lots_closed,
                    COUNT(DISTINCT rt.opening_signal_id) AS opening_signals,
                    SUM(rt.entry_qty) AS closed_qty_sum,
                    SUM(rt.gross_pnl) AS gross_pnl_sum,
                    SUM(rt.commission_total) AS commission_sum,
                    SUM(rt.net_pnl) AS net_pnl_sum
                FROM round_trips rt
                WHERE rt.closing_signal_id IS NOT NULL AND rt.closing_signal_id != ''{rt_where}
                GROUP BY rt.closing_signal_id, rt.closed_at, rt.broker, rt.symbol, rt.venue, rt.direction
                ORDER BY rt.closed_at DESC, rt.closing_signal_id DESC
                LIMIT ?
                ''',
                (*rt_params, lim),
            ).fetchall()]
            for row in latest_close_events:
                base_unit, quote_unit = _symbol_units(str(row.get('symbol') or ''))
                row['qtyUnit'] = base_unit
                row['pnlUnit'] = quote_unit
            latest_daily_stats = [dict(row) for row in conn.execute(
                f'''
                SELECT d.trade_day, d.broker, d.symbol, d.venue, d.lot_bucket, d.trades_count, d.gross_pnl_sum, d.commission_sum, d.net_pnl_sum, d.entry_qty_sum
                FROM daily_trade_stats d
                WHERE 1=1{d_where}
                ORDER BY d.trade_day DESC, d.broker, d.symbol, d.venue, d.lot_bucket
                LIMIT ?
                ''',
                (*d_params, lim),
            ).fetchall()]
            latest_signal = conn.execute('SELECT signal_id, received_at, origin, source_ticker, side, qty_text FROM signals ORDER BY received_at DESC, signal_id DESC LIMIT 1').fetchone()
            latest_execution = conn.execute('SELECT execution_id, received_at, broker, symbol, venue, status, error_text FROM executions ORDER BY received_at DESC, execution_id DESC LIMIT 1').fetchone()

            income_summary = conn.execute(f'''
                SELECT i.symbol,
                       SUM(CASE WHEN i.income_type='REALIZED_PNL' THEN i.income ELSE 0 END) AS realized_pnl,
                       SUM(CASE WHEN i.income_type='TRADING_FEE' THEN i.income ELSE 0 END) AS total_fees,
                       SUM(i.income) AS net_pnl,
                       COUNT(CASE WHEN i.income_type='REALIZED_PNL' AND i.income > 0 THEN 1 END) AS wins,
                       COUNT(CASE WHEN i.income_type='REALIZED_PNL' AND i.income < 0 THEN 1 END) AS losses
                FROM exchange_income i
                WHERE 1=1{inc_where}
                GROUP BY i.symbol
                ORDER BY net_pnl DESC
            ''', inc_params).fetchall()
            income_rows = [dict(r) for r in income_summary]

            income_totals = conn.execute(f'''
                SELECT SUM(CASE WHEN i.income_type='REALIZED_PNL' THEN i.income ELSE 0 END) AS total_realized,
                       SUM(CASE WHEN i.income_type='TRADING_FEE' THEN i.income ELSE 0 END) AS total_fees,
                       SUM(i.income) AS total_net,
                       COUNT(DISTINCT CASE WHEN i.income_type='REALIZED_PNL' THEN i.symbol END) AS symbols_traded
                FROM exchange_income i
                WHERE 1=1{inc_where}
            ''', inc_params).fetchone()

            recent_income = [dict(r) for r in conn.execute(f'''
                SELECT i.income_id, i.broker, i.symbol, i.income_type, i.income, i.asset, i.observed_at
                FROM exchange_income i
                WHERE 1=1{inc_where}
                ORDER BY i.observed_at DESC
                LIMIT ?
            ''', (*inc_params, max(1, min(int(limit), 50)))).fetchall()]
    db_size_bytes = path.stat().st_size if path.exists() else 0
    return {
        'dbPath': str(path),
        'dbExists': path.exists(),
        'dbSizeBytes': db_size_bytes,
        'brokerFilter': sorted(brokers) if brokers is not None else 'all',
        'yearFilter': sorted(years) if years is not None else 'all',
        'counters': counters,
        'incomeBySymbol': income_rows,
        'incomeTotals': dict(income_totals) if income_totals else {},
        'recentIncome': recent_income,
        'latestSignal': dict(latest_signal) if latest_signal else {},
        'latestExecution': dict(latest_execution) if latest_execution else {},
        'latestFills': latest_fills,
        'latestRoundTrips': latest_round_trips,
        'latestCloseEvents': latest_close_events,
        'latestDailyStats': latest_daily_stats,
    }


def record_execution_analytics(decision: Dict[str, Any]) -> None:
    execution_result = (decision or {}).get('executionResult') or {}
    destinations = execution_result.get('destinations') or []
    if not destinations:
        return

    with _DB_LOCK:
        with _connect() as conn:
            _ensure_counter_keys(conn)
            signal_id = _signal_id(decision)
            _upsert_signal(conn, signal_id, decision)
            for idx, destination in enumerate(destinations):
                execution_id = _execution_id(signal_id, idx)
                _upsert_execution(conn, signal_id, execution_id, idx, decision, destination)
                orders, fills = _extract_orders_and_fills(signal_id, execution_id, destination)
                for order in orders:
                    _upsert_order(conn, order)
                for fill in fills:
                    if _insert_fill(conn, fill):
                        _apply_fill_to_positions(conn, fill)


def _nested_value(value: Any) -> Any:
    if isinstance(value, dict):
        for key in ('value', 'units', 'qty', 'quantity', 'amount'):
            if key in value:
                return value[key]
        return ''
    return value


def _normalize_side(raw: Any) -> str:
    text = str(raw or '').strip().lower()
    if text in ('buy', 'side_buy', 'b', 'покупка'):
        return 'buy'
    if text in ('sell', 'side_sell', 's', 'продажа'):
        return 'sell'
    return text


def _observed_at_from_row(row: Dict[str, Any]) -> str:
    ts_val = (
        row.get('filledTime') or row.get('filledTm') or row.get('timestamp')
        or row.get('time') or row.get('updateTime') or row.get('date')
    )
    if not ts_val:
        return ''
    text = str(ts_val)
    try:
        ts_int = int(float(text))
        if ts_int > 10_000_000_000:
            return datetime.fromtimestamp(ts_int / 1000, tz=LOCAL_TZ).isoformat()
        return datetime.fromtimestamp(ts_int, tz=LOCAL_TZ).isoformat()
    except Exception:
        pass
    iso_text = text.replace('Z', '+00:00')
    # Alor uses 7-digit fractional seconds; fromisoformat wants 3 or 6.
    if '.' in iso_text and '+' in iso_text:
        head, tail = iso_text.split('.', 1)
        frac = ''
        rest = tail
        for i, ch in enumerate(tail):
            if ch.isdigit():
                frac += ch
            else:
                rest = tail[i:]
                break
        if len(frac) > 6:
            frac = frac[:6]
        elif 0 < len(frac) < 6:
            frac = frac.ljust(6, '0')
        iso_text = f'{head}.{frac}{rest}' if frac else f'{head}{rest}'
    try:
        dt = datetime.fromisoformat(iso_text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=LOCAL_TZ)
        return dt.astimezone(LOCAL_TZ).isoformat()
    except Exception:
        pass
    time_text = str(row.get('time') or '').strip()
    date_text = str(row.get('date') or '').strip()
    if date_text and time_text:
        for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%d.%m.%Y %H:%M:%S', '%d.%m.%Y %H:%M'):
            try:
                dt = datetime.strptime(f'{date_text} {time_text}', fmt).replace(tzinfo=LOCAL_TZ)
                return dt.isoformat()
            except Exception:
                continue
    return text


def _finam_symbol_venue(symbol: str) -> str:
    text = str(symbol or '')
    if '@' in text:
        return text.split('@', 1)[1].strip().upper() or 'MOEX'
    return 'MOEX'


def _normalize_broker_fill_row(broker: str, row: Dict[str, Any], default_symbol: str = '', default_venue: str = '') -> Dict[str, Any] | None:
    broker = str(broker or '').strip().lower()
    price_raw = row.get('price')
    if price_raw is None:
        price_raw = row.get('avgPrice')
    # Field priority is broker-specific: BingX volume=qty, Alor volume=notional money.
    if broker == 'alor':
        qty_keys = ('qty', 'qtyUnits', 'qtyBatch', 'quantity', 'executedQty')
    elif broker == 'finam':
        qty_keys = ('size', 'qty', 'volume', 'quantity', 'executedQty')
    else:
        qty_keys = ('volume', 'qty', 'size', 'quantity', 'executedQty')
    qty_raw = None
    for key in qty_keys:
        if row.get(key) is not None:
            qty_raw = row.get(key)
            break
    price_raw = _nested_value(price_raw)
    qty_raw = _nested_value(qty_raw)

    qty = abs(_to_float(qty_raw))
    if qty <= 0:
        return None
    price = abs(_to_float(price_raw))

    symbol = str(row.get('symbol') or row.get('security') or row.get('ticker') or default_symbol or '').strip()
    if not symbol:
        return None

    side = _normalize_side(row.get('side') or row.get('orderSide') or row.get('buysell'))
    broker_order_id = str(
        row.get('orderId') or row.get('orderID') or row.get('order_id')
        or row.get('orderNo') or row.get('orderno') or row.get('ordno') or ''
    )
    trade_id = str(row.get('tradeId') or row.get('tradeID') or row.get('trade_id') or row.get('tradeNo') or row.get('id') or '')

    commission_raw = row.get('commission') or row.get('fee') or row.get('brokerCommission')
    if isinstance(commission_raw, dict):
        commission_raw = _nested_value(commission_raw)
    commission = _to_float(commission_raw)
    commission_currency = str(
        row.get('currency') or row.get('commissionCurrency') or row.get('feeAsset')
        or row.get('currency_code') or ''
    )

    if broker == 'finam':
        venue = _finam_symbol_venue(symbol)
    elif broker == 'alor':
        venue = str(row.get('exchange') or row.get('_alorExchange') or default_venue or 'MOEX').strip().upper()
    else:
        venue = str(row.get('category') or default_venue or 'swap').strip().lower()

    fill_id = f'exchange:{broker}:{symbol}:{broker_order_id or trade_id}:{trade_id or "0"}'
    return {
        'fill_id': fill_id,
        'execution_id': f'exchange:{broker}:{symbol}',
        'signal_id': f'exchange-sync:{broker}:{symbol}',
        'order_local_id': None,
        'broker_order_id': broker_order_id,
        'fill_seq': _to_int(trade_id) or 0,
        'phase': 'exchange_sync',
        'observed_at': _observed_at_from_row(row),
        'broker': broker,
        'symbol': symbol,
        'venue': venue,
        'side': side,
        'qty': qty,
        'price': price,
        'notional': qty * price if price else 0.0,
        'commission': commission,
        'commission_currency': commission_currency,
        'liquidity_flag': str(row.get('liquidityFlag') or ''),
        'position_effect': '',
        'source_type': 'exchange_sync',
        'raw_json': _json_text(row),
    }


def _import_normalized_fills(conn: sqlite3.Connection, broker: str, fills: List[Dict[str, Any]]) -> Tuple[int, int]:
    imported = 0
    skipped = 0
    by_symbol: Dict[str, Dict[str, Any]] = {}
    for fill in fills:
        symbol = str(fill.get('symbol') or '')
        if not symbol:
            continue
        bucket = by_symbol.setdefault(symbol, {
            'venue': str(fill.get('venue') or ''),
            'items': [],
        })
        if not bucket['venue']:
            bucket['venue'] = str(fill.get('venue') or '')
        bucket['items'].append(fill)

    now_iso = datetime.now(LOCAL_TZ).isoformat()
    for symbol, bucket in by_symbol.items():
        venue = bucket['venue'] or ('swap' if broker == 'bingx' else '')
        signal_id = f'exchange-sync:{broker}:{symbol}'
        execution_id = f'exchange:{broker}:{symbol}'
        conn.execute(
            'INSERT OR IGNORE INTO signals(signal_id, received_at, origin, source_ticker) VALUES(?,?,?,?)',
            (signal_id, now_iso, 'exchange_sync', symbol),
        )
        conn.execute(
            '''INSERT OR IGNORE INTO executions(execution_id, signal_id, destination_index, received_at, broker, symbol, venue, side, status, dry_run)
            VALUES(?,?,?,?,?,?,?,?,?,?)''',
            (execution_id, signal_id, 0, now_iso, broker, symbol, venue, '', 'exchange_sync', 0),
        )
        for fill in bucket['items']:
            exists = conn.execute('SELECT 1 FROM fills WHERE fill_id = ?', (fill['fill_id'],)).fetchone()
            if exists:
                skipped += 1
                continue
            # Same economic fill from another source (order path vs exchange sync).
            if _logical_fill_exists(conn, fill):
                skipped += 1
                continue
            if _insert_fill(conn, fill):
                _recompute_fill_effect(conn, fill)
                _apply_fill_to_positions(conn, fill)
                imported += 1
            else:
                skipped += 1
    return imported, skipped


def _fill_epoch(observed_at: Any) -> int | None:
    if not observed_at:
        return None
    try:
        d = datetime.fromisoformat(str(observed_at).replace('Z', '+00:00'))
        if d.tzinfo is None:
            d = d.replace(tzinfo=LOCAL_TZ)
        return int(d.timestamp())
    except Exception:
        return None


def _logical_fill_exists(conn: sqlite3.Connection, fill: Dict[str, Any]) -> bool:
    """True if a twin fill already exists (same broker/symbol/side/qty/price/time ±1s)."""
    epoch = _fill_epoch(fill.get('observed_at'))
    if epoch is None:
        return False
    broker = str(fill.get('broker') or '')
    symbol = str(fill.get('symbol') or '')
    side = str(fill.get('side') or '')
    try:
        qty = float(fill.get('qty') or 0)
        price = float(fill.get('price') or 0)
    except Exception:
        return False
    fill_id = str(fill.get('fill_id') or '')
    for row in conn.execute(
        '''SELECT fill_id, observed_at FROM fills
           WHERE broker=? AND symbol=? AND side=?
             AND abs(qty - ?) < 1e-6 AND abs(price - ?) < 1e-9''',
        (broker, symbol, side, qty, price),
    ):
        if str(row['fill_id']) == fill_id:
            continue
        other = _fill_epoch(row['observed_at'])
        if other is not None and abs(other - epoch) <= 1:
            return True
    return False


def _get_sync_state(conn: sqlite3.Connection, broker: str) -> Dict[str, Any]:
    row = conn.execute(
        'SELECT broker, last_synced_at, cursor, last_imported FROM broker_sync_state WHERE broker=?',
        (str(broker or ''),),
    ).fetchone()
    if not row:
        return {'broker': broker, 'last_synced_at': '', 'cursor': '', 'last_imported': 0}
    return {
        'broker': row['broker'],
        'last_synced_at': str(row['last_synced_at'] or ''),
        'cursor': str(row['cursor'] or ''),
        'last_imported': int(row['last_imported'] or 0),
    }


def _set_sync_state(conn: sqlite3.Connection, broker: str, last_synced_at: str, cursor: str = '', last_imported: int = 0) -> None:
    conn.execute(
        '''
        INSERT INTO broker_sync_state(broker, last_synced_at, cursor, last_imported, updated_at)
        VALUES(?,?,?,?,CURRENT_TIMESTAMP)
        ON CONFLICT(broker) DO UPDATE SET
            last_synced_at=excluded.last_synced_at,
            cursor=excluded.cursor,
            last_imported=excluded.last_imported,
            updated_at=CURRENT_TIMESTAMP
        ''',
        (str(broker or ''), str(last_synced_at or ''), str(cursor or ''), int(last_imported or 0)),
    )


def get_broker_sync_states() -> Dict[str, Dict[str, Any]]:
    init_analytics_db()
    with _DB_LOCK:
        with _connect() as conn:
            rows = conn.execute('SELECT broker, last_synced_at, cursor, last_imported FROM broker_sync_state').fetchall()
            return {str(r['broker']): dict(r) for r in rows}


def _sync_since_iso(broker: str) -> str:
    """ISO start for incremental fetch: last sync minus 1-day overlap, or full history."""
    full_history_start = '2020-01-01T00:00:00'
    try:
        with _connect() as conn:
            state = _get_sync_state(conn, broker)
    except Exception:
        return full_history_start
    raw = str(state.get('last_synced_at') or '').strip()
    if not raw:
        return full_history_start
    try:
        dt = datetime.fromisoformat(raw.replace('Z', '+00:00'))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=LOCAL_TZ)
        overlap = dt.astimezone(LOCAL_TZ) - timedelta(days=1)
        return overlap.replace(tzinfo=None).isoformat()
    except Exception:
        return full_history_start


def _fetch_broker_fill_rows(broker: str, symbol: str = '', lookback_hours: int = 24, since_iso: str = '') -> List[Dict[str, Any]]:
    broker = str(broker or '').strip().lower()
    rows: List[Dict[str, Any]] = []

    if broker == 'bingx':
        from bingx_adapter import BingXBroker
        client = BingXBroker(testnet=False)
        import time as _time
        end_ts = int(_time.time() * 1000)
        if since_iso:
            try:
                start_dt = datetime.fromisoformat(str(since_iso).replace('Z', '+00:00'))
                if start_dt.tzinfo is None:
                    start_dt = start_dt.replace(tzinfo=LOCAL_TZ)
                start_ts = int(start_dt.timestamp() * 1000)
            except Exception:
                start_ts = end_ts - lookback_hours * 60 * 60 * 1000
        else:
            start_ts = end_ts - lookback_hours * 60 * 60 * 1000
        target_symbol = symbol
        if not target_symbol:
            return []
        payload = client.get_all_fill_orders(symbol=target_symbol, start_ts=start_ts, end_ts=end_ts, trading_unit='COIN')
        data = (payload or {}).get('data') or {}
        raw_rows = data.get('fill_orders') or data.get('fillOrders') or data.get('fills') or []
        if isinstance(data, list):
            raw_rows = data
        for row in raw_rows:
            if isinstance(row, dict):
                fill = _normalize_broker_fill_row('bingx', row, default_symbol=target_symbol, default_venue='swap')
                if fill:
                    rows.append(fill)
        return rows

    if broker == 'finam':
        from finam_adapter import fetch_account_trades_sync
        raw_rows = fetch_account_trades_sync(lookback_hours=lookback_hours, since_iso=since_iso)
        for row in raw_rows:
            if not isinstance(row, dict):
                continue
            if symbol and str(row.get('symbol') or '') != symbol:
                continue
            fill = _normalize_broker_fill_row('finam', row, default_venue='MOEX')
            if fill:
                rows.append(fill)
        return rows

    if broker == 'alor':
        from alor_adapter import fetch_account_trades_sync
        raw_rows = fetch_account_trades_sync(lookback_hours=lookback_hours, since_iso=since_iso)
        for row in raw_rows:
            if not isinstance(row, dict):
                continue
            row_symbol = str(row.get('symbol') or row.get('security') or '')
            if symbol and row_symbol != symbol:
                continue
            fill = _normalize_broker_fill_row('alor', row, default_venue=str(row.get('_alorExchange') or row.get('exchange') or 'MOEX'))
            if fill:
                rows.append(fill)
        return rows

    return rows


def sync_exchange_fills(broker: str, symbol: str, lookback_hours: int = 24, since_iso: str = '') -> Dict[str, Any]:
    """Fetch fills from exchange and import ones not tracked by WHR."""
    broker = str(broker or '').strip().lower()
    symbol = str(symbol or '').strip()
    if not broker:
        return {'ok': False, 'imported': 0, 'skipped': 0, 'error': 'missing broker'}
    if broker == 'bingx' and not symbol:
        return {'ok': False, 'imported': 0, 'skipped': 0, 'error': 'missing broker or symbol'}

    try:
        fills = _fetch_broker_fill_rows(broker, symbol=symbol, lookback_hours=lookback_hours, since_iso=since_iso)
        with _DB_LOCK:
            with _connect() as conn:
                _ensure_counter_keys(conn)
                imported, skipped = _import_normalized_fills(conn, broker, fills)
                _refresh_counters(conn)
        return {'ok': True, 'imported': imported, 'skipped': skipped}
    except Exception as e:
        return {'ok': False, 'imported': 0, 'skipped': 0, 'error': str(e)}


def sync_account_fills(broker: str, lookback_hours: int = 72) -> Dict[str, Any]:
    """Import account-level trades from last sync date (or full history on first run)."""
    broker = str(broker or '').strip().lower()
    if broker not in ('finam', 'alor', 'bingx'):
        return {'ok': False, 'imported': 0, 'skipped': 0, 'error': f'unsupported broker: {broker}'}
    try:
        init_analytics_db()
        since_iso = _sync_since_iso(broker)
        fills = _fetch_broker_fill_rows(broker, symbol='', lookback_hours=lookback_hours, since_iso=since_iso)
        imported = 0
        skipped = 0
        max_observed = ''
        with _DB_LOCK:
            with _connect() as conn:
                _ensure_counter_keys(conn)
                imported, skipped = _import_normalized_fills(conn, broker, fills)
                for fill in fills:
                    observed = str(fill.get('observed_at') or '')
                    if observed and observed > max_observed:
                        max_observed = observed
                _refresh_counters(conn)
        now_iso = datetime.now(LOCAL_TZ).isoformat()
        last_synced = max_observed or now_iso
        with _DB_LOCK:
            with _connect() as conn:
                _set_sync_state(conn, broker, last_synced_at=last_synced, last_imported=imported)
        result = {
            'ok': True,
            'imported': imported,
            'skipped': skipped,
            'since': since_iso,
            'lastSyncedAt': last_synced,
        }
        if broker == 'finam':
            income = sync_exchange_income(broker, symbol='', lookback_hours=lookback_hours, since_iso=since_iso)
            result['income'] = income
        return result
    except Exception as e:
        return {'ok': False, 'imported': 0, 'skipped': 0, 'error': str(e)}


def sync_exchange_income(broker: str, symbol: str = '', lookback_hours: int = 72, since_iso: str = '') -> Dict[str, Any]:
    """Fetch income (REALIZED_PNL, TRADING_FEE, Finam cash ops) and store as fact."""
    broker = str(broker or '').strip().lower()
    symbol = str(symbol or '').strip()
    if not broker:
        return {'ok': False, 'imported': 0, 'error': 'missing broker'}

    try:
        imported = 0
        if broker == 'bingx':
            if not symbol:
                return {'ok': False, 'imported': 0, 'error': 'missing symbol for bingx'}
            from bingx_adapter import BingXBroker
            client = BingXBroker(testnet=False)
            import time as _time
            end_ts = int(_time.time() * 1000)
            start_ts = end_ts - lookback_hours * 60 * 60 * 1000

            for income_type in ('REALIZED_PNL', 'TRADING_FEE', 'FUNDING_FEE', 'INSURANCE_CLEAR'):
                payload = client.get_income(symbol=symbol, income_type=income_type, start_time=start_ts, end_time=end_ts, limit=1000)
                data = (payload or {}).get('data') or []
                if isinstance(data, dict):
                    data = [data]
                rows = [r for r in data if isinstance(r, dict)]

                with _DB_LOCK:
                    with _connect() as conn:
                        _ensure_counter_keys(conn)
                        for row in rows:
                            trade_id = str(row.get('tradeId') or '')
                            order_id = str(row.get('orderId') or '')
                            ts_val = row.get('time')
                            income_id = f'income:{broker}:{symbol}:{income_type}:{trade_id or order_id or ts_val or "0"}'

                            exists = conn.execute('SELECT 1 FROM exchange_income WHERE income_id = ?', (income_id,)).fetchone()
                            if exists:
                                continue

                            observed_at = ''
                            if ts_val:
                                try:
                                    ts_int = int(ts_val)
                                    observed_at = datetime.fromtimestamp(ts_int / 1000, tz=LOCAL_TZ).isoformat()
                                except Exception:
                                    observed_at = str(ts_val)

                            conn.execute(
                                '''INSERT OR IGNORE INTO exchange_income(income_id, broker, symbol, income_type, income, asset, trade_id, order_id, observed_at, raw_json)
                                VALUES(?,?,?,?,?,?,?,?,?,?)''',
                                (
                                    income_id, broker, symbol, income_type,
                                    _to_float(row.get('income')),
                                    str(row.get('asset') or ''),
                                    trade_id, order_id, observed_at, _json_text(row),
                                ),
                            )
                            imported += 1
                        _refresh_counters(conn)

            return {'ok': True, 'imported': imported}

        if broker == 'finam':
            from finam_adapter import fetch_account_transactions_sync
            rows = [r for r in (fetch_account_transactions_sync(lookback_hours=lookback_hours, since_iso=since_iso) or []) if isinstance(r, dict)]
            with _DB_LOCK:
                with _connect() as conn:
                    _ensure_counter_keys(conn)
                    for row in rows:
                        tx_id = str(row.get('id') or '')
                        income_type = str(row.get('category') or row.get('transaction_category') or 'CASH').upper()
                        row_symbol = str(row.get('symbol') or '')
                        symbols = row.get('symbols') or []
                        if not row_symbol and isinstance(symbols, list) and symbols:
                            row_symbol = str(symbols[0] or '')
                        if symbol and row_symbol and row_symbol != symbol:
                            continue
                        if symbol and not row_symbol:
                            row_symbol = symbol
                        change = row.get('change') or {}
                        income = 0.0
                        if isinstance(change, dict):
                            income = _to_float(change.get('units')) + _to_float(change.get('nanos')) / 1_000_000_000.0
                        else:
                            income = _to_float(change)
                        income_id = f'income:{broker}:{row_symbol or "account"}:{income_type}:{tx_id or "0"}'
                        exists = conn.execute('SELECT 1 FROM exchange_income WHERE income_id = ?', (income_id,)).fetchone()
                        if exists:
                            continue
                        observed_at = _observed_at_from_row(row)
                        conn.execute(
                            '''INSERT OR IGNORE INTO exchange_income(income_id, broker, symbol, income_type, income, asset, trade_id, order_id, observed_at, raw_json)
                            VALUES(?,?,?,?,?,?,?,?,?,?)''',
                            (
                                income_id, broker, row_symbol, income_type,
                                income,
                                str((change.get('currency_code') if isinstance(change, dict) else '') or row.get('currency') or ''),
                                '', tx_id, observed_at, _json_text(row),
                            ),
                        )
                        imported += 1
                    _refresh_counters(conn)
            return {'ok': True, 'imported': imported}

        return {'ok': False, 'imported': 0, 'error': f'unsupported broker: {broker}'}
    except Exception as e:
        return {'ok': False, 'imported': 0, 'error': str(e)}


def _sync_all_tracked_symbols() -> Dict[str, Any]:
    """Sync fills from exchange for all broker+symbol pairs seen in analytics."""
    try:
        with _connect() as conn:
            rows = conn.execute(
                'SELECT DISTINCT broker, symbol FROM executions WHERE broker IS NOT NULL AND symbol IS NOT NULL'
            ).fetchall()
    except Exception:
        rows = []

    total_imported = 0
    total_skipped = 0
    account_results: Dict[str, Any] = {}

    # Account-level brokers: incremental sync from last_synced_at (full history on first run).
    for broker in ('finam', 'alor'):
        result = sync_account_fills(broker)
        account_results[broker] = result
        total_imported += int(result.get('imported') or 0)
        total_skipped += int(result.get('skipped') or 0)

    for row in rows:
        broker = str(row['broker'] or '').strip().lower()
        symbol = str(row['symbol'] or '').strip()
        if not broker or not symbol:
            continue
        if broker != 'bingx':
            continue
        since_iso = _sync_since_iso('bingx:' + symbol)
        result = sync_exchange_fills(broker, symbol, lookback_hours=72, since_iso=since_iso)
        total_imported += int(result.get('imported') or 0)
        total_skipped += int(result.get('skipped') or 0)
        sync_exchange_income(broker, symbol, lookback_hours=72)
        if result.get('ok'):
            with _DB_LOCK:
                with _connect() as conn:
                    _set_sync_state(conn, 'bingx:' + symbol, last_synced_at=datetime.now(LOCAL_TZ).isoformat(), last_imported=int(result.get('imported') or 0))
    return {
        'syncedSymbols': len(rows),
        'imported': total_imported,
        'skipped': total_skipped,
        'accountSync': account_results,
    }


def rebuild_analytics() -> Dict[str, Any]:
    init_analytics_db()
    sync_result = _sync_all_tracked_symbols()
    with _DB_LOCK:
        with _connect() as conn:
            result = _rebuild_all_locked(conn)
            result['exchangeSync'] = sync_result
            return result


def rebuild_analytics_today() -> Dict[str, Any]:
    init_analytics_db()
    sync_result = _sync_all_tracked_symbols()
    today = datetime.now(LOCAL_TZ).date().isoformat()
    with _DB_LOCK:
        with _connect() as conn:
            result = _rebuild_day_locked(conn, today)
            result['exchangeSync'] = sync_result
            result['day'] = today
            result['tz'] = 'Europe/Moscow'
            return result


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(str(ANALYTICS_DB_PATH), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')
    conn.execute('PRAGMA foreign_keys=ON')
    conn.execute('PRAGMA temp_store=MEMORY')
    return conn


def _load_fill_rows_with_requests(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    return conn.execute(
        '''
        SELECT
            f.fill_id, f.execution_id, f.signal_id, f.order_local_id, f.broker_order_id, f.fill_seq,
            f.phase, f.observed_at, f.broker, f.symbol, f.venue, f.side, f.qty, f.price, f.notional,
            f.commission, f.commission_currency, f.liquidity_flag, f.position_effect, f.source_type,
            f.raw_json, e.request_json
        FROM fills f
        LEFT JOIN executions e ON e.execution_id = f.execution_id
        ORDER BY f.observed_at, f.fill_id
        '''
    ).fetchall()


def _recompute_fill_effect(conn: sqlite3.Connection, fill: Dict[str, Any]) -> bool:
    request: Dict[str, Any] = {}
    try:
        loaded = json.loads(str(fill.get('request_json') or '{}'))
        if isinstance(loaded, dict):
            request = loaded
    except Exception:
        request = {}
    new_effect = _infer_position_effect(str(fill.get('phase') or ''), str(fill.get('side') or ''), request)
    changed = new_effect != str(fill.get('position_effect') or '')
    if changed:
        conn.execute('UPDATE fills SET position_effect=? WHERE fill_id=?', (new_effect, str(fill.get('fill_id') or '')))
        fill['position_effect'] = new_effect
    return changed


def _rebuild_all_locked(conn: sqlite3.Connection) -> Dict[str, Any]:
    _ensure_counter_keys(conn)
    fill_rows = _load_fill_rows_with_requests(conn)
    _clear_derived_analytics(conn)
    recomputed_effects = 0
    replayed_fills = 0
    for row in fill_rows:
        fill = dict(row)
        if _recompute_fill_effect(conn, fill):
            recomputed_effects += 1
        replayed_fills += 1
        _apply_fill_to_positions(conn, fill)
    _refresh_counters(conn)
    counters = {
        str(row['counter_key']): int(row['counter_value'] or 0)
        for row in conn.execute('SELECT counter_key, counter_value FROM analytics_counters').fetchall()
    }
    return {
        'ok': True,
        'replayedFills': replayed_fills,
        'recomputedEffects': recomputed_effects,
        'counters': counters,
    }


def _rebuild_day_locked(conn: sqlite3.Connection, day: str) -> Dict[str, Any]:
    _ensure_counter_keys(conn)
    fill_rows = _load_fill_rows_with_requests(conn)
    _clear_derived_analytics(conn)
    pre_day_count = 0
    day_count = 0
    recomputed_effects = 0
    for row in fill_rows:
        fill = dict(row)
        if _recompute_fill_effect(conn, fill):
            recomputed_effects += 1
        fill_day = _observed_local_day(str(fill.get('observed_at') or ''))
        if fill_day and fill_day < day:
            _apply_fill_to_positions(conn, fill)
            pre_day_count += 1
    conn.execute('DELETE FROM round_trip_fills')
    conn.execute('DELETE FROM round_trips')
    conn.execute('DELETE FROM daily_trade_stats')
    for row in fill_rows:
        fill = dict(row)
        fill_day = _observed_local_day(str(fill.get('observed_at') or ''))
        if fill_day == day:
            _apply_fill_to_positions(conn, fill)
            day_count += 1
    _refresh_counters(conn)
    counters = {
        str(row['counter_key']): int(row['counter_value'] or 0)
        for row in conn.execute('SELECT counter_key, counter_value FROM analytics_counters').fetchall()
    }
    return {
        'ok': True,
        'mode': 'today_only',
        'seedFillsBeforeDay': pre_day_count,
        'replayedDayFills': day_count,
        'recomputedEffects': recomputed_effects,
        'counters': counters,
    }


def _ensure_counter_keys(conn: sqlite3.Connection) -> None:
    for key in ('signals', 'executions', 'orders', 'fills', 'round_trips'):
        conn.execute('INSERT OR IGNORE INTO analytics_counters(counter_key, counter_value) VALUES(?, 0)', (key,))


def _clear_derived_analytics(conn: sqlite3.Connection) -> None:
    conn.execute('DELETE FROM round_trip_fills')
    conn.execute('DELETE FROM round_trips')
    conn.execute('DELETE FROM daily_trade_stats')
    conn.execute('DELETE FROM open_lots')


def _refresh_counters(conn: sqlite3.Connection) -> None:
    counts = {
        'signals': int((conn.execute('SELECT COUNT(*) AS c FROM signals').fetchone()['c']) or 0),
        'executions': int((conn.execute('SELECT COUNT(*) AS c FROM executions').fetchone()['c']) or 0),
        'orders': int((conn.execute('SELECT COUNT(*) AS c FROM orders').fetchone()['c']) or 0),
        'fills': int((conn.execute('SELECT COUNT(*) AS c FROM fills').fetchone()['c']) or 0),
        'round_trips': int((conn.execute('SELECT COUNT(*) AS c FROM round_trips').fetchone()['c']) or 0),
    }
    for key, value in counts.items():
        conn.execute(
            '''
            INSERT INTO analytics_counters(counter_key, counter_value, updated_at)
            VALUES(?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(counter_key) DO UPDATE SET
                counter_value = excluded.counter_value,
                updated_at = CURRENT_TIMESTAMP
            ''',
            (key, value),
        )


def _counter_increment(conn: sqlite3.Connection, counter_key: str, delta: int = 1) -> None:
    conn.execute(
        '''
        INSERT INTO analytics_counters(counter_key, counter_value, updated_at)
        VALUES(?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(counter_key) DO UPDATE SET
            counter_value = counter_value + excluded.counter_value,
            updated_at = CURRENT_TIMESTAMP
        ''',
        (counter_key, int(delta)),
    )


def _row_exists(conn: sqlite3.Connection, table: str, key_name: str, key_value: str) -> bool:
    row = conn.execute(f'SELECT 1 FROM {table} WHERE {key_name}=? LIMIT 1', (key_value,)).fetchone()
    return bool(row)


def _signal_id(decision: Dict[str, Any]) -> str:
    raw = str((decision or {}).get('jobId') or '').strip()
    if raw:
        return raw
    basis = json.dumps({
        'receivedAt': decision.get('receivedAt'),
        'origin': decision.get('origin'),
        'payload': decision.get('payload'),
        'route': decision.get('route'),
    }, ensure_ascii=False, sort_keys=True, default=str)
    return 'signal-' + hashlib.sha1(basis.encode('utf-8')).hexdigest()[:24]


def _execution_id(signal_id: str, destination_index: int) -> str:
    return f'{signal_id}:dest:{destination_index + 1}'


def _safe_name(value: Any) -> str:
    text = str(value or '').strip().lower()
    safe = []
    for ch in text:
        safe.append(ch if ch.isalnum() else '-')
    return ''.join(safe).strip('-') or 'na'


def _json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _upsert_signal(conn: sqlite3.Connection, signal_id: str, decision: Dict[str, Any]) -> None:
    payload = decision.get('payload') or {}
    route = decision.get('executionResult') or decision.get('route') or {}
    inserted = not _row_exists(conn, 'signals', 'signal_id', signal_id)
    conn.execute(
        '''
        INSERT INTO signals(signal_id, received_at, origin, route_id, route_name, source_ticker, side, qty_text, payload_json)
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(signal_id) DO UPDATE SET
            received_at=excluded.received_at,
            origin=excluded.origin,
            route_id=excluded.route_id,
            route_name=excluded.route_name,
            source_ticker=excluded.source_ticker,
            side=excluded.side,
            qty_text=excluded.qty_text,
            payload_json=excluded.payload_json
        ''',
        (
            signal_id,
            str(decision.get('receivedAt') or ''),
            str(decision.get('origin') or ''),
            str(route.get('routeId') or (decision.get('route') or {}).get('id') or ''),
            str(route.get('routeName') or (decision.get('route') or {}).get('name') or ''),
            str(payload.get('sourceTicker') or payload.get('ticker') or ''),
            str(payload.get('side') or ''),
            str(payload.get('qty') or ''),
            _json_text(payload),
        ),
    )
    if inserted:
        _counter_increment(conn, 'signals', 1)


def _upsert_execution(conn: sqlite3.Connection, signal_id: str, execution_id: str, destination_index: int, decision: Dict[str, Any], destination: Dict[str, Any]) -> None:
    request = destination.get('request') or {}
    result = destination.get('results') or {}
    inserted = not _row_exists(conn, 'executions', 'execution_id', execution_id)
    venue = destination.get('category') or destination.get('exchange') or destination.get('account') or ''
    error_text = str(destination.get('error') or result.get('error') or result.get('msg') or '')
    status = 'dry_run' if bool(destination.get('dryRun')) else ('error' if error_text else 'placed')
    conn.execute(
        '''
        INSERT INTO executions(
            execution_id, signal_id, destination_index, received_at, broker, symbol, venue, account, side,
            requested_qty_text, execution_mode, signal_mode, status, error_text, dry_run, request_json, result_json
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(execution_id) DO UPDATE SET
            signal_id=excluded.signal_id,
            destination_index=excluded.destination_index,
            received_at=excluded.received_at,
            broker=excluded.broker,
            symbol=excluded.symbol,
            venue=excluded.venue,
            account=excluded.account,
            side=excluded.side,
            requested_qty_text=excluded.requested_qty_text,
            execution_mode=excluded.execution_mode,
            signal_mode=excluded.signal_mode,
            status=excluded.status,
            error_text=excluded.error_text,
            dry_run=excluded.dry_run,
            request_json=excluded.request_json,
            result_json=excluded.result_json
        ''',
        (
            execution_id,
            signal_id,
            int(destination_index),
            str(decision.get('receivedAt') or ''),
            str(destination.get('broker') or ''),
            str(destination.get('symbol') or ''),
            str(venue),
            str(destination.get('account') or ''),
            str(request.get('side') or destination.get('side') or ''),
            str(request.get('qty') or destination.get('qty') or ''),
            str(request.get('executionMode') or request.get('mode') or ''),
            str(request.get('signalMode') or ''),
            status,
            error_text,
            1 if bool(destination.get('dryRun')) else 0,
            _json_text(request),
            _json_text(result),
        ),
    )
    if inserted:
        _counter_increment(conn, 'executions', 1)


def _extract_orders_and_fills(signal_id: str, execution_id: str, destination: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    broker = str(destination.get('broker') or '')
    request = destination.get('request') or {}
    venue = str(destination.get('category') or destination.get('exchange') or destination.get('account') or '')
    symbol = str(destination.get('symbol') or request.get('symbol') or request.get('ticker') or '')
    side = str(request.get('side') or '').lower()
    client_order_id = str(request.get('clientOrderId') or '')
    is_reduce_only = 1 if bool(request.get('reduceOnly')) else 0 if 'reduceOnly' in request else None

    orders: List[Dict[str, Any]] = []
    fills: List[Dict[str, Any]] = []
    order_attempts = request.get('orderAttempts') or []
    if isinstance(order_attempts, list) and order_attempts:
        for item in order_attempts:
            if not isinstance(item, dict):
                continue
            phase = str(item.get('phase') or 'primary')
            attempt_no = _to_int(item.get('attempt')) or (len(orders) + 1)
            order_local_id = f"{execution_id}:order:{_safe_name(phase)}:{attempt_no}"
            broker_order_id = str(item.get('orderId') or '')
            executed_qty = _to_float(item.get('executedQty'))
            fill_rows = item.get('fills') or []
            if not fill_rows and executed_qty > 0:
                fill_rows = [{
                    'seq': 1,
                    'observedAt': item.get('observedCompletedAt') or item.get('observedStartedAt') or '',
                    'qty': executed_qty,
                    'price': _to_float(item.get('avgFillPrice')) or _to_float(item.get('placedPrice')),
                    'commission': _to_float(item.get('commissionTotal')),
                    'commissionCurrency': item.get('commissionCurrency') or '',
                    'sourceType': 'attempt-summary',
                }]
            commission_total = sum(_to_float(fill.get('commission')) for fill in fill_rows if isinstance(fill, dict))
            avg_fill_price = _weighted_avg_price(fill_rows, fallback_price=_to_float(item.get('placedPrice')))
            orders.append({
                'order_local_id': order_local_id,
                'execution_id': execution_id,
                'signal_id': signal_id,
                'broker_order_id': broker_order_id,
                'client_order_id': client_order_id,
                'phase': phase,
                'attempt_no': attempt_no,
                'side': side,
                'symbol': symbol,
                'venue': venue,
                'placed_price': _to_float(item.get('placedPrice')),
                'requested_qty': _to_float(item.get('placedQty')),
                'executed_qty': executed_qty,
                'remaining_qty': _to_float(item.get('remainingQty')),
                'avg_fill_price': avg_fill_price,
                'status': str(item.get('finalStatus') or item.get('confirmedStatus') or ''),
                'commission_total': commission_total,
                'commission_currency': _first_nonempty([fill.get('commissionCurrency') for fill in fill_rows if isinstance(fill, dict)]),
                'fill_count': len([fill for fill in fill_rows if isinstance(fill, dict) and _to_float(fill.get('qty')) > 0]),
                'is_reduce_only': is_reduce_only,
                'observed_started_at': str(item.get('observedStartedAt') or ''),
                'observed_completed_at': str(item.get('observedCompletedAt') or ''),
                'raw_json': _json_text(item),
            })
            fill_seq = 0
            for fill in fill_rows:
                if not isinstance(fill, dict):
                    continue
                qty = _to_float(fill.get('qty'))
                if qty <= 0:
                    continue
                fill_seq += 1
                price = _to_float(fill.get('price')) or _to_float(item.get('placedPrice'))
                fills.append({
                    'fill_id': f'{order_local_id}:fill:{fill_seq}',
                    'execution_id': execution_id,
                    'signal_id': signal_id,
                    'order_local_id': order_local_id,
                    'broker_order_id': broker_order_id,
                    'fill_seq': fill_seq,
                    'phase': phase,
                    'observed_at': str(fill.get('observedAt') or item.get('observedCompletedAt') or item.get('observedStartedAt') or ''),
                    'broker': broker,
                    'symbol': symbol,
                    'venue': venue,
                    'side': side,
                    'qty': qty,
                    'price': price,
                    'notional': qty * price if price else 0.0,
                    'commission': _to_float(fill.get('commission')),
                    'commission_currency': str(fill.get('commissionCurrency') or ''),
                    'liquidity_flag': str(fill.get('liquidityFlag') or ''),
                    'position_effect': _infer_position_effect(phase, side, request),
                    'source_type': str(fill.get('sourceType') or 'unknown'),
                    'raw_json': _json_text(fill),
                })
    else:
        result = destination.get('results') or {}
        broker_order_id = str(result.get('orderId') or result.get('orderID') or result.get('id') or '')
        if broker_order_id or result or request:
            order_local_id = f'{execution_id}:order:primary:1'
            orders.append({
                'order_local_id': order_local_id,
                'execution_id': execution_id,
                'signal_id': signal_id,
                'broker_order_id': broker_order_id,
                'client_order_id': client_order_id,
                'phase': 'primary',
                'attempt_no': 1,
                'side': side,
                'symbol': symbol,
                'venue': venue,
                'placed_price': _to_float(request.get('price')),
                'requested_qty': _to_float(request.get('qty') or destination.get('qty')),
                'executed_qty': _to_float(result.get('executedQty') or result.get('cumExecQty') or result.get('filledQty')),
                'remaining_qty': _to_float(result.get('remainingQty')),
                'avg_fill_price': _to_float(result.get('avgPrice') or result.get('price')),
                'status': str(result.get('status') or ''),
                'commission_total': _to_float(result.get('commission') or result.get('fee')),
                'commission_currency': str(result.get('feeCurrency') or ''),
                'fill_count': 0,
                'is_reduce_only': is_reduce_only,
                'observed_started_at': '',
                'observed_completed_at': '',
                'raw_json': _json_text({'request': request, 'result': result}),
            })
    return orders, fills


def _upsert_order(conn: sqlite3.Connection, order: Dict[str, Any]) -> None:
    inserted = not _row_exists(conn, 'orders', 'order_local_id', str(order.get('order_local_id') or ''))
    conn.execute(
        '''
        INSERT INTO orders(
            order_local_id, execution_id, signal_id, broker_order_id, client_order_id, phase, attempt_no, side, symbol, venue,
            placed_price, requested_qty, executed_qty, remaining_qty, avg_fill_price, status, commission_total, commission_currency,
            fill_count, is_reduce_only, observed_started_at, observed_completed_at, raw_json
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(order_local_id) DO UPDATE SET
            execution_id=excluded.execution_id,
            signal_id=excluded.signal_id,
            broker_order_id=excluded.broker_order_id,
            client_order_id=excluded.client_order_id,
            phase=excluded.phase,
            attempt_no=excluded.attempt_no,
            side=excluded.side,
            symbol=excluded.symbol,
            venue=excluded.venue,
            placed_price=excluded.placed_price,
            requested_qty=excluded.requested_qty,
            executed_qty=excluded.executed_qty,
            remaining_qty=excluded.remaining_qty,
            avg_fill_price=excluded.avg_fill_price,
            status=excluded.status,
            commission_total=excluded.commission_total,
            commission_currency=excluded.commission_currency,
            fill_count=excluded.fill_count,
            is_reduce_only=excluded.is_reduce_only,
            observed_started_at=excluded.observed_started_at,
            observed_completed_at=excluded.observed_completed_at,
            raw_json=excluded.raw_json
        ''',
        (
            order['order_local_id'], order['execution_id'], order['signal_id'], order['broker_order_id'], order['client_order_id'],
            order['phase'], order['attempt_no'], order['side'], order['symbol'], order['venue'], order['placed_price'],
            order['requested_qty'], order['executed_qty'], order['remaining_qty'], order['avg_fill_price'], order['status'],
            order['commission_total'], order['commission_currency'], order['fill_count'], order['is_reduce_only'],
            order['observed_started_at'], order['observed_completed_at'], order['raw_json'],
        ),
    )
    if inserted:
        _counter_increment(conn, 'orders', 1)


def _insert_fill(conn: sqlite3.Connection, fill: Dict[str, Any]) -> bool:
    cursor = conn.execute(
        '''
        INSERT OR IGNORE INTO fills(
            fill_id, execution_id, signal_id, order_local_id, broker_order_id, fill_seq, phase, observed_at, broker,
            symbol, venue, side, qty, price, notional, commission, commission_currency, liquidity_flag, position_effect,
            source_type, raw_json
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            fill['fill_id'], fill['execution_id'], fill['signal_id'], fill['order_local_id'], fill['broker_order_id'], fill['fill_seq'],
            fill['phase'], fill['observed_at'], fill['broker'], fill['symbol'], fill['venue'], fill['side'], fill['qty'], fill['price'],
            fill['notional'], fill['commission'], fill['commission_currency'], fill['liquidity_flag'], fill['position_effect'],
            fill['source_type'], fill['raw_json'],
        ),
    )
    inserted = bool(cursor.rowcount)
    if inserted:
        _counter_increment(conn, 'fills', 1)
    return inserted


def _hedge_position_side(fill: Dict[str, Any]) -> str:
    """LONG/SHORT from BingX fill payload (empty = one-way / netting)."""
    try:
        raw = fill.get('raw_json') or '{}'
        if isinstance(raw, str):
            raw = json.loads(raw)
        if isinstance(raw, dict):
            ps = str(raw.get('positionSide') or '').upper()
            if ps in ('LONG', 'SHORT'):
                return ps
    except Exception:
        pass
    return ''


def _apply_fill_to_positions(conn: sqlite3.Connection, fill: Dict[str, Any]) -> None:
    fill_side = str(fill.get('side') or '').lower()
    if fill_side not in ('buy', 'sell'):
        return
    broker = str(fill.get('broker') or '')
    symbol = str(fill.get('symbol') or '')
    venue = str(fill.get('venue') or '')
    position_effect = str(fill.get('position_effect') or '').lower()
    hedge = _hedge_position_side(fill)
    qty_remaining = Decimal(str(fill.get('qty') or 0))
    if qty_remaining <= 0:
        return
    price = Decimal(str(fill.get('price') or 0))
    fill_commission_remaining = Decimal(str(fill.get('commission') or 0))
    opposite_side = 'sell' if fill_side == 'buy' else 'buy'

    close_like_effects = {'close', 'open_or_close', 'reduce_long', 'reduce_short'}
    open_like_effects = {'open', 'open_or_close', 'add_long', 'add_short', 'reduce_long', 'reduce_short'}

    # Hedge mode: LONG/SHORT are separate books — do not net them.
    # LONG: sell closes longs only; buy opens/increases longs only.
    # SHORT: buy closes shorts only; sell opens/increases shorts only.
    allow_close = position_effect in close_like_effects
    allow_open = position_effect in open_like_effects
    if hedge == 'LONG':
        allow_close = (fill_side == 'sell' and allow_close)
        allow_open = (fill_side == 'buy' and allow_open)
        if fill_side == 'buy':
            opposite_side_for_close = 'buy'  # unused
        else:
            opposite_side_for_close = 'buy'  # sell closes long lots
    elif hedge == 'SHORT':
        allow_close = (fill_side == 'buy' and allow_close)
        allow_open = (fill_side == 'sell' and allow_open)
        opposite_side_for_close = 'sell'  # buy closes short lots
    else:
        opposite_side_for_close = opposite_side

    rows = []
    if allow_close:
        rows = conn.execute(
            '''
            SELECT * FROM open_lots
            WHERE broker=? AND symbol=? AND venue=? AND side=? AND remaining_qty > 0
            ORDER BY opened_at, lot_id
            ''',
            (broker, symbol, venue, opposite_side_for_close),
        ).fetchall()

    if rows:
        total_open_qty = sum(Decimal(str(r['remaining_qty'] or 0)) for r in rows)
        total_open_commission = sum(Decimal(str(r['remaining_commission'] or 0)) for r in rows)
        avg_entry_price = Decimal('0')
        if total_open_qty > 0:
            weighted_sum = sum(Decimal(str(r['open_price'] or 0)) * Decimal(str(r['remaining_qty'] or 0)) for r in rows)
            avg_entry_price = weighted_sum / total_open_qty

        matched_qty = min(qty_remaining, total_open_qty)
        if matched_qty > 0:
            entry_commission_alloc = (total_open_commission * matched_qty / total_open_qty) if total_open_qty > 0 else Decimal('0')
            exit_commission_alloc = (fill_commission_remaining * matched_qty / qty_remaining) if qty_remaining > 0 else Decimal('0')

            if rows[0]['side'] == 'buy':
                gross_pnl = (price - avg_entry_price) * matched_qty
                direction = 'long'
            else:
                gross_pnl = (avg_entry_price - price) * matched_qty
                direction = 'short'
            commission_total = entry_commission_alloc + exit_commission_alloc
            net_pnl = gross_pnl - commission_total
            earliest_open = min(str(r['opened_at'] or '') for r in rows)
            round_trip_id = _round_trip_id(f"avg:{broker}:{symbol}", fill['fill_id'], matched_qty)
            holding_time_sec = _holding_seconds(earliest_open, str(fill.get('observed_at') or ''))

            conn.execute(
                '''
                INSERT OR IGNORE INTO round_trips(
                    round_trip_id, broker, symbol, venue, direction, opened_at, closed_at, holding_time_sec,
                    entry_qty, exit_qty, entry_avg_price, exit_avg_price, gross_pnl, entry_commission, exit_commission,
                    commission_total, net_pnl, entry_fill_count, exit_fill_count, entry_order_count, exit_order_count,
                    opening_fill_id, closing_fill_id, opening_order_local_id, closing_order_local_id,
                    opening_signal_id, closing_signal_id, opening_execution_id, closing_execution_id
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 1, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    round_trip_id, broker, symbol, venue, direction,
                    earliest_open, str(fill.get('observed_at') or ''), holding_time_sec,
                    float(matched_qty), float(matched_qty), float(avg_entry_price), float(price),
                    float(gross_pnl), float(entry_commission_alloc), float(exit_commission_alloc),
                    float(commission_total), float(net_pnl),
                    len(rows), 1,
                    None,
                    str(fill.get('fill_id')) if fill.get('fill_id') else None,
                    None,
                    str(fill.get('order_local_id')) if fill.get('order_local_id') else None,
                    None,
                    str(fill.get('signal_id')) if fill.get('signal_id') else None,
                    None,
                    str(fill.get('execution_id')) if fill.get('execution_id') else None,
                ),
            )
            _update_daily_trade_stats(conn, str(fill.get('observed_at') or ''), broker, symbol, venue, matched_qty, gross_pnl, commission_total, net_pnl)

            ratio = matched_qty / total_open_qty if total_open_qty > 0 else Decimal('0')
            for r in rows:
                lot_qty = Decimal(str(r['remaining_qty'] or 0))
                lot_comm = Decimal(str(r['remaining_commission'] or 0))
                reduce_qty = lot_qty * ratio
                reduce_comm = lot_comm * ratio
                new_qty = lot_qty - reduce_qty
                new_comm = lot_comm - reduce_comm
                if new_qty <= Decimal('0.00000001'):
                    conn.execute('DELETE FROM open_lots WHERE lot_id=?', (str(r['lot_id']),))
                else:
                    conn.execute(
                        'UPDATE open_lots SET remaining_qty=?, remaining_commission=? WHERE lot_id=?',
                        (float(new_qty), float(new_comm), str(r['lot_id'])),
                    )

            qty_remaining -= matched_qty
            fill_commission_remaining -= exit_commission_alloc

    if qty_remaining > 0 and allow_open:
        lot_id = f"lot:{fill['fill_id']}"
        conn.execute(
            '''
            INSERT OR IGNORE INTO open_lots(
                lot_id, broker, symbol, venue, side, opened_at, remaining_qty, remaining_commission,
                open_price, open_fill_id, open_order_local_id, open_execution_id, open_signal_id
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                lot_id,
                broker,
                symbol,
                venue,
                fill_side,
                str(fill.get('observed_at') or ''),
                float(qty_remaining),
                float(fill_commission_remaining),
                float(price),
                str(fill.get('fill_id')) if fill.get('fill_id') else None,
                str(fill.get('order_local_id')) if fill.get('order_local_id') else None,
                str(fill.get('execution_id')) if fill.get('execution_id') else None,
                str(fill.get('signal_id')) if fill.get('signal_id') else None,
            ),
        )


def _update_daily_trade_stats(
    conn: sqlite3.Connection,
    closed_at: str,
    broker: str,
    symbol: str,
    venue: str,
    matched_qty: Decimal,
    gross_pnl: Decimal,
    commission_total: Decimal,
    net_pnl: Decimal,
) -> None:
    trade_day = str(closed_at or '')[:10]
    lot_bucket = _lot_bucket(matched_qty)
    conn.execute(
        '''
        INSERT INTO daily_trade_stats(trade_day, broker, symbol, venue, lot_bucket, trades_count, gross_pnl_sum, commission_sum, net_pnl_sum, entry_qty_sum)
        VALUES(?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
        ON CONFLICT(trade_day, broker, symbol, venue, lot_bucket) DO UPDATE SET
            trades_count = trades_count + 1,
            gross_pnl_sum = gross_pnl_sum + excluded.gross_pnl_sum,
            commission_sum = commission_sum + excluded.commission_sum,
            net_pnl_sum = net_pnl_sum + excluded.net_pnl_sum,
            entry_qty_sum = entry_qty_sum + excluded.entry_qty_sum
        ''',
        (trade_day, broker, symbol, venue, lot_bucket, float(gross_pnl), float(commission_total), float(net_pnl), float(matched_qty)),
    )


def _lot_bucket(qty: Decimal) -> str:
    value = float(qty)
    if value <= 1:
        return '1'
    if value <= 3:
        return '2-3'
    if value <= 5:
        return '4-5'
    return '6+'


def _round_trip_id(lot_id: str, fill_id: str, matched_qty: Decimal) -> str:
    raw = f'{lot_id}|{fill_id}|{matched_qty}'
    return 'rt-' + hashlib.sha1(raw.encode('utf-8')).hexdigest()[:24]


def _parse_dt(value: str) -> datetime | None:
    try:
        if not value:
            return None
        return datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    except Exception:
        return None


def _observed_local_day(value: str) -> str:
    dt = _parse_dt(value)
    if not dt:
        return ''
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=LOCAL_TZ)
    return dt.astimezone(LOCAL_TZ).date().isoformat()


def _holding_seconds(opened_at: str, closed_at: str) -> float | None:
    try:
        if not opened_at or not closed_at:
            return None
        open_dt = _parse_dt(opened_at)
        close_dt = _parse_dt(closed_at)
        if not open_dt or not close_dt:
            return None
        return max(0.0, (close_dt - open_dt).total_seconds())
    except Exception:
        return None


def _infer_position_effect(phase: str, side: str, request: Dict[str, Any] | None = None) -> str:
    phase_text = str(phase or '').lower()
    if 'close' in phase_text:
        return 'close'
    if 'open' in phase_text:
        return 'open'

    request = request or {}
    signal_mode = str(request.get('signalMode') or '').strip().lower()
    netting_action = str(request.get('nettingAction') or '').strip().lower()
    target_direction = str(request.get('targetDirection') or '').strip().lower()

    if signal_mode == 'target-direction':
        if netting_action in ('target_direction_open_or_increase_target', 'open_same_side_leg'):
            if target_direction == 'long':
                return 'add_long'
            if target_direction == 'short':
                return 'add_short'
        if netting_action in ('target_direction_close_opposite_then_open_target', 'close_opposite_leg_only') or bool(request.get('reduceOnly')):
            if target_direction == 'long':
                return 'reduce_short'
            if target_direction == 'short':
                return 'reduce_long'
            if side == 'buy':
                return 'reduce_short'
            if side == 'sell':
                return 'reduce_long'
            return 'close'

    if bool(request.get('reduceOnly')):
        if side == 'buy':
            return 'reduce_short'
        if side == 'sell':
            return 'reduce_long'
        return 'close'

    return 'open_or_close' if side in ('buy', 'sell') else 'unknown'


def _weighted_avg_price(fill_rows: List[Dict[str, Any]], fallback_price: float = 0.0) -> float:
    total_qty = 0.0
    total_notional = 0.0
    for fill in fill_rows:
        if not isinstance(fill, dict):
            continue
        qty = _to_float(fill.get('qty'))
        price = _to_float(fill.get('price')) or fallback_price
        if qty <= 0 or price <= 0:
            continue
        total_qty += qty
        total_notional += qty * price
    if total_qty > 0:
        return total_notional / total_qty
    return fallback_price or 0.0


def _first_nonempty(values: List[Any]) -> str:
    for value in values:
        text = str(value or '').strip()
        if text:
            return text
    return ''


def _to_float(value: Any) -> float:
    if value in (None, ''):
        return 0.0
    try:
        return float(value)
    except Exception:
        try:
            return float(Decimal(str(value).strip()))
        except Exception:
            return 0.0


def _to_int(value: Any) -> int:
    if value in (None, ''):
        return 0
    try:
        return int(value)
    except Exception:
        return 0
