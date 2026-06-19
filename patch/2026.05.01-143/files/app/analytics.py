#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Tuple

from settings import ANALYTICS_DB_PATH

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

CREATE INDEX IF NOT EXISTS idx_executions_signal_id ON executions(signal_id);
CREATE INDEX IF NOT EXISTS idx_orders_execution_id ON orders(execution_id);
CREATE INDEX IF NOT EXISTS idx_orders_broker_order_id ON orders(broker_order_id);
CREATE INDEX IF NOT EXISTS idx_fills_execution_id ON fills(execution_id);
CREATE INDEX IF NOT EXISTS idx_fills_order_local_id ON fills(order_local_id);
CREATE INDEX IF NOT EXISTS idx_fills_symbol_time ON fills(broker, symbol, venue, observed_at);
CREATE INDEX IF NOT EXISTS idx_open_lots_lookup ON open_lots(broker, symbol, venue, opened_at);
CREATE INDEX IF NOT EXISTS idx_round_trips_symbol_time ON round_trips(broker, symbol, venue, closed_at);
CREATE INDEX IF NOT EXISTS idx_round_trip_fills_round_trip_id ON round_trip_fills(round_trip_id);
"""


def init_analytics_db() -> Dict[str, Any]:
    path = Path(ANALYTICS_DB_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    with _DB_LOCK:
        with _connect() as conn:
            conn.executescript(SCHEMA_SQL)
            _ensure_counter_keys(conn)
    return {'ok': True, 'path': str(path)}


def _fill_phase_label(phase: str) -> str:
    value = str(phase or '').strip().lower()
    if value == 'target-open':
        return 'open'
    if value.startswith('target-close'):
        return 'close'
    return value or 'primary'


def _fill_sizing_basis(phase: str, request_json: str) -> str:
    phase_label = _fill_phase_label(phase)
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


def analytics_overview(limit: int = 20) -> Dict[str, Any]:
    path = Path(ANALYTICS_DB_PATH)
    with _DB_LOCK:
        with _connect() as conn:
            _ensure_counter_keys(conn)
            counters = {
                str(row['counter_key']): int(row['counter_value'] or 0)
                for row in conn.execute('SELECT counter_key, counter_value FROM analytics_counters').fetchall()
            }
            latest_fills = [dict(row) for row in conn.execute(
                '''
                SELECT
                    f.fill_id, f.observed_at, f.broker, f.symbol, f.venue, f.side, f.qty, f.price, f.notional,
                    f.commission, f.commission_currency, f.order_local_id, f.execution_id, f.signal_id,
                    f.phase, f.source_type, e.signal_mode, e.requested_qty_text, e.request_json
                FROM fills f
                LEFT JOIN executions e ON e.execution_id = f.execution_id
                ORDER BY f.observed_at DESC, f.fill_id DESC
                LIMIT ?
                ''',
                (max(1, min(int(limit), 100)),),
            ).fetchall()]
            for row in latest_fills:
                row['phaseLabel'] = _fill_phase_label(str(row.get('phase') or ''))
                row['sizingBasis'] = _fill_sizing_basis(str(row.get('phase') or ''), str(row.get('request_json') or ''))
            latest_round_trips = [dict(row) for row in conn.execute(
                '''
                SELECT round_trip_id, closed_at, broker, symbol, venue, direction, entry_qty, entry_avg_price, exit_avg_price, gross_pnl, commission_total, net_pnl, opening_signal_id, closing_signal_id
                FROM round_trips
                ORDER BY closed_at DESC, round_trip_id DESC
                LIMIT ?
                ''',
                (max(1, min(int(limit), 100)),),
            ).fetchall()]
            latest_daily_stats = [dict(row) for row in conn.execute(
                '''
                SELECT trade_day, broker, symbol, venue, lot_bucket, trades_count, gross_pnl_sum, commission_sum, net_pnl_sum, entry_qty_sum
                FROM daily_trade_stats
                ORDER BY trade_day DESC, broker, symbol, venue, lot_bucket
                LIMIT ?
                ''',
                (max(1, min(int(limit), 100)),),
            ).fetchall()]
            latest_signal = conn.execute('SELECT signal_id, received_at, origin, source_ticker, side, qty_text FROM signals ORDER BY received_at DESC, signal_id DESC LIMIT 1').fetchone()
            latest_execution = conn.execute('SELECT execution_id, received_at, broker, symbol, venue, status, error_text FROM executions ORDER BY received_at DESC, execution_id DESC LIMIT 1').fetchone()
    db_size_bytes = path.stat().st_size if path.exists() else 0
    return {
        'dbPath': str(path),
        'dbExists': path.exists(),
        'dbSizeBytes': db_size_bytes,
        'counters': counters,
        'latestSignal': dict(latest_signal) if latest_signal else {},
        'latestExecution': dict(latest_execution) if latest_execution else {},
        'latestFills': latest_fills,
        'latestRoundTrips': latest_round_trips,
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


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(str(ANALYTICS_DB_PATH), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')
    conn.execute('PRAGMA foreign_keys=ON')
    conn.execute('PRAGMA temp_store=MEMORY')
    return conn


def _ensure_counter_keys(conn: sqlite3.Connection) -> None:
    for key in ('signals', 'executions', 'orders', 'fills', 'round_trips'):
        conn.execute('INSERT OR IGNORE INTO analytics_counters(counter_key, counter_value) VALUES(?, 0)', (key,))


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
                    'position_effect': _infer_position_effect(phase, side),
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


def _apply_fill_to_positions(conn: sqlite3.Connection, fill: Dict[str, Any]) -> None:
    fill_side = str(fill.get('side') or '').lower()
    if fill_side not in ('buy', 'sell'):
        return
    broker = str(fill.get('broker') or '')
    symbol = str(fill.get('symbol') or '')
    venue = str(fill.get('venue') or '')
    qty_remaining = Decimal(str(fill.get('qty') or 0))
    if qty_remaining <= 0:
        return
    price = Decimal(str(fill.get('price') or 0))
    fill_commission_remaining = Decimal(str(fill.get('commission') or 0))
    opposite_side = 'sell' if fill_side == 'buy' else 'buy'

    rows = conn.execute(
        '''
        SELECT * FROM open_lots
        WHERE broker=? AND symbol=? AND venue=? AND side=? AND remaining_qty > 0
        ORDER BY opened_at, lot_id
        ''',
        (broker, symbol, venue, opposite_side),
    ).fetchall()

    for row in rows:
        if qty_remaining <= 0:
            break
        lot_qty_before = Decimal(str(row['remaining_qty'] or 0))
        if lot_qty_before <= 0:
            continue
        matched_qty = min(qty_remaining, lot_qty_before)
        lot_commission_before = Decimal(str(row['remaining_commission'] or 0))
        entry_commission_alloc = Decimal('0')
        if lot_qty_before > 0 and lot_commission_before > 0:
            entry_commission_alloc = (lot_commission_before * matched_qty / lot_qty_before)
        exit_commission_alloc = Decimal('0')
        if qty_remaining > 0 and fill_commission_remaining > 0:
            exit_commission_alloc = (fill_commission_remaining * matched_qty / qty_remaining)

        open_price = Decimal(str(row['open_price'] or 0))
        if row['side'] == 'buy':
            gross_pnl = (price - open_price) * matched_qty
            direction = 'long'
        else:
            gross_pnl = (open_price - price) * matched_qty
            direction = 'short'
        commission_total = entry_commission_alloc + exit_commission_alloc
        net_pnl = gross_pnl - commission_total
        round_trip_id = _round_trip_id(row['lot_id'], fill['fill_id'], matched_qty)
        holding_time_sec = _holding_seconds(str(row['opened_at'] or ''), str(fill.get('observed_at') or ''))

        round_trip_cursor = conn.execute(
            '''
            INSERT OR IGNORE INTO round_trips(
                round_trip_id, broker, symbol, venue, direction, opened_at, closed_at, holding_time_sec,
                entry_qty, exit_qty, entry_avg_price, exit_avg_price, gross_pnl, entry_commission, exit_commission,
                commission_total, net_pnl, entry_fill_count, exit_fill_count, entry_order_count, exit_order_count,
                opening_fill_id, closing_fill_id, opening_order_local_id, closing_order_local_id,
                opening_signal_id, closing_signal_id, opening_execution_id, closing_execution_id
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 1, 1, 1, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                round_trip_id,
                broker,
                symbol,
                venue,
                direction,
                str(row['opened_at'] or ''),
                str(fill.get('observed_at') or ''),
                holding_time_sec,
                float(matched_qty),
                float(matched_qty),
                float(open_price),
                float(price),
                float(gross_pnl),
                float(entry_commission_alloc),
                float(exit_commission_alloc),
                float(commission_total),
                float(net_pnl),
                str(row['open_fill_id'] or ''),
                str(fill.get('fill_id') or ''),
                str(row['open_order_local_id'] or ''),
                str(fill.get('order_local_id') or ''),
                str(row['open_signal_id'] or ''),
                str(fill.get('signal_id') or ''),
                str(row['open_execution_id'] or ''),
                str(fill.get('execution_id') or ''),
            ),
        )
        if round_trip_cursor.rowcount:
            _counter_increment(conn, 'round_trips', 1)
        conn.execute(
            'INSERT OR IGNORE INTO round_trip_fills(link_id, round_trip_id, fill_id, leg, matched_qty, price, commission_alloc) VALUES(?, ?, ?, ?, ?, ?, ?)',
            (
                f'{round_trip_id}:entry', round_trip_id, str(row['open_fill_id'] or ''), 'entry', float(matched_qty), float(open_price), float(entry_commission_alloc),
            ),
        )
        conn.execute(
            'INSERT OR IGNORE INTO round_trip_fills(link_id, round_trip_id, fill_id, leg, matched_qty, price, commission_alloc) VALUES(?, ?, ?, ?, ?, ?, ?)',
            (
                f'{round_trip_id}:exit', round_trip_id, str(fill.get('fill_id') or ''), 'exit', float(matched_qty), float(price), float(exit_commission_alloc),
            ),
        )
        _update_daily_trade_stats(conn, str(fill.get('observed_at') or ''), broker, symbol, venue, matched_qty, gross_pnl, commission_total, net_pnl)

        new_lot_qty = lot_qty_before - matched_qty
        new_lot_commission = lot_commission_before - entry_commission_alloc
        if new_lot_qty <= 0:
            conn.execute('DELETE FROM open_lots WHERE lot_id=?', (str(row['lot_id']),))
        else:
            conn.execute(
                'UPDATE open_lots SET remaining_qty=?, remaining_commission=? WHERE lot_id=?',
                (float(new_lot_qty), float(new_lot_commission), str(row['lot_id'])),
            )

        qty_remaining -= matched_qty
        fill_commission_remaining -= exit_commission_alloc

    if qty_remaining > 0:
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
                str(fill.get('fill_id') or ''),
                str(fill.get('order_local_id') or ''),
                str(fill.get('execution_id') or ''),
                str(fill.get('signal_id') or ''),
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


def _holding_seconds(opened_at: str, closed_at: str) -> float | None:
    try:
        if not opened_at or not closed_at:
            return None
        open_norm = opened_at.replace('Z', '+00:00')
        close_norm = closed_at.replace('Z', '+00:00')
        from datetime import datetime
        return max(0.0, (datetime.fromisoformat(close_norm) - datetime.fromisoformat(open_norm)).total_seconds())
    except Exception:
        return None


def _infer_position_effect(phase: str, side: str) -> str:
    phase_text = str(phase or '').lower()
    if 'close' in phase_text:
        return 'close'
    if 'open' in phase_text:
        return 'open'
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
