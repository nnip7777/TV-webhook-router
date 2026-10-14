#!/usr/bin/env python3
"""Tests for the smart-inc mode (BingX only).

Run with the service interpreter:
    .venv/bin/python tests/test_smart_inc.py
"""
from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'app'))

import analytics  # noqa: E402
import execution  # noqa: E402
from execution import smart_inc_max_multiplier, smart_inc_multiplier, smart_inc_avg_tol, smart_inc_required_add_qty, smart_inc_open_qty, smart_inc_required_add_usdt, _bingx_order_status  # noqa: E402


class SmartIncAvgTolTest(unittest.TestCase):
    def test_empty_is_none(self):
        self.assertIsNone(smart_inc_avg_tol({}))
        self.assertIsNone(smart_inc_avg_tol({'smartIncTol': ''}))
        self.assertIsNone(smart_inc_avg_tol({'smartIncTol': None}))

    def test_positive_float(self):
        self.assertEqual(smart_inc_avg_tol({'smartIncTol': '0.01'}), 0.01)
        self.assertEqual(smart_inc_avg_tol({'smartIncTol': '10'}), 10.0)
        self.assertEqual(smart_inc_avg_tol({'smartIncTol': '10%'}), 10.0)

    def test_zero_or_invalid(self):
        self.assertIsNone(smart_inc_avg_tol({'smartIncTol': '0'}))
        self.assertIsNone(smart_inc_avg_tol({'smartIncTol': 'abc'}))


class SmartIncRequiredAddQtyTest(unittest.TestCase):
    """Gas example: P=3.528, tol=0.01 → LONG skip if A≤3.538; SHORT skip if A≥3.518."""

    def test_long_already_in_range_skips(self):
        # A=3.53, P=3.528, tol=0.01 → A ≤ P+tol → 0
        self.assertEqual(
            smart_inc_required_add_qty(10, 3.53, 3.528, 'long', 0.01), 0.0)

    def test_long_needs_add(self):
        # A=3.60, P=3.528, tol=0.01 → need = 10*(3.60-3.528-0.01)/0.01 = 62
        need = smart_inc_required_add_qty(10, 3.60, 3.528, 'long', 0.01)
        self.assertAlmostEqual(need, 62.0, places=6)

    def test_short_already_in_range_skips(self):
        # A=3.52, P=3.528, tol=0.01 → A ≥ P-tol → 0
        self.assertEqual(
            smart_inc_required_add_qty(10, 3.52, 3.528, 'short', 0.01), 0.0)

    def test_short_needs_add(self):
        # A=3.40, P=3.528, tol=0.01 → need = 10*(3.528-0.01-3.40)/0.01 = 118
        need = smart_inc_required_add_qty(10, 3.40, 3.528, 'short', 0.01)
        self.assertAlmostEqual(need, 118.0, places=6)

    def test_gold_tol_10(self):
        # XAU: P=3800, A=3850, tol=10 → need = Q*(3850-3800-10)/10 = 4*Q
        need = smart_inc_required_add_qty(10, 3850, 3800, 'long', 10.0)
        self.assertAlmostEqual(need, 40.0, places=6)

    def test_bad_inputs(self):
        self.assertIsNone(smart_inc_required_add_qty(0, 3.5, 3.528, 'long', 0.01))
        self.assertIsNone(smart_inc_required_add_qty(10, 0, 3.528, 'long', 0.01))
        self.assertIsNone(smart_inc_required_add_qty(10, 3.5, 0, 'long', 0.01))
        self.assertIsNone(smart_inc_required_add_qty(10, 3.5, 3.528, '', 0.01))
        self.assertIsNone(smart_inc_required_add_qty(10, 3.5, 3.528, 'long', None))


class SmartIncOpenQtyTest(unittest.TestCase):
    """need is always contracts; open/add order may be USDT notional."""

    def test_usdt_open_multiplies_by_mark(self):
        # 199.906 contracts @ 3.575 → ~714.66 USDT (bug was quoteOrderQty=199.9)
        self.assertAlmostEqual(smart_inc_open_qty(199.906, 3.575, 'usdt'), 199.906 * 3.575, places=6)

    def test_contracts_open_passthrough(self):
        self.assertEqual(smart_inc_open_qty(199.906, 3.575, 'contracts'), 199.906)

    def test_quote_aliases(self):
        self.assertAlmostEqual(smart_inc_open_qty(10, 2.0, 'quote'), 20.0)
        self.assertAlmostEqual(smart_inc_open_qty(10, 2.0, 'notional'), 20.0)

    def test_zero_mark_falls_back_to_contracts(self):
        self.assertEqual(smart_inc_open_qty(50, 0, 'usdt'), 50.0)

    def test_user_halving_example(self):
        # M=400 USDT, gap=0.04, tol=0.02 → need_contracts * mark = M*(gap/tol-1)=400
        # Q=110.2 contracts @3.63, A=P-0.04, P=3.63, tol=0.02
        Q, P, tol = 110.2, 3.63, 0.02
        A = P - 0.04
        need = smart_inc_required_add_qty(Q, A, P, 'short', tol)
        usdt = smart_inc_open_qty(need, P, 'usdt')
        self.assertAlmostEqual(usdt, 400.0, delta=5.0)


class SmartIncRequiredAddUsdtTest(unittest.TestCase):
    def test_gas_short_usdt_direct(self):
        # live case: Q=156.05, A=3.607, P=3.648, tol=0.016 → ~889.48 USDT
        usdt = smart_inc_required_add_usdt(156.05, 3.607, 3.648, 'short', 0.016)
        self.assertAlmostEqual(usdt, 889.485, places=3)

    def test_in_range_is_zero(self):
        self.assertEqual(smart_inc_required_add_usdt(100, 3.55, 3.56, 'short', 0.02), 0.0)

    def test_halving_from_notional(self):
        # Q*P=400, gap=0.04, tol=0.02 → N=400
        Q, P, tol = 400 / 3.63, 3.63, 0.02
        usdt = smart_inc_required_add_usdt(Q, P - 0.04, P, 'short', tol)
        self.assertAlmostEqual(usdt, 400.0, delta=1.0)

    def test_bad_inputs_none(self):
        self.assertIsNone(smart_inc_required_add_usdt(0, 3.5, 3.6, 'short', 0.01))
        self.assertIsNone(smart_inc_required_add_usdt(10, 3.5, 3.6, 'long', None))


class BingxOrderStatusTest(unittest.TestCase):
    def test_cancelled_british_normalizes(self):
        self.assertEqual(_bingx_order_status({'status': 'CANCELLED'}), 'CANCELED')
        self.assertEqual(_bingx_order_status({'status': 'canceled'}), 'CANCELED')
        self.assertEqual(_bingx_order_status({'status': 'NEW'}), 'NEW')
        self.assertEqual(_bingx_order_status({'status': 'FILLED'}), 'FILLED')


class SmartIncMultiplierTest(unittest.TestCase):
    def test_no_history_means_start(self):
        self.assertEqual(smart_inc_multiplier(None), 1)

    def test_same_hour_is_zero(self):
        self.assertEqual(smart_inc_multiplier(0), 0)
        self.assertEqual(smart_inc_multiplier(0.99), 0)

    def test_hours_map_to_multiplier(self):
        self.assertEqual(smart_inc_multiplier(1), 1)
        self.assertEqual(smart_inc_multiplier(5), 5)
        self.assertEqual(smart_inc_multiplier(35.9), 35)

    def test_restart_window_resets_to_one(self):
        self.assertEqual(smart_inc_multiplier(36), 1)
        self.assertEqual(smart_inc_multiplier(100), 1)

    def test_cap_from_settings(self):
        os.environ['SMART_INC_MAX_MULTIPLIER'] = '10'
        try:
            self.assertEqual(smart_inc_max_multiplier(), 10)
            self.assertEqual(smart_inc_multiplier(25), 10)
        finally:
            os.environ.pop('SMART_INC_MAX_MULTIPLIER', None)

    def test_cap_floor_and_invalid_fallback(self):
        os.environ['SMART_INC_MAX_MULTIPLIER'] = '0'
        try:
            self.assertEqual(smart_inc_max_multiplier(), 1)
        finally:
            os.environ.pop('SMART_INC_MAX_MULTIPLIER', None)
        os.environ['SMART_INC_MAX_MULTIPLIER'] = 'abc'
        try:
            self.assertEqual(smart_inc_max_multiplier(), 36)
        finally:
            os.environ.pop('SMART_INC_MAX_MULTIPLIER', None)


class HoursSinceLastFillTest(unittest.TestCase):
    """Bucketed clock-hour difference against a temp analytics DB."""

    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix='.sqlite3')
        os.close(fd)
        self._orig_path = analytics.ANALYTICS_DB_PATH
        analytics.ANALYTICS_DB_PATH = Path(self.db_path)
        conn = sqlite3.connect(self.db_path)
        conn.executescript(analytics.SCHEMA_SQL)
        conn.commit()
        conn.close()

    def tearDown(self):
        analytics.ANALYTICS_DB_PATH = self._orig_path
        os.unlink(self.db_path)

    def _insert_fill(self, observed_at: str, fill_id: str = 'f1'):
        conn = sqlite3.connect(self.db_path)
        conn.execute(
            'INSERT INTO fills(fill_id, signal_id, execution_id, phase, observed_at, broker, symbol, venue, side, qty, price) '
            "VALUES (?, 'sig1', 'exec1', 'primary', ?, 'bingx', 'TEST-USDT', 'swap', 'buy', 1, 1)",
            (fill_id, observed_at),
        )
        conn.commit()
        conn.close()

    def test_no_history_returns_none(self):
        self.assertIsNone(analytics.hours_since_last_fill('bingx', 'TEST-USDT'))

    def test_missing_args_return_none(self):
        self.assertIsNone(analytics.hours_since_last_fill('', 'TEST-USDT'))
        self.assertIsNone(analytics.hours_since_last_fill('bingx', ''))

    def test_fill_in_current_hour_is_zero(self):
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        same_hour = now.replace(minute=0, second=0, microsecond=0)  # hour bucket start
        self._insert_fill(same_hour.isoformat())
        self.assertEqual(analytics.hours_since_last_fill('bingx', 'TEST-USDT'), 0.0)

    def test_mixed_timezone_offsets_use_max_epoch(self):
        # Two fills with different tz offsets: the later one must win,
        # even though the raw strings do not sort lexicographically.
        self._insert_fill('2026-10-06T22:00:37.000+08:00', fill_id='f1')  # = 14:00:37Z
        self._insert_fill('2026-10-06T16:26:26+03:00', fill_id='f2')       # = 13:26:26Z
        from datetime import datetime, timedelta, timezone
        now = datetime.now(timezone.utc)
        expected = float(max(0, int(now.timestamp() // 3600) - int(now.timestamp() // 3600)))
        # Both fills are far in the past relative to now, so the value must be
        # >= 0 and derived from the later fill (22:00+08:00). Recompute exactly:
        later = datetime.fromisoformat('2026-10-06T22:00:37.000+08:00').timestamp()
        want = float(max(0, int(now.timestamp() // 3600) - int(later // 3600)))
        got = analytics.hours_since_last_fill('bingx', 'TEST-USDT')
        self.assertEqual(got, want)
        self.assertGreaterEqual(got, expected)


class FakeBingXBroker:
    """Minimal fake: dry-run path only needs prepare + position mode."""

    last_prepared_qty = None
    last_qty_kind = None

    def __init__(self, *args, **kwargs):
        pass

    def prepare_limit_order(self, symbol, side, qty, price=None, qty_kind='contracts'):
        FakeBingXBroker.last_prepared_qty = qty
        FakeBingXBroker.last_qty_kind = qty_kind
        return {
            'price': price or 100,
            'symbol': symbol,
            'side': side,
            'quantity': qty,
            'quoteOrderQty': qty,
            'contract': {
                'symbol': symbol,
                'pricePrecision': 2,
                'quantityPrecision': 3,
                'asset': 'USDT',
                'displayName': 'TEST',
            },
            'bookTicker': {},
            'depthPlan': {},
        }

    def get_position_mode(self):
        return {'data': {'dualSidePosition': 'true'}}


class ExecuteBingXSmartIncTest(unittest.TestCase):
    """_execute_bingx sizing: multiplier applies, same-hour skips."""

    def setUp(self):
        self._orig_broker = execution.BingXBroker
        self._orig_hours = execution.hours_since_last_fill
        execution.BingXBroker = FakeBingXBroker
        FakeBingXBroker.last_prepared_qty = None

    def tearDown(self):
        execution.BingXBroker = self._orig_broker
        execution.hours_since_last_fill = self._orig_hours
        os.environ.pop('SMART_INC_MAX_MULTIPLIER', None)

    def _destination(self, **extra):
        dest = {
            'broker': 'bingx',
            'symbol': 'TEST-USDT',
            'signalMode': 'target-direction',
            'qtyMode': 'fixed',
            'qty': 75,
            'qtyKind': 'usdt',
            'openQtyKind': 'usdt',
            'smartInc': True,
            'executionMode': 'maker',
            'dryRun': True,
        }
        dest.update(extra)
        return dest

    def _payload(self, side='2short'):
        return {'sourceTicker': 'SRC', 'side': side, 'qty': 1}

    def test_multiplier_scales_quantity(self):
        execution.hours_since_last_fill = lambda b, s: 5.0
        import asyncio
        result = asyncio.run(execution._execute_bingx(self._payload(), self._destination()))
        self.assertNotIn('skipped', result)
        self.assertEqual(result['request']['smartInc']['multiplier'], 5)
        self.assertEqual(result['request']['smartInc']['baseQty'], 75)
        self.assertEqual(FakeBingXBroker.last_prepared_qty, 375)
        self.assertEqual(FakeBingXBroker.last_qty_kind, 'usdt')

    def test_same_hour_skips_without_touching_exchange(self):
        execution.hours_since_last_fill = lambda b, s: 0.0

        class ExplodingBroker:
            def __init__(self, *a, **k):
                raise AssertionError('exchange client must not be created on skip')

        execution.BingXBroker = ExplodingBroker
        import asyncio
        result = asyncio.run(execution._execute_bingx(self._payload(), self._destination()))
        self.assertTrue(result.get('skipped'))
        self.assertEqual(result['results']['reason'], 'smart_inc_same_hour')
        self.assertEqual(result['request']['smartInc']['multiplier'], 0)
        self.assertEqual(result['request']['stage'], 'smart_inc_skip')

    def test_no_history_uses_base_qty(self):
        execution.hours_since_last_fill = lambda b, s: None
        import asyncio
        result = asyncio.run(execution._execute_bingx(self._payload(), self._destination()))
        self.assertNotIn('skipped', result)
        self.assertEqual(result['request']['smartInc']['multiplier'], 1)
        self.assertEqual(FakeBingXBroker.last_prepared_qty, 75)

    def test_restart_window_uses_base_qty(self):
        execution.hours_since_last_fill = lambda b, s: 40.0
        import asyncio
        result = asyncio.run(execution._execute_bingx(self._payload(), self._destination()))
        self.assertEqual(result['request']['smartInc']['multiplier'], 1)

    def test_cap_from_settings_applied(self):
        os.environ['SMART_INC_MAX_MULTIPLIER'] = '3'
        execution.hours_since_last_fill = lambda b, s: 20.0
        import asyncio
        result = asyncio.run(execution._execute_bingx(self._payload(), self._destination()))
        self.assertEqual(result['request']['smartInc']['multiplier'], 3)
        self.assertEqual(FakeBingXBroker.last_prepared_qty, 225)

    def test_flag_off_leaves_quantity_untouched(self):
        execution.hours_since_last_fill = lambda b, s: 10.0
        import asyncio
        result = asyncio.run(execution._execute_bingx(self._payload(), self._destination(smartInc=False)))
        self.assertNotIn('smartInc', result['request'])
        self.assertEqual(FakeBingXBroker.last_prepared_qty, 75)

    def test_step_side_not_multiplied(self):
        # step-side (buy/sell) ignores smart-inc entirely
        execution.hours_since_last_fill = lambda b, s: 10.0
        import asyncio
        dest = self._destination(signalMode='step-side', qtyKind='contracts', openQtyKind='contracts')
        dest.pop('smartInc')
        dest['smartInc'] = True
        result = asyncio.run(execution._execute_bingx(self._payload(side='sell'), dest))
        self.assertNotIn('smartInc', result['request'])
        self.assertEqual(FakeBingXBroker.last_prepared_qty, 75)


if __name__ == '__main__':
    unittest.main(verbosity=2)
