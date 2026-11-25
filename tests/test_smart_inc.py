#!/usr/bin/env python3
"""Tests for the smart-inc mode (BingX + Bybit sizing helpers).

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
from execution import (
    smart_inc_max_multiplier,
    smart_inc_multiplier,
    smart_inc_avg_tol,
    smart_inc_t_step,
    smart_inc_wind_tal,
    smart_inc_wind_multiplier,
    smart_inc_hook_mult,
    smart_inc_effective_tal,
    smart_inc_effective_window,
    smart_inc_avg_tol_allowed,
    smart_inc_required_add_qty,
    smart_inc_open_qty,
    smart_inc_required_add_usdt,
    smart_inc_tf_minutes,
    _bingx_order_status,
)  # noqa: E402


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


class SmartIncTStepTest(unittest.TestCase):
    """Cell tStep is the base for hook tal/w_tal multipliers."""

    def test_tstep_preferred(self):
        self.assertEqual(smart_inc_t_step({'tStep': '0.002', 'smartIncTol': '9'}), 0.002)

    def test_legacy_smartIncTol_fallback(self):
        self.assertEqual(smart_inc_t_step({'smartIncTol': '0.003'}), 0.003)

    def test_empty_none(self):
        self.assertIsNone(smart_inc_t_step({}))
        self.assertIsNone(smart_inc_t_step({'tStep': '0', 'smartIncTol': ''}))


class SmartIncWindTalTest(unittest.TestCase):
    """Cell w_tal / Wind_tal = informational last webhook multiplier."""

    def test_empty_is_none(self):
        self.assertIsNone(smart_inc_wind_tal({}))
        self.assertIsNone(smart_inc_wind_tal({'w_tal': '', 'Wind_tal': ''}))
        self.assertIsNone(smart_inc_wind_tal({'Wind_tal': None}))

    def test_positive_float(self):
        self.assertEqual(smart_inc_wind_tal({'w_tal': '2.5'}), 2.5)
        self.assertEqual(smart_inc_wind_tal({'Wind_tal': '10'}), 10.0)
        self.assertEqual(smart_inc_wind_tal({'Wind_tal': '0.005%'}), 0.005)

    def test_zero_or_invalid(self):
        self.assertIsNone(smart_inc_wind_tal({'w_tal': '0'}))
        self.assertIsNone(smart_inc_wind_tal({'Wind_tal': 'abc'}))


class SmartIncWindMultiplierTest(unittest.TestCase):
    """Webhook w_tal / tal are multipliers from tStep."""

    def test_from_payload(self):
        self.assertEqual(smart_inc_wind_multiplier({'w_tal': 2.5}), 2.5)
        self.assertEqual(smart_inc_hook_mult({'tal': 3}, 'tal'), 3.0)
        self.assertEqual(smart_inc_hook_mult({'w_tal': '2.5%'}, 'w_tal'), 2.5)

    def test_missing_or_invalid(self):
        self.assertIsNone(smart_inc_wind_multiplier({}))
        self.assertIsNone(smart_inc_wind_multiplier({'w_tal': None}))
        self.assertIsNone(smart_inc_wind_multiplier({'w_tal': '0'}))
        self.assertIsNone(smart_inc_wind_multiplier({'w_tal': 'abc'}))
        self.assertIsNone(smart_inc_wind_multiplier(None))
        self.assertIsNone(smart_inc_hook_mult({}, 'tal'))


class SmartIncEffectiveWindowTest(unittest.TestCase):
    """Scheme A: tal_eff = hook.tal × tStep; wind = hook.w_tal × tStep."""

    def test_effective_tal(self):
        # tStep=0.002, hook tal=3 → 0.006
        self.assertAlmostEqual(
            smart_inc_effective_tal(0.002, {'tal': 3}), 0.006, places=9)

    def test_effective_window_from_tstep(self):
        # tStep=0.002, hook w_tal=2.5 → 0.005 (each from base)
        self.assertAlmostEqual(
            smart_inc_effective_window(0.002, {'w_tal': 2.5}), 0.005, places=9)

    def test_no_tstep_or_no_mult_is_none(self):
        self.assertIsNone(smart_inc_effective_tal(None, {'tal': 2}))
        self.assertIsNone(smart_inc_effective_tal(0.002, {}))
        self.assertIsNone(smart_inc_effective_window(None, {'w_tal': 2.5}))
        self.assertIsNone(smart_inc_effective_window(0.002, {'w_tal': 0}))
        self.assertIsNone(smart_inc_effective_window(0.002, {}))
        self.assertIsNone(smart_inc_effective_window(0.002, None))
        # mult=1 is valid: equals tStep
        self.assertAlmostEqual(smart_inc_effective_window(0.002, {'w_tal': 1}), 0.002, places=9)
        self.assertAlmostEqual(smart_inc_effective_tal(0.002, {'tal': 1}), 0.002, places=9)


class SmartIncAvgTolAllowedTest(unittest.TestCase):
    """Gate: avg_tol only when |avg−mark| > effective window (or gate off)."""

    def test_gate_off_always_allowed(self):
        self.assertTrue(smart_inc_avg_tol_allowed(3.50, 3.53, None))
        self.assertTrue(smart_inc_avg_tol_allowed(3.50, 3.53, 0))

    def test_within_window_blocks(self):
        # window=0.005, gap=0.003 → blocked
        self.assertFalse(smart_inc_avg_tol_allowed(3.500, 3.503, 0.005))
        # boundary gap == wind → blocked (only strictly outside runs)
        self.assertFalse(smart_inc_avg_tol_allowed(3.500, 3.505, 0.005))

    def test_outside_window_allows(self):
        # window=0.005, gap=0.006 → allowed (tal still applies after)
        self.assertTrue(smart_inc_avg_tol_allowed(3.500, 3.506, 0.005))

    def test_abs_gap_both_sides(self):
        self.assertFalse(smart_inc_avg_tol_allowed(3.510, 3.508, 0.005))
        self.assertTrue(smart_inc_avg_tol_allowed(3.520, 3.508, 0.005))

    def test_bad_inputs_allow(self):
        # Fail-open: no avg/mark → don't block the rest of smart-inc
        self.assertTrue(smart_inc_avg_tol_allowed(0, 3.5, 0.005))
        self.assertTrue(smart_inc_avg_tol_allowed('x', 3.5, 0.005))


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

    def test_long_avg_below_mark_outside_tol_catches_up(self):
        # NG1 2026-11-03: A=2.871, P=2.885, tol=0.005, |gap|=0.014 > tol
        # Old one-sided formula returned 0 → hours ×22 = 1100 USDT blow-up.
        need = smart_inc_required_add_qty(17.403, 2.871, 2.885, 'long', 0.005)
        self.assertGreater(need, 0.0)

    def test_short_avg_above_mark_outside_tol_catches_up(self):
        need = smart_inc_required_add_qty(10, 3.60, 3.50, 'short', 0.01)
        self.assertGreater(need, 0.0)

    def test_abs_gap_boundary(self):
        # |A-P| == tol → still in range (float noise: use almostEqual)
        self.assertAlmostEqual(
            smart_inc_required_add_qty(10, 3.538, 3.528, 'long', 0.01), 0.0, places=9)
        self.assertAlmostEqual(
            smart_inc_required_add_qty(10, 3.518, 3.528, 'short', 0.01), 0.0, places=9)

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
    def setUp(self):
        self._saved = {}
        for k in (
            'BINGX_SMART_INC_MAX_MULTIPLIER', 'BYBIT_SMART_INC_MAX_MULTIPLIER',
            'BINGX_SMART_INC_RESTART_HOURS', 'BYBIT_SMART_INC_RESTART_HOURS',
            'SMART_INC_MAX_MULTIPLIER', 'SMART_INC_RESTART_HOURS',
        ):
            self._saved[k] = os.environ.pop(k, None)

    def tearDown(self):
        for k, v in self._saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def test_no_history_means_start(self):
        self.assertEqual(smart_inc_multiplier(None), 1)

    def test_same_bar_is_zero(self):
        self.assertEqual(smart_inc_multiplier(0), 0)
        self.assertEqual(smart_inc_multiplier(0.99), 0)

    def test_bars_map_to_multiplier(self):
        self.assertEqual(smart_inc_multiplier(1), 1)
        self.assertEqual(smart_inc_multiplier(5), 5)
        self.assertEqual(smart_inc_multiplier(35.9), 35)

    def test_restart_window_resets_to_one(self):
        # default restart bars = 36 when env cleared
        self.assertEqual(smart_inc_multiplier(36), 1)
        self.assertEqual(smart_inc_multiplier(100), 1)

    def test_cap_from_settings(self):
        os.environ['BINGX_SMART_INC_MAX_MULTIPLIER'] = '10'
        try:
            self.assertEqual(smart_inc_max_multiplier('bingx'), 10)
            self.assertEqual(smart_inc_multiplier(25, 'bingx'), 10)
        finally:
            os.environ.pop('BINGX_SMART_INC_MAX_MULTIPLIER', None)

    def test_cap_floor_and_invalid_fallback(self):
        os.environ['BINGX_SMART_INC_MAX_MULTIPLIER'] = '0'
        try:
            self.assertEqual(smart_inc_max_multiplier('bingx'), 1)
        finally:
            os.environ.pop('BINGX_SMART_INC_MAX_MULTIPLIER', None)
        os.environ['BINGX_SMART_INC_MAX_MULTIPLIER'] = 'abc'
        os.environ.pop('SMART_INC_MAX_MULTIPLIER', None)
        try:
            # invalid per-broker + no legacy → field default 36
            self.assertEqual(smart_inc_max_multiplier('bingx'), 36)
        finally:
            os.environ.pop('BINGX_SMART_INC_MAX_MULTIPLIER', None)

    def test_per_broker_keys_independent(self):
        os.environ['BINGX_SMART_INC_MAX_MULTIPLIER'] = '7'
        os.environ['BYBIT_SMART_INC_MAX_MULTIPLIER'] = '3'
        try:
            self.assertEqual(smart_inc_max_multiplier('bingx'), 7)
            self.assertEqual(smart_inc_max_multiplier('bybit'), 3)
        finally:
            os.environ.pop('BINGX_SMART_INC_MAX_MULTIPLIER', None)
            os.environ.pop('BYBIT_SMART_INC_MAX_MULTIPLIER', None)


class SmartIncTfMinutesTest(unittest.TestCase):
    def test_default_is_60(self):
        self.assertEqual(smart_inc_tf_minutes({}), 60)
        self.assertEqual(smart_inc_tf_minutes({'smartIncTf': ''}), 60)
        self.assertEqual(smart_inc_tf_minutes(None), 60)

    def test_valid_values(self):
        for v in (1, 5, 8, 10, 15, 20, 60, 280, 1440):
            self.assertEqual(smart_inc_tf_minutes({'smartIncTf': v}), v)
            self.assertEqual(smart_inc_tf_minutes({'smartIncTf': str(v)}), v)

    def test_invalid_falls_back(self):
        self.assertEqual(smart_inc_tf_minutes({'smartIncTf': 'abc'}), 60)
        self.assertEqual(smart_inc_tf_minutes({'smartIncTf': 0}), 60)


class BarsSinceLastFillTest(unittest.TestCase):
    """Bucketed TF-bar difference against a temp analytics DB."""

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
        self.assertIsNone(analytics.bars_since_last_fill('bingx', 'TEST-USDT', 60))

    def test_missing_args_return_none(self):
        self.assertIsNone(analytics.bars_since_last_fill('', 'TEST-USDT', 60))
        self.assertIsNone(analytics.bars_since_last_fill('bingx', '', 60))
        self.assertIsNone(analytics.bars_since_last_fill('bingx', 'TEST-USDT', 0))

    def test_fill_in_current_bar_is_zero(self):
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        # Align to 15m bucket start so now and fill share the same bar.
        step = 15 * 60
        bucket_start = now.replace(minute=(now.minute // 15) * 15, second=0, microsecond=0)
        self._insert_fill(bucket_start.isoformat())
        got = analytics.bars_since_last_fill('bingx', 'TEST-USDT', 15)
        self.assertEqual(got, 0.0)

    def test_two_bars_ago_is_two(self):
        from datetime import datetime, timedelta, timezone
        now = datetime.now(timezone.utc)
        step = timedelta(minutes=15)
        fill = now - (2 * step) + timedelta(seconds=1)  # still in the bar 2 steps back
        # Truncate fill to its 15m bucket start so bucket index difference is exactly 2
        fill_bucket = fill.replace(minute=(fill.minute // 15) * 15, second=0, microsecond=0)
        # If fill_bucket + 2*step is still <= now's bucket, difference is 2
        self._insert_fill(fill_bucket.isoformat())
        got = analytics.bars_since_last_fill('bingx', 'TEST-USDT', 15)
        want = float(max(0, int(now.timestamp() // (15 * 60)) - int(fill_bucket.timestamp() // (15 * 60))))
        self.assertEqual(got, want)


class HoursSinceLastFillTest(unittest.TestCase):
    """Legacy hour-bucket helper still works (default TF path / tests)."""

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

    def test_fill_in_current_hour_is_zero(self):
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        same_hour = now.replace(minute=0, second=0, microsecond=0)
        self._insert_fill(same_hour.isoformat())
        self.assertEqual(analytics.hours_since_last_fill('bingx', 'TEST-USDT'), 0.0)


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
    """_execute_bingx sizing: multiplier applies, same-bar skips."""

    def setUp(self):
        self._orig_broker = execution.BingXBroker
        self._orig_bars = execution.bars_since_last_fill
        execution.BingXBroker = FakeBingXBroker
        FakeBingXBroker.last_prepared_qty = None

    def tearDown(self):
        execution.BingXBroker = self._orig_broker
        execution.bars_since_last_fill = self._orig_bars
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
        execution.bars_since_last_fill = lambda b, s, tf: 5.0
        import asyncio
        result = asyncio.run(execution._execute_bingx(self._payload(), self._destination()))
        self.assertNotIn('skipped', result)
        self.assertEqual(result['request']['smartInc']['multiplier'], 5)
        self.assertEqual(result['request']['smartInc']['baseQty'], 75)
        self.assertEqual(result['request']['smartInc']['mode'], 'bars')
        self.assertEqual(result['request']['smartInc']['tf'], 60)
        self.assertEqual(FakeBingXBroker.last_prepared_qty, 375)
        self.assertEqual(FakeBingXBroker.last_qty_kind, 'usdt')

    def test_same_bar_skips_without_touching_exchange(self):
        execution.bars_since_last_fill = lambda b, s, tf: 0.0

        class ExplodingBroker:
            def __init__(self, *a, **k):
                raise AssertionError('exchange client must not be created on skip')

        execution.BingXBroker = ExplodingBroker
        import asyncio
        result = asyncio.run(execution._execute_bingx(self._payload(), self._destination()))
        self.assertTrue(result.get('skipped'))
        self.assertEqual(result['results']['reason'], 'smart_inc_same_bar')
        self.assertEqual(result['request']['smartInc']['multiplier'], 0)
        self.assertEqual(result['request']['stage'], 'smart_inc_skip')

    def test_no_history_uses_base_qty(self):
        execution.bars_since_last_fill = lambda b, s, tf: None
        import asyncio
        result = asyncio.run(execution._execute_bingx(self._payload(), self._destination()))
        self.assertNotIn('skipped', result)
        self.assertEqual(result['request']['smartInc']['multiplier'], 1)
        self.assertEqual(FakeBingXBroker.last_prepared_qty, 75)

    def test_restart_window_uses_base_qty(self):
        execution.bars_since_last_fill = lambda b, s, tf: 40.0
        import asyncio
        result = asyncio.run(execution._execute_bingx(self._payload(), self._destination()))
        self.assertEqual(result['request']['smartInc']['multiplier'], 1)

    def test_cap_from_settings_applied(self):
        os.environ['BINGX_SMART_INC_MAX_MULTIPLIER'] = '3'
        try:
            execution.bars_since_last_fill = lambda b, s, tf: 20.0
            import asyncio
            result = asyncio.run(execution._execute_bingx(self._payload(), self._destination()))
            self.assertEqual(result['request']['smartInc']['multiplier'], 3)
            self.assertEqual(FakeBingXBroker.last_prepared_qty, 225)
        finally:
            os.environ.pop('BINGX_SMART_INC_MAX_MULTIPLIER', None)

    def test_flag_off_leaves_quantity_untouched(self):
        execution.bars_since_last_fill = lambda b, s, tf: 10.0
        import asyncio
        result = asyncio.run(execution._execute_bingx(self._payload(), self._destination(smartInc=False)))
        self.assertNotIn('smartInc', result['request'])
        self.assertEqual(FakeBingXBroker.last_prepared_qty, 75)

    def test_step_side_not_multiplied(self):
        execution.bars_since_last_fill = lambda b, s, tf: 10.0
        import asyncio
        dest = self._destination(signalMode='step-side', qtyKind='contracts', openQtyKind='contracts')
        dest.pop('smartInc')
        dest['smartInc'] = True
        result = asyncio.run(execution._execute_bingx(self._payload(side='sell'), dest))
        self.assertNotIn('smartInc', result['request'])
        self.assertEqual(FakeBingXBroker.last_prepared_qty, 75)

    def test_cell_tf_passed_to_bars_helper(self):
        seen = {}
        def _bars(b, s, tf):
            seen['tf'] = tf
            return 2.0
        execution.bars_since_last_fill = _bars
        import asyncio
        result = asyncio.run(execution._execute_bingx(
            self._payload(), self._destination(smartIncTf=15)))
        self.assertEqual(seen.get('tf'), 15)
        self.assertEqual(result['request']['smartInc']['tf'], 15)
        self.assertEqual(result['request']['smartInc']['multiplier'], 2)
        self.assertEqual(FakeBingXBroker.last_prepared_qty, 150)


if __name__ == '__main__':
    unittest.main(verbosity=2)
