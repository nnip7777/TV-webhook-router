#!/usr/bin/env python3
"""Tests for the periodic BingX risk sync + settings save journal details.

Run with the service interpreter:
    .venv/bin/python tests/test_risk_sync.py
"""
from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'app'))

import execution  # noqa: E402
from execution import (  # noqa: E402
    RISK_SYNC_GROWTH_RATIO,
    bingx_risk_pct_by_symbol,
    risk_sync_default_pct,
    risk_sync_should_adjust,
    risk_sync_target_isolated,
)


class RiskSyncTriggerTest(unittest.TestCase):
    def test_growth_ratio_constant(self):
        self.assertEqual(RISK_SYNC_GROWTH_RATIO, 1.30)

    def test_adjust_at_exact_threshold(self):
        # 12% target → trigger at 15.6% (exactly +30%)
        self.assertTrue(risk_sync_should_adjust(15.6, 12.0))

    def test_no_adjust_below_threshold(self):
        self.assertFalse(risk_sync_should_adjust(15.59, 12.0))

    def test_no_adjust_when_below_target(self):
        self.assertFalse(risk_sync_should_adjust(5.0, 12.0))

    def test_no_adjust_without_target(self):
        self.assertFalse(risk_sync_should_adjust(50.0, 0.0))
        self.assertFalse(risk_sync_should_adjust(50.0, -1.0))

    def test_no_adjust_without_current_reading(self):
        self.assertFalse(risk_sync_should_adjust(0.0, 12.0))


class RiskSyncTargetIsolatedTest(unittest.TestCase):
    def test_target_from_current_risk_and_isolated(self):
        # maintenance = 0.20 × 10 = 2 → target = 2 / 0.12 = 16.666...
        target = risk_sync_target_isolated(0.20, 10.0, 1000.0, 12.0)
        self.assertAlmostEqual(target, 2.0 / 0.12, places=8)

    def test_fallback_maintenance_from_notional(self):
        # riskRate/isolated unavailable → maintenance = 0.3% × notional
        target = risk_sync_target_isolated(0.0, 0.0, 1000.0, 12.0)
        self.assertAlmostEqual(target, 3.0 / 0.12, places=8)

    def test_zero_target_returns_zero(self):
        self.assertEqual(risk_sync_target_isolated(0.20, 10.0, 1000.0, 0.0), 0.0)

    def test_no_maintenance_returns_zero(self):
        self.assertEqual(risk_sync_target_isolated(0.0, 0.0, 0.0, 12.0), 0.0)

    def test_adding_margin_reduces_risk_to_target(self):
        target = risk_sync_target_isolated(0.30, 8.0, 1000.0, 12.0)
        risk_after = (0.30 * 8.0) / target
        self.assertAlmostEqual(risk_after, 0.12, places=8)


class BingXRiskTargetsTest(unittest.TestCase):
    def test_collects_bingx_cells_only(self):
        config = {
            'routes': [
                {
                    'destinations': [
                        {'broker': 'bingx', 'symbol': 'NCCO7241NATGAS2USD-USDT', 'riskPct': 12.0},
                        {'broker': 'alor', 'symbol': 'SOMETHING', 'riskPct': 50.0},
                    ],
                },
                {
                    'destinations': [
                        {'broker': 'BingX', 'symbol': 'BTC-USDT', 'riskPct': '8.5%'},
                        {'broker': 'bingx', 'symbol': '', 'riskPct': 5.0},
                        {'broker': 'bingx', 'symbol': 'NO-PCT'},
                    ],
                },
            ],
        }
        targets = bingx_risk_pct_by_symbol(config)
        self.assertEqual(targets, {
            'NCCO7241NATGAS2USDUSDT': 12.0,
            'BTCUSDT': 8.5,
        })

    def test_empty_config(self):
        self.assertEqual(bingx_risk_pct_by_symbol({}), {})
        self.assertEqual(bingx_risk_pct_by_symbol({'routes': None}), {})

    def test_non_positive_pct_ignored(self):
        config = {'routes': [{'destinations': [
            {'broker': 'bingx', 'symbol': 'A-USDT', 'riskPct': 0},
            {'broker': 'bingx', 'symbol': 'B-USDT', 'riskPct': -3},
        ]}]}
        self.assertEqual(bingx_risk_pct_by_symbol(config), {})


class RiskSyncDefaultPctTest(unittest.TestCase):
    def test_env_value(self):
        os.environ['RISK_SYNC_DEFAULT_PCT'] = '10.5'
        try:
            self.assertEqual(risk_sync_default_pct(), 10.5)
        finally:
            os.environ.pop('RISK_SYNC_DEFAULT_PCT', None)

    def test_missing_key_falls_back_to_field_default(self):
        os.environ.pop('RISK_SYNC_DEFAULT_PCT', None)
        self.assertEqual(risk_sync_default_pct(), 12.0)

    def test_zero_disables_positions_without_cell(self):
        os.environ['RISK_SYNC_DEFAULT_PCT'] = '0'
        try:
            self.assertEqual(risk_sync_default_pct(), 0.0)
        finally:
            os.environ.pop('RISK_SYNC_DEFAULT_PCT', None)

    def test_invalid_value_falls_back_to_default(self):
        os.environ['RISK_SYNC_DEFAULT_PCT'] = 'abc'
        try:
            self.assertEqual(risk_sync_default_pct(), 12.0)
        finally:
            os.environ.pop('RISK_SYNC_DEFAULT_PCT', None)


class SettingsFieldHelpersTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import server  # heavy import (adapters) — once per run
        cls.server = server

    def test_normalize_float(self):
        field = {'key': 'X', 'type': 'float', 'default': '12'}
        self.assertEqual(self.server._normalize_env_value(field, '12.5', '/root'), '12.5')
        self.assertEqual(self.server._normalize_env_value(field, '', '/root'), '12')
        self.assertEqual(self.server._normalize_env_value(field, 'abc', '/root'), '12')
        self.assertEqual(self.server._normalize_env_value(field, '0', '/root'), '0.0')

    def test_normalize_bool(self):
        field = {'key': 'X', 'type': 'bool', 'default': 'true'}
        self.assertEqual(self.server._normalize_env_value(field, 'true', '/root'), 'true')
        self.assertEqual(self.server._normalize_env_value(field, 'on', '/root'), 'true')
        self.assertEqual(self.server._normalize_env_value(field, 'false', '/root'), 'false')

    def test_bool_renders_as_checkbox(self):
        field = {'key': 'RISK_SYNC_ENABLED', 'label': 'risk', 'type': 'bool', 'default': 'true'}
        checked = self.server._render_env_input(field, {'RISK_SYNC_ENABLED': 'true'}, '/root')
        unchecked = self.server._render_env_input(field, {'RISK_SYNC_ENABLED': 'false'}, '/root')
        self.assertIn("type='checkbox'", checked)
        self.assertIn(' checked', checked)
        self.assertIn("type='checkbox'", unchecked)
        self.assertNotIn(' checked', unchecked)

    def test_journal_mask_secrets(self):
        self.assertEqual(self.server._settings_journal_mask({'key': 'BINGX_SECRET_KEY', 'type': 'text'}, 'abc'), '***')
        self.assertEqual(self.server._settings_journal_mask({'key': 'TELEGRAM_BOT_TOKEN', 'type': 'text'}, '123'), '***')
        self.assertEqual(self.server._settings_journal_mask({'key': 'WEBHOOK_ROUTER_ADMIN_PASSWORD', 'type': 'password'}, 'pw'), '***')
        self.assertEqual(self.server._settings_journal_mask({'key': 'RISK_SYNC_INTERVAL_SECONDS', 'type': 'int'}, '90'), '90')
        self.assertEqual(self.server._settings_journal_mask({'key': 'X', 'type': 'text'}, ''), '')


class FakeBingXClient:
    def __init__(self, rows, balance_available='100'):
        self.rows = rows
        self.balance_available = balance_available
        self.adjust_calls = []

    def get_positions(self, symbol=None):
        rows = self.rows
        if symbol:
            norm = str(symbol).replace('-', '').upper()
            rows = [r for r in rows if str(r.get('symbol', '')).replace('-', '').upper() == norm]
        return {'data': rows}

    def get_balance(self):
        return {'data': [{'asset': 'USDT', 'equity': '500', 'availableMargin': self.balance_available}]}

    def adjust_isolated_margin(self, symbol, position_side, amount, direction_type=1):
        self.adjust_calls.append((symbol, position_side, float(amount), direction_type))
        norm = str(symbol).replace('-', '').upper()
        for row in self.rows:
            row_norm = str(row.get('symbol', '')).replace('-', '').upper()
            row_side = str(row.get('positionSide') or row.get('side') or 'BOTH').upper()
            if row_norm != norm or row_side != str(position_side).upper():
                continue
            margin = float(row.get('margin') or 0)
            risk_rate = float(row.get('riskRate') or 0)
            maintenance = risk_rate * margin
            new_margin = margin + amount if int(direction_type) == 1 else max(0.0, margin - amount)
            row['margin'] = str(new_margin)
            row['riskRate'] = str(maintenance / new_margin) if new_margin > 0 else '0'
        return {'code': 0, 'msg': 'ok'}


class BingXRiskSyncOnceTest(unittest.TestCase):
    def setUp(self):
        import server
        self.server = server
        self.journal = []
        self._orig_append = server.append_journal
        self._orig_broker = server.BingXBroker
        self._orig_load_config = server.load_config
        server.append_journal = lambda entry: self.journal.append(entry)
        server.load_config = lambda: {
            'routes': [{'destinations': [
                {'broker': 'bingx', 'symbol': 'BTC-USDT', 'riskPct': 12.0},
            ]}],
            'brokers': {'bingx': {'defaultDestination': {}}},
        }

    def tearDown(self):
        self.server.append_journal = self._orig_append
        self.server.BingXBroker = self._orig_broker
        self.server.load_config = self._orig_load_config

    def test_adds_margin_when_risk_grew_over_threshold(self):
        # 20% ≥ 12×1.3 → add margin: maint = 0.20×10 = 2 → target = 16.666…, delta ≈ 6.666
        client = FakeBingXClient([{
            'symbol': 'BTC-USDT', 'positionSide': 'BOTH', 'positionAmt': '1',
            'riskRate': '0.20', 'margin': '10', 'markPrice': '1000',
        }])
        self.server.BingXBroker = lambda testnet=False: client
        self.server._bingx_risk_sync_once()
        self.assertEqual(len(client.adjust_calls), 1)
        symbol, side, amount, direction = client.adjust_calls[0]
        self.assertEqual(symbol, 'BTC-USDT')
        self.assertEqual(side, 'BOTH')
        self.assertEqual(direction, 1)
        self.assertAlmostEqual(amount, 2.0 / 0.12 - 10.0, places=6)
        self.assertEqual(len(self.journal), 1)
        entry = self.journal[0]
        self.assertEqual(entry['kind'], 'risk-sync')
        self.assertEqual(entry['status'], 'ok')
        self.assertEqual(entry['ticker'], 'BTC-USDT')
        self.assertIn('riskAfter=12%', entry['details'])

    def test_skips_when_risk_below_threshold(self):
        client = FakeBingXClient([{
            'symbol': 'BTC-USDT', 'positionSide': 'BOTH', 'positionAmt': '1',
            'riskRate': '0.15', 'margin': '10', 'markPrice': '1000',
        }])
        self.server.BingXBroker = lambda testnet=False: client
        self.server._bingx_risk_sync_once()
        self.assertEqual(client.adjust_calls, [])
        self.assertEqual(self.journal, [])

    def test_skips_zero_qty_positions(self):
        client = FakeBingXClient([{
            'symbol': 'BTC-USDT', 'positionSide': 'BOTH', 'positionAmt': '0',
            'riskRate': '0.50', 'margin': '1', 'markPrice': '1000',
        }])
        self.server.BingXBroker = lambda testnet=False: client
        self.server._bingx_risk_sync_once()
        self.assertEqual(client.adjust_calls, [])

    def test_dry_run_skips_api_calls(self):
        self.server.load_config = lambda: {
            'routes': [],
            'brokers': {'bingx': {'defaultDestination': {'dryRun': True}}},
        }
        client = FakeBingXClient([{
            'symbol': 'BTC-USDT', 'positionSide': 'BOTH', 'positionAmt': '1',
            'riskRate': '0.90', 'margin': '1', 'markPrice': '1000',
        }])
        self.server.BingXBroker = lambda testnet=False: client
        self.server._bingx_risk_sync_once()
        self.assertEqual(client.adjust_calls, [])

    def test_insufficient_balance_journals_error(self):
        client = FakeBingXClient([{
            'symbol': 'BTC-USDT', 'positionSide': 'BOTH', 'positionAmt': '1',
            'riskRate': '0.20', 'margin': '10', 'markPrice': '1000',
        }], balance_available='0')
        self.server.BingXBroker = lambda testnet=False: client
        self.server._bingx_risk_sync_once()
        self.assertEqual(client.adjust_calls, [])
        self.assertEqual(len(self.journal), 1)
        self.assertEqual(self.journal[0]['status'], 'error')
        self.assertIn('insufficient_available_balance', self.journal[0]['details'])


class BingXPreparedNotionalTest(unittest.TestCase):
    """USDT opens must not mark×qty (gas flip 2026-10-18: 500 USDT → fake 1775)."""

    def test_quote_order_qty_is_already_usdt(self):
        from execution import _bingx_prepared_notional, _bingx_target_isolated_for_notional
        prepared = {'quoteOrderQty': '500', 'price': '3.391', 'qtyKind': 'usdt'}
        notional = _bingx_prepared_notional(prepared)
        self.assertAlmostEqual(notional, 500.0, places=8)
        # riskPct=15 → maintenance 0.3%×500 / 0.15 = 10 USDT (was 35.51 with bug)
        target = _bingx_target_isolated_for_notional(notional, 15.0)
        self.assertAlmostEqual(target, 10.0, places=8)

    def test_contracts_qty_uses_price(self):
        from execution import _bingx_prepared_notional
        prepared = {'quantity': '10', 'price': '3.391', 'qtyKind': 'contracts'}
        self.assertAlmostEqual(_bingx_prepared_notional(prepared), 33.91, places=6)

    def test_regression_old_mixed_units_formula(self):
        # old: mark × (short_contracts + usdt_qty) = 3.391 × 523.598 = 1775.5 → target 35.51
        from execution import _bingx_target_isolated_for_notional
        buggy = 3.391 * (23.598 + 500)
        self.assertAlmostEqual(_bingx_target_isolated_for_notional(buggy, 15.0), 35.51041636, places=6)


if __name__ == '__main__':
    unittest.main()
