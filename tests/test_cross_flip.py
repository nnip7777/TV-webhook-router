#!/usr/bin/env python3
"""Tests for Bybit/BingX cross-margin flip decision helpers."""
from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'app'))

from execution import (  # noqa: E402
    _bingx_position_upnl,
    _parse_taker_rate_pct,
    bingx_cross_symbols,
    fetch_taker_fee_rate_pct,
    flip_close_net_pnl,
    should_keep_opposite_on_cross,
)


class ShouldKeepOppositeOnCrossTest(unittest.TestCase):
    def test_net_loss_keeps(self):
        self.assertTrue(should_keep_opposite_on_cross(-1.5))
        self.assertTrue(should_keep_opposite_on_cross(-0.01))

    def test_net_profit_closes(self):
        self.assertFalse(should_keep_opposite_on_cross(1.2))
        self.assertFalse(should_keep_opposite_on_cross(0.0))

    def test_unknown_fails_toward_close(self):
        self.assertFalse(should_keep_opposite_on_cross(None))
        self.assertFalse(should_keep_opposite_on_cross('abc'))
        self.assertFalse(should_keep_opposite_on_cross(''))


class ParseTakerRateTest(unittest.TestCase):
    def test_bingx_data_fields(self):
        payload = {'data': {'takerCommissionRate': '0.0005', 'makerCommissionRate': '0.0002'}}
        self.assertAlmostEqual(_parse_taker_rate_pct('bingx', payload), 0.05, places=9)

    def test_bybit_list_symbol_match(self):
        payload = {'result': {'list': [
            {'symbol': 'XAUTUSDT', 'takerFeeRate': '0.001', 'makerFeeRate': '0.00036'},
        ]}}
        self.assertAlmostEqual(_parse_taker_rate_pct('bybit', payload, 'XAUTUSDT'), 0.1, places=9)
        self.assertIsNone(_parse_taker_rate_pct('bybit', payload, 'BTCUSDT'))

    def test_bad_payload_none(self):
        self.assertIsNone(_parse_taker_rate_pct('bingx', None))
        self.assertIsNone(_parse_taker_rate_pct('bybit', {}))


class FlipCloseNetPnlTest(unittest.TestCase):
    """net = upnl − entry fees already paid − notional × taker fee rate%."""

    def setUp(self):
        self._saved = {}
        for k in ('BINGX_FLIP_FEE_RATE_PCT', 'BYBIT_FLIP_FEE_RATE_PCT', 'FLIP_FEE_RATE_PCT'):
            self._saved[k] = os.environ.pop(k, None)
        from execution import _FEE_RATE_CACHE
        self._cache_backup = dict(_FEE_RATE_CACHE)
        _FEE_RATE_CACHE.clear()

    def tearDown(self):
        for k, v in self._saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        from execution import _FEE_RATE_CACHE
        _FEE_RATE_CACHE.clear()
        _FEE_RATE_CACHE.update(self._cache_backup)

    def test_settings_fallback_when_exchange_unavailable(self):
        os.environ['BINGX_FLIP_FEE_RATE_PCT'] = '0.1'
        # client without get_commission_rate → force fail path via bad client
        class BadClient:
            def get_commission_rate(self):
                raise RuntimeError('down')
        net = flip_close_net_pnl(
            0.3, entry_fees=0.2, notional=200.0,
            broker='bingx', client=BadClient(),
        )
        self.assertEqual(net['feeSource'], 'settings')
        self.assertAlmostEqual(net['feeRatePct'], 0.1, places=9)
        self.assertAlmostEqual(net['exitFeeEst'], 0.2, places=9)
        self.assertAlmostEqual(net['netPnl'], -0.1, places=9)
        self.assertTrue(should_keep_opposite_on_cross(net['netPnl']))

    def test_exchange_taker_rate_used(self):
        class OkBingX:
            def get_commission_rate(self):
                return {'data': {'takerCommissionRate': '0.0005'}}
        net = flip_close_net_pnl(
            1.0, entry_fees=0.0, notional=1000.0,
            broker='bingx', client=OkBingX(),
        )
        # 0.05% of 1000 = 0.5
        self.assertEqual(net['feeSource'], 'exchange')
        self.assertAlmostEqual(net['feeRatePct'], 0.05, places=9)
        self.assertAlmostEqual(net['exitFeeEst'], 0.5, places=9)
        self.assertAlmostEqual(net['netPnl'], 0.5, places=9)

    def test_explicit_rate_skips_fetch(self):
        net = flip_close_net_pnl(1.0, 0, 100.0, broker='bingx', fee_rate_pct=0.2, fee_source='test')
        self.assertAlmostEqual(net['exitFeeEst'], 0.2, places=9)
        self.assertEqual(net['feeSource'], 'test')

    def test_no_fees_matches_upnl(self):
        net = flip_close_net_pnl(0.5, 0, 0, 'bingx')
        self.assertAlmostEqual(net['netPnl'], 0.5, places=9)
        self.assertAlmostEqual(net['exitFeeEst'], 0.0, places=9)

    def test_bad_inputs_zero(self):
        net = flip_close_net_pnl(None, None, None, '')
        self.assertAlmostEqual(net['netPnl'], 0.0, places=9)
        self.assertFalse(should_keep_opposite_on_cross(net['netPnl']))


class BingxCrossSymbolsTest(unittest.TestCase):
    def test_only_bingx_cross_destinations(self):
        config = {
            'routes': [
                {'destinations': [
                    {'broker': 'bingx', 'symbol': 'BTC-USDT', 'cross': True},
                    {'broker': 'bingx', 'symbol': 'ETH-USDT'},
                    {'broker': 'bybit', 'symbol': 'XAUTUSDT', 'cross': True},
                    {'broker': 'finam', 'symbol': 'GAZP', 'cross': True},
                ]},
                {'destinations': [
                    {'broker': 'bingx', 'symbol': 'ng-usdt', 'cross': True},
                ]},
            ]
        }
        self.assertEqual(bingx_cross_symbols(config), {'BTCUSDT', 'NGUSDT'})

    def test_empty_config(self):
        self.assertEqual(bingx_cross_symbols({}), set())
        self.assertEqual(bingx_cross_symbols(None), set())


class BingxPositionUpnlTest(unittest.TestCase):
    def test_known_keys(self):
        self.assertEqual(_bingx_position_upnl({'unrealizedProfit': -2.5}), -2.5)
        self.assertEqual(_bingx_position_upnl({'unrealisedPnl': '1.25'}), 1.25)
        self.assertEqual(_bingx_position_upnl({'unrealizedPnl': 0}), 0.0)

    def test_unknown_fails_closed(self):
        self.assertEqual(_bingx_position_upnl({}), 0.0)
        self.assertEqual(_bingx_position_upnl(None), 0.0)
        self.assertEqual(_bingx_position_upnl({'foo': 1}), 0.0)


class BingxSetMarginTypeWireEnumTest(unittest.TestCase):
    """BingX marginType API accepts CROSSED, not CROSS (UI/journal may pass CROSS)."""

    def _capture(self, margin_type):
        from bingx_adapter import BingXBroker
        captured = {}
        client = BingXBroker.__new__(BingXBroker)  # skip __init__ (no network/env)
        def _fake_request(method, path, params=None, **kwargs):
            captured['path'] = path
            captured['params'] = params or {}
            return {'code': 0}
        client._request = _fake_request
        client.set_margin_type('NCCO-USDT', margin_type)
        return captured

    def test_cross_maps_to_crosed(self):
        cap = self._capture('CROSS')
        self.assertEqual(cap['params'].get('marginType'), 'CROSSED')

    def test_isolated_unchanged(self):
        cap = self._capture('ISOLATED')
        self.assertEqual(cap['params'].get('marginType'), 'ISOLATED')

    def test_crosed_passthrough(self):
        cap = self._capture('CROSSED')
        self.assertEqual(cap['params'].get('marginType'), 'CROSSED')


if __name__ == '__main__':
    unittest.main()
