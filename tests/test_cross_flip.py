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
    bingx_cross_symbols,
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


class FlipCloseNetPnlTest(unittest.TestCase):
    """net = upnl − entry fees already paid − notional × close fee rate%."""

    def setUp(self):
        self._saved = {}
        for k in ('BINGX_FLIP_FEE_RATE_PCT', 'BYBIT_FLIP_FEE_RATE_PCT', 'FLIP_FEE_RATE_PCT'):
            self._saved[k] = os.environ.pop(k, None)

    def tearDown(self):
        for k, v in self._saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def test_gross_profit_net_loss_keeps(self):
        # upnl +0.3 but entry 0.2 + exit 0.2 → net −0.1 → keep
        os.environ['BINGX_FLIP_FEE_RATE_PCT'] = '0.1'  # 0.1% of notional
        net = flip_close_net_pnl(0.3, entry_fees=0.2, notional=200.0, broker='bingx')
        self.assertAlmostEqual(net['exitFeeEst'], 0.2, places=9)
        self.assertAlmostEqual(net['netPnl'], -0.1, places=9)
        self.assertTrue(should_keep_opposite_on_cross(net['netPnl']))

    def test_net_profit_closes(self):
        os.environ['BYBIT_FLIP_FEE_RATE_PCT'] = '0.05'
        net = flip_close_net_pnl(1.0, entry_fees=0.1, notional=100.0, broker='bybit')
        # exit = 100 * 0.05/100 = 0.05; net = 1 - 0.1 - 0.05 = 0.85
        self.assertAlmostEqual(net['netPnl'], 0.85, places=9)
        self.assertFalse(should_keep_opposite_on_cross(net['netPnl']))

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
