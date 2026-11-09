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
    should_keep_opposite_on_cross,
)


class ShouldKeepOppositeOnCrossTest(unittest.TestCase):
    def test_loss_keeps(self):
        self.assertTrue(should_keep_opposite_on_cross(-1.5))
        self.assertTrue(should_keep_opposite_on_cross(-0.01))

    def test_profit_closes(self):
        self.assertFalse(should_keep_opposite_on_cross(1.2))
        self.assertFalse(should_keep_opposite_on_cross(0.0))

    def test_unknown_fails_toward_close(self):
        self.assertFalse(should_keep_opposite_on_cross(None))
        self.assertFalse(should_keep_opposite_on_cross('abc'))
        self.assertFalse(should_keep_opposite_on_cross(''))


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


if __name__ == '__main__':
    unittest.main()
