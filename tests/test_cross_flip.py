#!/usr/bin/env python3
"""Tests for Bybit cross-margin flip decision helper."""
from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'app'))

from execution import should_keep_opposite_on_cross  # noqa: E402


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


if __name__ == '__main__':
    unittest.main()
