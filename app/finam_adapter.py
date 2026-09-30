#!/usr/bin/env python3
import asyncio
import importlib.util
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from settings import FINAM_ACCOUNT_ID, FINAM_SECRET_PATH, SMART_EXECUTOR_PATH


def load_workspace_module():
    spec = importlib.util.spec_from_file_location('smart_order_executor', SMART_EXECUTOR_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class FinamBroker:
    def __init__(self):
        self.module = load_workspace_module()
        secret = FINAM_SECRET_PATH.read_text().strip()
        self.client = self.module.FinamClient(secret, FINAM_ACCOUNT_ID)

    async def auth_check(self) -> Dict[str, Any]:
        try:
            await self.client.get_jwt_token()
            account = await self.client.get_account()
            return {'ok': True, 'account': account}
        except Exception as e:
            return {'ok': False, 'error': str(e)}

    async def place_order(self, symbol: str, side: str, qty: Any, exchange: str, price=None, dry_run: bool = True) -> Dict[str, Any]:
        if dry_run:
            return {
                'dryRun': True,
                'wouldPlace': {
                    'symbol': symbol,
                    'side': side,
                    'qty': qty,
                    'exchange': exchange,
                    'price': price,
                }
            }

        order_type = 'ORDER_TYPE_LIMIT' if price is not None else 'ORDER_TYPE_MARKET'
        return await self.client.place_order(
            symbol=symbol,
            board=exchange,
            side='SIDE_BUY' if str(side).lower() == 'buy' else 'SIDE_SELL',
            quantity=int(float(qty)),
            price=price,
            order_type=order_type,
        )

    async def fetch_account_trades(self, lookback_hours: int = 72, since_iso: str = '') -> List[Dict[str, Any]]:
        end = datetime.now(timezone.utc)
        if since_iso:
            try:
                start = datetime.fromisoformat(str(since_iso).replace('Z', '+00:00'))
                if start.tzinfo is None:
                    start = start.replace(tzinfo=timezone.utc)
            except Exception:
                start = end - timedelta(hours=max(1, int(lookback_hours)))
        else:
            start = end - timedelta(hours=max(1, int(lookback_hours)))
        return await self._paged_trades(start, end)

    async def _paged_trades(self, start: datetime, end: datetime) -> List[Dict[str, Any]]:
        # Finam caps one response at 100 trades and rejects very old endTime.
        rows: List[Dict[str, Any]] = []
        cursor = start
        # Walk forward in 14-day windows; if a window is full (100), split it.
        async def _pull(a: datetime, b: datetime) -> List[Dict[str, Any]]:
            if a >= b:
                return []
            start_iso = a.strftime('%Y-%m-%dT%H:%M:%SZ')
            end_iso = b.strftime('%Y-%m-%dT%H:%M:%SZ')
            try:
                batch = await self.client.get_account_trades(start_iso, end_iso)
            except Exception:
                # Older windows may be rejected by Finam retention — skip.
                return []
            return list(batch or [])

        async def _walk(a: datetime, b: datetime, depth: int = 0) -> List[Dict[str, Any]]:
            batch = await _pull(a, b)
            if len(batch) < 100 or depth >= 8:
                return batch
            mid = a + (b - a) / 2
            left = await _walk(a, mid, depth + 1)
            right = await _walk(mid, b, depth + 1)
            return left + right

        while cursor < end:
            chunk_end = min(cursor + timedelta(days=14), end)
            rows.extend(await _walk(cursor, chunk_end))
            cursor = chunk_end
        return rows

    async def fetch_account_transactions(self, lookback_hours: int = 72, since_iso: str = '') -> List[Dict[str, Any]]:
        end = datetime.now(timezone.utc)
        if since_iso:
            try:
                start = datetime.fromisoformat(str(since_iso).replace('Z', '+00:00'))
                if start.tzinfo is None:
                    start = start.replace(tzinfo=timezone.utc)
            except Exception:
                start = end - timedelta(hours=max(1, int(lookback_hours)))
        else:
            start = end - timedelta(hours=max(1, int(lookback_hours)))
        rows: List[Dict[str, Any]] = []
        cursor = start
        max_span = timedelta(days=30)
        while cursor < end:
            chunk_end = min(cursor + max_span, end)
            start_iso = cursor.strftime('%Y-%m-%dT%H:%M:%SZ')
            end_iso = chunk_end.strftime('%Y-%m-%dT%H:%M:%SZ')
            try:
                batch = await self.client.get_account_transactions(start_iso, end_iso)
            except Exception:
                batch = []
            rows.extend(batch or [])
            cursor = chunk_end
        return rows


def auth_check_sync() -> Dict[str, Any]:
    return asyncio.run(FinamBroker().auth_check())


def fetch_account_trades_sync(lookback_hours: int = 72, since_iso: str = '') -> List[Dict[str, Any]]:
    return asyncio.run(FinamBroker().fetch_account_trades(lookback_hours, since_iso=since_iso))


def fetch_account_transactions_sync(lookback_hours: int = 72, since_iso: str = '') -> List[Dict[str, Any]]:
    return asyncio.run(FinamBroker().fetch_account_transactions(lookback_hours, since_iso=since_iso))
