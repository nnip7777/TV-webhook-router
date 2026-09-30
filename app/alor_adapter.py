#!/usr/bin/env python3
import asyncio
import importlib.util
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from settings import ALOR_CONFIG_PATH, SMART_EXECUTOR_PATH

# Exchange enum accepted by Alor /md/v2/... routes
ALOR_HISTORY_EXCHANGES = ('MOEX', 'SPBX')


def load_workspace_module():
    spec = importlib.util.spec_from_file_location('smart_order_executor', SMART_EXECUTOR_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _load_alor_cfg() -> Dict[str, Any]:
    return json.loads(ALOR_CONFIG_PATH.read_text())


class AlorBroker:
    def __init__(self):
        self.module = load_workspace_module()
        cfg = _load_alor_cfg()
        self.portfolio = str(cfg.get('portfolio') or '')
        self.client_id = str(cfg.get('client_id') or '')
        self.client = self.module.AlorClient(
            refresh_token=str(cfg.get('refresh_token') or ''),
            portfolio=self.portfolio,
            client_id=self.client_id,
        )

    async def fetch_account_trades(self, lookback_hours: int = 72, since_iso: str = '') -> List[Dict[str, Any]]:
        """All portfolio trades from since_iso (or lookback) + current session.

        History lives at /md/v2/Stats/{exchange}/{portfolio}/history/trades
        (NOT /md/v2/Clients/.../history/trades which returns empty).
        """
        if since_iso:
            date_from = str(since_iso)
            if len(date_from) == 10:
                date_from = f'{date_from}T00:00:00.0000000Z'
            elif not date_from.endswith('Z') and '+' not in date_from:
                date_from = f'{date_from}.0000000Z' if 'T' in date_from else f'{date_from}T00:00:00.0000000Z'
        else:
            start = datetime.now(timezone.utc) - timedelta(hours=max(1, int(lookback_hours)))
            date_from = start.strftime('%Y-%m-%dT%H:%M:%S.0000000Z')
        rows: List[Dict[str, Any]] = []
        seen = set()

        def _add(row: Dict[str, Any], source: str) -> None:
            if not isinstance(row, dict):
                return
            key = str(row.get('id') or row.get('orderno') or '') + ':' + str(row.get('symbol') or '') + ':' + str(row.get('date') or '')
            if key in seen:
                return
            seen.add(key)
            row = dict(row)
            row.setdefault('_alorSource', source)
            rows.append(row)

        async with httpx_client() as client:
            token = await self.client.get_access_token()
            headers = {'Authorization': f'Bearer {token}', 'Accept': 'application/json'}
            for exchange in ALOR_HISTORY_EXCHANGES:
                # Past sessions: /md/v2/Stats/...
                # Pagination cursor is query param `from` (trade id), NOT fromId.
                from_id = ''
                for _page in range(500):
                    params: Dict[str, Any] = {
                        'dateFrom': date_from,
                        'limit': 1000,
                        'orderByTradeDate': 'true',
                        'descending': 'false',
                    }
                    if from_id:
                        params['from'] = from_id
                    resp = await client.get(
                        f'https://api.alor.ru/md/v2/Stats/{exchange}/{self.portfolio}/history/trades',
                        headers=headers,
                        params=params,
                    )
                    if resp.status_code != 200:
                        break
                    batch = resp.json()
                    if not isinstance(batch, list) or not batch:
                        break
                    added = 0
                    for row in batch:
                        before = len(seen)
                        _add(row, f'history:{exchange}')
                        if len(seen) != before:
                            added += 1
                    next_id = str(batch[-1].get('id') or '')
                    if len(batch) < 1000 or not next_id or next_id == from_id:
                        break
                    if added == 0 and from_id:
                        break
                    from_id = next_id

                # Current session: /md/v2/Clients/.../trades (history excludes today)
                try:
                    day_rows = await self.client.get_trades(exchange=exchange, history=False, limit=500)
                except Exception:
                    day_rows = []
                for row in day_rows or []:
                    _add(row, f'session:{exchange}')

        return rows


def httpx_client():
    import httpx
    return httpx.AsyncClient(timeout=60)


def fetch_account_trades_sync(lookback_hours: int = 72, since_iso: str = '') -> List[Dict[str, Any]]:
    return asyncio.run(AlorBroker().fetch_account_trades(lookback_hours, since_iso=since_iso))
