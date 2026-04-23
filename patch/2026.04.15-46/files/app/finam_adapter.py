#!/usr/bin/env python3
import asyncio
import importlib.util
from typing import Any, Dict

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


def auth_check_sync() -> Dict[str, Any]:
    return asyncio.run(FinamBroker().auth_check())
