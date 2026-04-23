#!/usr/bin/env python3
import json
from pathlib import Path
from typing import Any, Dict

import schwab.auth as schwab_auth
import schwab.orders.equities as equities
from schwab.utils import Utils

from settings import SCHWAB_CONFIG_PATH


def load_config() -> Dict[str, Any]:
    with open(SCHWAB_CONFIG_PATH, 'r') as f:
        cfg = json.load(f)
    token_path = str(cfg.get('token_path') or '').strip()
    if token_path and not Path(token_path).expanduser().is_absolute():
        cfg['token_path'] = str((Path(SCHWAB_CONFIG_PATH).resolve().parent / token_path).resolve())
    return cfg


class SchwabBroker:
    def __init__(self):
        cfg = load_config()
        self.cfg = cfg
        self.client = schwab_auth.easy_client(
            api_key=cfg['api_key'],
            app_secret=cfg['app_secret'],
            callback_url=cfg['callback_url'],
            token_path=cfg['token_path'],
        )

    def auth_check(self) -> Dict[str, Any]:
        try:
            resp = self.client.get_account_numbers()
            status = getattr(resp, 'status_code', None)
            text = getattr(resp, 'text', '')
            return {
                'ok': status == 200,
                'status_code': status,
                'preview': text[:1000],
            }
        except Exception as e:
            return {'ok': False, 'error': str(e)}

    def resolve_account_hash(self, requested_account: str) -> str:
        resp = self.client.get_account_numbers()
        data = resp.json()
        if requested_account and requested_account != 'primary':
            for item in data:
                if item.get('accountNumber') == requested_account or item.get('hashValue') == requested_account:
                    return item.get('hashValue')
        if not data:
            raise RuntimeError('No Schwab accounts available')
        return data[0]['hashValue']

    def place_equity_order(self, account_id: str, symbol: str, side: str, qty: Any, dry_run: bool = True) -> Dict[str, Any]:
        resolved_account = self.resolve_account_hash(account_id)
        if dry_run:
            return {
                'dryRun': True,
                'wouldPlace': {
                    'account_id': resolved_account,
                    'symbol': symbol,
                    'side': side,
                    'qty': qty,
                }
            }

        if str(side).lower() == 'buy':
            order_spec = equities.equity_buy_market(symbol, int(float(qty)))
        else:
            order_spec = equities.equity_sell_market(symbol, int(float(qty)))

        resp = self.client.place_order(resolved_account, order_spec)
        order_id = None
        order_status = ''
        order_detail = {}
        try:
            order_id = Utils(self.client, resolved_account).extract_order_id(resp)
        except Exception:
            order_id = None
        if order_id:
            try:
                order_resp = self.client.get_order(order_id, resolved_account)
                if getattr(order_resp, 'status_code', None) == 200:
                    order_detail = order_resp.json() or {}
                    order_status = str(order_detail.get('status') or '')
            except Exception:
                pass
        return {
            'status_code': getattr(resp, 'status_code', None),
            'text': getattr(resp, 'text', ''),
            'location': getattr(resp, 'headers', {}).get('Location', ''),
            'order_id': order_id,
            'order_status': order_status,
            'order_detail': order_detail,
        }


def auth_check_sync() -> Dict[str, Any]:
    return SchwabBroker().auth_check()
