#!/usr/bin/env python3
import hashlib
import hmac
import json
import os
import re
import socket
import time
from typing import Any, Dict, Optional
from urllib.parse import urlencode

import requests
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager

BYBIT_RECV_WINDOW_MS = 30000

from settings import BYBIT_BIND_INTERFACE, BYBIT_LIVE_BASE_URL, BYBIT_TESTNET_BASE_URL


class InterfaceBoundHTTPAdapter(HTTPAdapter):
    def __init__(self, interface_name: str, *args, **kwargs):
        self.interface_name = str(interface_name or '').strip()
        super().__init__(*args, **kwargs)

    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        options = list(pool_kwargs.get('socket_options') or [])
        if self.interface_name and hasattr(socket, 'SO_BINDTODEVICE'):
            options.append((socket.SOL_SOCKET, socket.SO_BINDTODEVICE, self.interface_name.encode()))
        pool_kwargs['socket_options'] = options
        self.poolmanager = PoolManager(num_pools=connections, maxsize=maxsize, block=block, **pool_kwargs)


class BybitBroker:
    """Минимальный клиент Bybit API v5 для webhook-router."""

    def __init__(self, api_key: Optional[str] = None, secret_key: Optional[str] = None, testnet: bool = False, bind_interface: Optional[str] = None):
        self.api_key = api_key or os.getenv('BYBIT_API_KEY')
        self.secret_key = secret_key or os.getenv('BYBIT_SECRET_KEY')
        self.testnet = testnet
        self.base_url = BYBIT_TESTNET_BASE_URL if testnet else BYBIT_LIVE_BASE_URL
        self.bind_interface = str(bind_interface or BYBIT_BIND_INTERFACE or '').strip()
        self.session = requests.Session()
        if self.bind_interface:
            adapter = InterfaceBoundHTTPAdapter(self.bind_interface)
            self.session.mount('https://', adapter)
            self.session.mount('http://', adapter)
        self._time_offset_ms = 0
        self._time_offset_checked_at = 0.0

        if not self.api_key or not self.secret_key:
            raise ValueError('Bybit API credentials are not configured')

    def _sign(self, payload: str) -> str:
        return hmac.new(
            self.secret_key.encode('utf-8'),
            payload.encode('utf-8'),
            hashlib.sha256,
        ).hexdigest()

    def _server_time_ms(self) -> Optional[int]:
        try:
            response = self.session.get(f'{self.base_url}/v5/market/time', timeout=10)
            data = response.json()
            result = data.get('result') or {}
            time_value = result.get('timeNano') or result.get('timeSecond') or result.get('time') or data.get('time')
            if time_value is None:
                return None
            text = str(time_value)
            if len(text) > 13:
                value = int(text[:13])
            else:
                value = int(text)
                if value < 10**12:
                    value *= 1000
            return value
        except Exception:
            return None

    def _set_time_offset_ms(self, server_ms: Optional[int]) -> None:
        if server_ms is None:
            return
        now = time.time()
        self._time_offset_ms = int(server_ms) - int(now * 1000)
        self._time_offset_checked_at = now

    def _extract_server_time_ms(self, payload: Dict[str, Any]) -> Optional[int]:
        result = (payload or {}).get('result') or {}
        for value in (
            result.get('timeNano'),
            result.get('timeSecond'),
            result.get('time'),
            (payload or {}).get('time'),
        ):
            if value in (None, ''):
                continue
            try:
                text = str(value)
                if len(text) > 13:
                    return int(text[:13])
                parsed = int(text)
                if parsed < 10**12:
                    parsed *= 1000
                return parsed
            except Exception:
                pass
        message = str((payload or {}).get('retMsg') or (payload or {}).get('msg') or '')
        match = re.search(r'server_timestamp\[(\d+)\]', message)
        if match:
            try:
                return int(match.group(1))
            except Exception:
                return None
        return None

    def _current_timestamp_ms(self) -> int:
        now = time.time()
        if (now - self._time_offset_checked_at) > 30:
            self._set_time_offset_ms(self._server_time_ms())
        return int(time.time() * 1000) + int(self._time_offset_ms)

    def _request(self, method: str, path: str, params: Optional[Dict[str, Any]] = None, body: Optional[Dict[str, Any]] = None, retry_on_time_error: bool = True) -> Dict[str, Any]:
        params = params or {}
        body = body or {}

        timestamp = str(self._current_timestamp_ms())
        recv_window = str(BYBIT_RECV_WINDOW_MS)

        request_kwargs: Dict[str, Any] = {
            'method': method.upper(),
            'url': f'{self.base_url}{path}',
            'headers': {
                'X-BAPI-API-KEY': self.api_key,
                'X-BAPI-SIGN-TYPE': '2',
                'X-BAPI-TIMESTAMP': timestamp,
                'X-BAPI-RECV-WINDOW': recv_window,
            },
            'timeout': 20,
        }

        if method.upper() == 'GET':
            payload_str = urlencode(params)
            request_kwargs['params'] = params
        else:
            payload_str = json.dumps(body, separators=(',', ':'), ensure_ascii=False)
            request_kwargs['data'] = payload_str.encode('utf-8')
            request_kwargs['headers']['Content-Type'] = 'application/json'

        signature_payload = f'{timestamp}{self.api_key}{recv_window}{payload_str}'
        request_kwargs['headers']['X-BAPI-SIGN'] = self._sign(signature_payload)

        response = self.session.request(**request_kwargs)

        content_type = response.headers.get('Content-Type', '')
        if 'application/json' in content_type:
            try:
                data = response.json()
            except (json.JSONDecodeError, ValueError):
                data = {'raw': response.text, 'json_parse_error': True}
        else:
            data = {'raw': response.text}

        data['_http_status'] = response.status_code
        if self.bind_interface:
            data['_bind_interface'] = self.bind_interface

        ret_code = data.get('retCode')
        if retry_on_time_error and ret_code in (10002, '10002'):
            server_ms = self._extract_server_time_ms(data) or self._server_time_ms()
            self._set_time_offset_ms(server_ms)
            return self._request(method, path, params=params, body=body, retry_on_time_error=False)

        return data

    def get_api_info(self) -> Dict[str, Any]:
        return self._request('GET', '/v5/user/query-api')

    def get_wallet_balance(self, account_type: str = 'UNIFIED') -> Dict[str, Any]:
        return self._request('GET', '/v5/account/wallet-balance', params={'accountType': account_type})

    def get_positions(self, category: str = 'linear', symbol: Optional[str] = None, settle_coin: Optional[str] = None) -> Dict[str, Any]:
        params: Dict[str, Any] = {'category': category}
        if symbol:
            params['symbol'] = symbol
        elif settle_coin:
            params['settleCoin'] = settle_coin
        return self._request('GET', '/v5/position/list', params=params)

    def add_margin(self, symbol: str, margin: Any, category: str = 'linear', position_idx: Optional[int] = None) -> Dict[str, Any]:
        """Add isolated margin (POST /v5/position/add-margin). margin > 0 only.

        Bybit rejects more than 4 decimal places (retCode 10001 'margin only
        supports 4 decimal places').
        """
        from decimal import Decimal, ROUND_HALF_UP
        try:
            q = Decimal(str(margin)).quantize(Decimal('0.0001'), rounding=ROUND_HALF_UP)
            margin_text = format(q, 'f').rstrip('0').rstrip('.') if q == q.to_integral() else format(q.normalize(), 'f')
            if not margin_text or Decimal(margin_text) <= 0:
                margin_text = '0.0001'
        except Exception:
            margin_text = str(margin)
        body: Dict[str, Any] = {
            'category': category,
            'symbol': symbol,
            'margin': margin_text,
        }
        if position_idx is not None:
            body['positionIdx'] = int(position_idx)
        return self._request('POST', '/v5/position/add-margin', body=body)

    def set_margin_mode(self, symbol: str, trade_mode: int = 1, position_idx: Optional[int] = None) -> Dict[str, Any]:
        """trade_mode: 0=cross, 1=isolated (Bybit linear)."""
        body: Dict[str, Any] = {
            'symbol': symbol,
            'tradeMode': int(trade_mode),
        }
        if position_idx is not None:
            body['positionIdx'] = int(position_idx)
        return self._request('POST', '/v5/position/set-margin-mode', body=body)

    def get_qty_rules(self, symbol: str, category: str = 'linear') -> Dict[str, Any]:
        """lotSizeFilter for linear symbol: minOrderQty / qtyStep / minNotionalValue."""
        cache = getattr(self, '_qty_rules_cache', None)
        if cache is None:
            cache = {}
            self._qty_rules_cache = cache
        key = f'{category}:{symbol}'
        if key in cache:
            return cache[key]
        rules = {'minOrderQty': 0.001, 'qtyStep': 0.001, 'minNotionalValue': 0.0}
        try:
            info = self._request('GET', '/v5/market/instruments-info', params={'category': category, 'symbol': symbol})
            row = (((info.get('result') or {}).get('list') or [None]) or [None])[0]
            lot = (row or {}).get('lotSizeFilter') or {}
            rules = {
                'minOrderQty': float(lot.get('minOrderQty') or 0.001),
                'qtyStep': float(lot.get('qtyStep') or 0.001),
                'minNotionalValue': float(lot.get('minNotionalValue') or 0),
            }
        except Exception:
            pass
        cache[key] = rules
        return rules

    def usdt_to_contract_qty(self, symbol: str, usdt_qty: Any, price: Any, category: str = 'linear') -> float:
        """USDT notional → contracts, quantized DOWN to qtyStep (Bybit linear API takes contracts)."""
        from decimal import Decimal, ROUND_DOWN
        usdt = Decimal(str(usdt_qty or '0'))
        px = Decimal(str(price or '0'))
        if usdt <= 0 or px <= 0:
            return 0.0
        rules = self.get_qty_rules(symbol, category)
        step = Decimal(str(rules.get('qtyStep') or '0.001'))
        min_qty = Decimal(str(rules.get('minOrderQty') or '0.001'))
        contracts = (usdt / px)
        contracts = contracts.quantize(step, rounding=ROUND_DOWN)
        if contracts < min_qty:
            return 0.0
        min_notional = Decimal(str(rules.get('minNotionalValue') or '0'))
        if min_notional > 0 and contracts * px < min_notional:
            return 0.0
        return float(contracts)

    def place_order(
        self,
        symbol: str,
        side: str,
        qty: Any,
        category: str = 'linear',
        order_type: str = 'Market',
        price: Optional[Any] = None,
        reduce_only: bool = False,
        position_idx: Optional[int] = None,
        qty_kind: str = 'contracts',
    ) -> Dict[str, Any]:
        """qty: contracts by default; qty_kind='usdt' converts via lotSizeFilter."""
        out_qty = qty
        if str(qty_kind or '').lower() in ('usdt', 'quote', 'quote_usdt', 'notional'):
            px = price
            if px in (None, '', 0, '0'):
                try:
                    book = self._request('GET', '/v5/market/tickers', params={'category': category, 'symbol': symbol})
                    row = (((book.get('result') or {}).get('list') or [None]) or [None])[0] or {}
                    px = row.get('markPrice') or row.get('lastPrice') or 0
                except Exception:
                    px = 0
            out_qty = self.usdt_to_contract_qty(symbol, qty, px, category)
            if not out_qty:
                return {'retCode': 10001, 'retMsg': 'Qty invalid (usdt below minOrderQty/minNotional)', 'result': {}}
        body: Dict[str, Any] = {
            'category': category,
            'symbol': symbol,
            'side': 'Buy' if str(side).lower() == 'buy' else 'Sell',
            'orderType': 'Limit' if str(order_type).lower() == 'limit' else 'Market',
            'qty': str(out_qty),
            'reduceOnly': reduce_only,
        }
        if position_idx is not None:
            body['positionIdx'] = int(position_idx)

        if body['orderType'] == 'Limit':
            if price is None:
                raise ValueError('Limit order requires price')
            body['price'] = str(price)
            body['timeInForce'] = 'GTC'
        else:
            body['timeInForce'] = 'IOC'

        return self._request('POST', '/v5/order/create', body=body)
