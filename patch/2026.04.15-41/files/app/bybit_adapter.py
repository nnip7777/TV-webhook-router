#!/usr/bin/env python3
import hashlib
import hmac
import json
import os
import socket
import time
from typing import Any, Dict, Optional
from urllib.parse import urlencode

import requests
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager

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

        if not self.api_key or not self.secret_key:
            raise ValueError('Bybit API credentials are not configured')

    def _sign(self, payload: str) -> str:
        return hmac.new(
            self.secret_key.encode('utf-8'),
            payload.encode('utf-8'),
            hashlib.sha256,
        ).hexdigest()

    def _request(self, method: str, path: str, params: Optional[Dict[str, Any]] = None, body: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        params = params or {}
        body = body or {}

        timestamp = str(int(time.time() * 1000))
        recv_window = '5000'

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
            data = response.json()
        else:
            data = {'raw': response.text}

        data['_http_status'] = response.status_code
        if self.bind_interface:
            data['_bind_interface'] = self.bind_interface
        return data

    def get_api_info(self) -> Dict[str, Any]:
        return self._request('GET', '/v5/user/query-api')

    def place_order(
        self,
        symbol: str,
        side: str,
        qty: Any,
        category: str = 'linear',
        order_type: str = 'Market',
        price: Optional[Any] = None,
        reduce_only: bool = False,
    ) -> Dict[str, Any]:
        body: Dict[str, Any] = {
            'category': category,
            'symbol': symbol,
            'side': 'Buy' if str(side).lower() == 'buy' else 'Sell',
            'orderType': 'Limit' if str(order_type).lower() == 'limit' else 'Market',
            'qty': str(qty),
            'reduceOnly': reduce_only,
        }

        if body['orderType'] == 'Limit':
            if price is None:
                raise ValueError('Limit order requires price')
            body['price'] = str(price)
            body['timeInForce'] = 'GTC'
        else:
            body['timeInForce'] = 'IOC'

        return self._request('POST', '/v5/order/create', body=body)
