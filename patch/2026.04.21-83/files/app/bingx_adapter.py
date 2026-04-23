#!/usr/bin/env python3
import hashlib
import hmac
import json
import os
import socket
import time
from decimal import Decimal, ROUND_DOWN
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager

from settings import (
    BINGX_API_KEY,
    BINGX_BIND_INTERFACE,
    BINGX_LIVE_BASE_URL,
    BINGX_LIVE_FALLBACK_BASE_URL,
    BINGX_RECV_WINDOW,
    BINGX_SECRET_KEY,
    BINGX_SOURCE_KEY,
    BINGX_TESTNET_BASE_URL,
    BINGX_TESTNET_FALLBACK_BASE_URL,
)


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


class BingXBroker:
    """Минимальный клиент BingX Perpetual Swap API для webhook-router."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        testnet: bool = False,
        bind_interface: Optional[str] = None,
        source_key: Optional[str] = None,
        recv_window: Optional[int] = None,
    ):
        self.api_key = api_key or BINGX_API_KEY or os.getenv('BINGX_API_KEY')
        self.secret_key = secret_key or BINGX_SECRET_KEY or os.getenv('BINGX_SECRET_KEY')
        self.testnet = bool(testnet)
        self.base_urls = [
            BINGX_TESTNET_BASE_URL if self.testnet else BINGX_LIVE_BASE_URL,
            BINGX_TESTNET_FALLBACK_BASE_URL if self.testnet else BINGX_LIVE_FALLBACK_BASE_URL,
        ]
        self.bind_interface = str(bind_interface or BINGX_BIND_INTERFACE or '').strip()
        self.source_key = str(source_key or BINGX_SOURCE_KEY or 'BX-AI-SKILL').strip() or 'BX-AI-SKILL'
        self.recv_window = max(1, min(5000, int(recv_window or BINGX_RECV_WINDOW or 5000)))
        self.session = requests.Session()
        if self.bind_interface:
            adapter = InterfaceBoundHTTPAdapter(self.bind_interface)
            self.session.mount('https://', adapter)
            self.session.mount('http://', adapter)

    def _require_credentials(self) -> None:
        if not self.api_key or not self.secret_key:
            raise ValueError('BingX API credentials are not configured')

    def _sign(self, payload: str) -> str:
        self._require_credentials()
        return hmac.new(
            self.secret_key.encode('utf-8'),
            payload.encode('utf-8'),
            hashlib.sha256,
        ).hexdigest()

    def _canonical(self, params: Dict[str, Any]) -> str:
        return '&'.join(f'{key}={params[key]}' for key in sorted(params.keys()))

    def _request(self, method: str, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        self._require_credentials()
        params = dict(params or {})
        params.setdefault('timestamp', int(time.time() * 1000))
        params.setdefault('recvWindow', self.recv_window)
        canonical = self._canonical(params)
        signature = self._sign(canonical)
        payload = f'{canonical}&signature={signature}'

        headers = {
            'X-BX-APIKEY': self.api_key,
            'X-SOURCE-KEY': self.source_key,
        }

        last_error: Optional[Exception] = None
        for index, base_url in enumerate(self.base_urls):
            try:
                request_kwargs: Dict[str, Any] = {
                    'method': method.upper(),
                    'url': f"{base_url.rstrip('/')}{path}",
                    'headers': headers,
                    'timeout': 20,
                }
                if method.upper() in ('GET', 'DELETE'):
                    request_kwargs['url'] = f"{base_url.rstrip('/')}{path}?{payload}"
                else:
                    request_kwargs['data'] = payload.encode('utf-8')
                    request_kwargs['headers'] = {
                        **headers,
                        'Content-Type': 'application/x-www-form-urlencoded',
                    }

                response = self.session.request(**request_kwargs)
                content_type = response.headers.get('Content-Type', '')
                if 'application/json' in content_type:
                    data = response.json()
                else:
                    try:
                        data = json.loads(response.text)
                    except Exception:
                        data = {'raw': response.text}
                data['_http_status'] = response.status_code
                data['_base_url'] = base_url
                if self.bind_interface:
                    data['_bind_interface'] = self.bind_interface
                return data
            except requests.RequestException as exc:
                last_error = exc
                if index >= len(self.base_urls) - 1:
                    raise
        if last_error:
            raise last_error
        raise RuntimeError('BingX request failed')

    def get_server_time(self) -> Dict[str, Any]:
        return self._request('GET', '/openApi/swap/v2/server/time')

    def get_balance(self) -> Dict[str, Any]:
        return self._request('GET', '/openApi/swap/v3/user/balance')

    def get_positions(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        params: Dict[str, Any] = {}
        if symbol:
            params['symbol'] = symbol
        return self._request('GET', '/openApi/swap/v2/user/positions', params=params)

    def set_margin_type(self, symbol: str, margin_type: str = 'ISOLATED') -> Dict[str, Any]:
        return self._request('POST', '/openApi/swap/v2/trade/marginType', params={
            'symbol': symbol,
            'marginType': str(margin_type or 'ISOLATED').upper(),
        })

    def set_leverage(self, symbol: str, side: str, leverage: int) -> Dict[str, Any]:
        side_normalized = str(side or '').strip().upper()
        if side_normalized not in ('LONG', 'SHORT'):
            side_normalized = 'LONG'
        return self._request('POST', '/openApi/swap/v2/trade/leverage', params={
            'symbol': symbol,
            'side': side_normalized,
            'leverage': max(1, int(leverage)),
        })

    def adjust_isolated_margin(self, symbol: str, position_side: str, amount: Any, direction_type: int = 1) -> Dict[str, Any]:
        return self._request('POST', '/openApi/swap/v2/trade/positionMargin', params={
            'symbol': symbol,
            'positionSide': str(position_side or 'BOTH').upper(),
            'amount': _decimal_text(amount, 8),
            'directionType': int(direction_type),
        })

    def get_contracts(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        params: Dict[str, Any] = {}
        if symbol:
            params['symbol'] = symbol
        return self._request('GET', '/openApi/swap/v2/quote/contracts', params=params)

    def get_book_ticker(self, symbol: str) -> Dict[str, Any]:
        return self._request('GET', '/openApi/swap/v2/quote/bookTicker', params={'symbol': symbol})

    def get_depth(self, symbol: str, limit: int = 20) -> Dict[str, Any]:
        return self._request('GET', '/openApi/swap/v2/quote/depth', params={'symbol': symbol, 'limit': limit})

    def auth_check(self) -> Dict[str, Any]:
        try:
            balance = self.get_balance()
            ok = balance.get('code') == 0
            return {
                'ok': ok,
                'balance': balance,
            }
        except Exception as e:
            return {'ok': False, 'error': str(e)}

    def _normalize_symbol(self, symbol: str) -> str:
        raw = str(symbol or '').strip().upper()
        if not raw:
            return ''
        return raw.replace('-', '').replace('_', '').replace('.P', '').replace('/', '')

    def get_contract(self, symbol: str) -> Dict[str, Any]:
        candidates = []
        raw_symbol = str(symbol or '').strip()
        normalized = self._normalize_symbol(raw_symbol)
        if raw_symbol:
            candidates.append(raw_symbol)
        if normalized and normalized != raw_symbol:
            candidates.append(normalized)
        if normalized.endswith('USDT'):
            dashed = normalized[:-4] + '-USDT'
            if dashed not in candidates:
                candidates.append(dashed)

        for candidate in candidates:
            result = self.get_contracts(symbol=candidate)
            contracts = result.get('data') or []
            if isinstance(contracts, dict) and contracts:
                contract_symbol = str(contracts.get('symbol') or '').strip()
                if contract_symbol and self._normalize_symbol(contract_symbol) == normalized:
                    return contracts
            if isinstance(contracts, list):
                for item in contracts:
                    item_symbol = str(item.get('symbol') or '').strip()
                    if item_symbol.upper() == candidate.upper() or self._normalize_symbol(item_symbol) == normalized:
                        return item
                for item in contracts:
                    if isinstance(item, dict):
                        item_symbol = str(item.get('symbol') or '').strip()
                        if self._normalize_symbol(item_symbol) == normalized:
                            return item

        result = self.get_contracts()
        contracts = result.get('data') or []
        if isinstance(contracts, dict) and contracts:
            contract_symbol = str(contracts.get('symbol') or '').strip()
            if contract_symbol and self._normalize_symbol(contract_symbol) == normalized:
                return contracts
        if isinstance(contracts, list):
            for item in contracts:
                item_symbol = str(item.get('symbol') or '').strip()
                if self._normalize_symbol(item_symbol) == normalized:
                    return item
        return {}

    def prepare_limit_order(self, symbol: str, side: str, qty: Any, price: Optional[Any] = None) -> Dict[str, Any]:
        contract = self.get_contract(symbol)
        if not contract:
            raise RuntimeError(f'BingX contract not found: {symbol}')

        price_precision = _safe_int(contract.get('pricePrecision'))
        quantity_precision = _safe_int(contract.get('quantityPrecision'))

        book = {}
        selected_price = price
        side_normalized = str(side or '').strip().lower()
        if side_normalized not in ('buy', 'sell'):
            raise ValueError(f'Unsupported side for BingX: {side}')

        contract_symbol = str(contract.get('symbol') or symbol)

        if selected_price in (None, ''):
            book_result = self.get_book_ticker(contract_symbol)
            if book_result.get('code') != 0:
                raise RuntimeError(str(book_result.get('msg') or f"BingX bookTicker error {book_result.get('code')}"))
            book = book_result.get('data') or {}
            ticker = book.get('book_ticker') if isinstance(book.get('book_ticker'), dict) else book
            selected_price = ticker.get('askPrice') if side_normalized == 'buy' else ticker.get('bidPrice')
            if selected_price in (None, '', '0'):
                selected_price = ticker.get('ask_price') if side_normalized == 'buy' else ticker.get('bid_price')
            if selected_price in (None, '', '0'):
                selected_price = (
                    ticker.get('price')
                    or ticker.get('lastPrice')
                    or ticker.get('last_price')
                    or contract.get('lastPrice')
                    or contract.get('last_price')
                    or contract.get('markPrice')
                    or contract.get('mark_price')
                )
            if selected_price in (None, '', '0'):
                raise RuntimeError(
                    'BingX best quote is unavailable for '
                    f"{contract_symbol} | book={json.dumps(book, ensure_ascii=False)}"
                    f" | contract={json.dumps(contract, ensure_ascii=False)}"
                )

        quantity_text = _decimal_text(qty, quantity_precision)
        price_text = _decimal_text(selected_price, price_precision)
        if Decimal(quantity_text) <= 0:
            raise ValueError('BingX quantity must be greater than 0')
        if Decimal(price_text) <= 0:
            raise ValueError('BingX price must be greater than 0')

        return {
            'symbol': contract_symbol,
            'side': 'BUY' if side_normalized == 'buy' else 'SELL',
            'quantity': quantity_text,
            'price': price_text,
            'contract': contract,
            'bookTicker': book,
        }

    def place_limit_order(
        self,
        prepared: Dict[str, Any],
        position_side: str = 'BOTH',
        reduce_only: Optional[bool] = None,
        client_order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            'symbol': prepared['symbol'],
            'side': prepared['side'],
            'type': 'LIMIT',
            'quantity': prepared['quantity'],
            'price': prepared['price'],
            'timeInForce': 'GTC',
            'positionSide': str(position_side or 'BOTH').upper(),
        }
        if client_order_id:
            params['clientOrderId'] = str(client_order_id)
        if reduce_only is not None:
            params['reduceOnly'] = 'true' if bool(reduce_only) else 'false'
        return self._request('POST', '/openApi/swap/v2/trade/order', params=params)


def _safe_int(value: Any, default: int = 8) -> int:
    try:
        return max(0, int(value))
    except Exception:
        return default


def _decimal_text(value: Any, precision: int) -> str:
    dec = Decimal(str(value))
    if precision >= 0:
        quantum = Decimal('1').scaleb(-precision)
        dec = dec.quantize(quantum, rounding=ROUND_DOWN)
    text = format(dec, 'f')
    if '.' in text:
        text = text.rstrip('0').rstrip('.')
    return text or '0'
