#!/usr/bin/env python3
import hashlib
import hmac
import json
import os
import socket
import time
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from typing import Any, Dict, List, Optional


def _bingx_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    for key in ('data', 'result', 'symbols', 'list'):
        rows = (payload or {}).get(key)
        if isinstance(rows, list):
            return [item for item in rows if isinstance(item, dict)]
        if isinstance(rows, dict):
            if all(not isinstance(v, (list, dict)) for v in rows.values()):
                return [rows]
            for nested_key in ('list', 'symbols', 'data', 'result'):
                nested = rows.get(nested_key)
                if isinstance(nested, list):
                    return [item for item in nested if isinstance(item, dict)]
    return []

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

    def _public_request(self, method: str, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        params = dict(params or {})
        last_error: Optional[Exception] = None
        for index, base_url in enumerate(self.base_urls):
            try:
                url = f"{base_url.rstrip('/')}{path}"
                query = self._canonical(params) if params else ''
                if query:
                    url = f"{url}?{query}"
                response = self.session.request(method=method.upper(), url=url, timeout=20)
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
                data['_request_path'] = path
                data['_request_query'] = query
                data['_request_mode'] = 'public'
                data['_response_snippet'] = str(data)[:500]
                if self.bind_interface:
                    data['_bind_interface'] = self.bind_interface
                return data
            except requests.RequestException as exc:
                last_error = exc
                if index >= len(self.base_urls) - 1:
                    raise
        if last_error:
            raise last_error
        raise RuntimeError('BingX public request failed')

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
        return self._public_request('GET', '/openApi/swap/v2/server/time')

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

    def get_position_mode(self) -> Dict[str, Any]:
        return self._request('GET', '/openApi/swap/v1/positionSide/dual')

    def set_position_mode(self, hedged: bool) -> Dict[str, Any]:
        return self._request('POST', '/openApi/swap/v1/positionSide/dual', params={
            'dualSidePosition': 'true' if bool(hedged) else 'false',
        })

    def get_contracts(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        params: Dict[str, Any] = {}
        if symbol:
            params['symbol'] = symbol
        return self._public_request('GET', '/openApi/swap/v2/quote/contracts', params=params)

    def get_symbols_v3(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        params: Dict[str, Any] = {}
        if symbol:
            params['symbol'] = symbol
        return self._public_request('GET', '/openApi/swap/v3/quote/symbols', params=params)

    def get_book_ticker(self, symbol: str) -> Dict[str, Any]:
        return self._request('GET', '/openApi/swap/v2/quote/bookTicker', params={'symbol': symbol})

    def get_depth(self, symbol: str, limit: int = 20) -> Dict[str, Any]:
        return self._request('GET', '/openApi/swap/v2/quote/depth', params={'symbol': symbol, 'limit': limit})

    def get_best_depth_price(self, symbol: str, side: str, target_qty: Any, qty_kind: str = 'contracts', limit: int = 20) -> Dict[str, Any]:
        depth_result = self.get_depth(symbol, limit=limit)
        if depth_result.get('code') != 0:
            raise RuntimeError(str(depth_result.get('msg') or f"BingX depth error {depth_result.get('code')}"))
        raw = depth_result.get('data') or {}
        if isinstance(raw, list):
            raw = raw[0] if raw and isinstance(raw[0], dict) else {}
        bids = raw.get('bids') or []
        asks = raw.get('asks') or []
        side_normalized = str(side or '').strip().lower()
        qty_kind_normalized = str(qty_kind or 'contracts').strip().lower()
        levels = asks if side_normalized == 'buy' else bids
        target = Decimal(str(target_qty or '0'))
        if target <= 0:
            return {
                'selectedPrice': None,
                'coveredQty': '0',
                'fullyCovered': True,
                'levelsUsed': [],
                'depth': raw,
            }

        covered = Decimal('0')
        levels_used = []
        selected_price = None
        for level in levels:
            if not isinstance(level, (list, tuple)) or len(level) < 2:
                continue
            try:
                price = Decimal(str(level[0]))
                level_qty = Decimal(str(level[1]))
            except Exception:
                continue
            if price <= 0 or level_qty <= 0:
                continue
            available = level_qty if qty_kind_normalized not in ('usdt', 'quote', 'quote_usdt', 'notional') else level_qty * price
            covered += available
            levels_used.append({
                'price': format(price, 'f'),
                'rawQty': format(level_qty, 'f'),
                'availableQty': format(available, 'f'),
            })
            selected_price = price
            if covered >= target:
                break

        return {
            'selectedPrice': None if selected_price is None else format(selected_price, 'f'),
            'coveredQty': format(covered, 'f'),
            'fullyCovered': covered >= target,
            'levelsUsed': levels_used,
            'depth': raw,
        }

    def get_order(self, symbol: str, order_id: Optional[str] = None, client_order_id: Optional[str] = None) -> Dict[str, Any]:
        params: Dict[str, Any] = {'symbol': symbol}
        if order_id:
            params['orderId'] = str(order_id)
        if client_order_id:
            params['clientOrderId'] = str(client_order_id)
        return self._request('GET', '/openApi/swap/v2/trade/order', params=params)

    def get_all_fill_orders(
        self,
        symbol: str,
        start_ts: int,
        end_ts: int,
        order_id: Optional[str] = None,
        trading_unit: str = 'COIN',
        currency: Optional[str] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            'symbol': symbol,
            'tradingUnit': str(trading_unit or 'COIN').upper(),
            'startTs': int(start_ts),
            'endTs': int(end_ts),
        }
        if order_id:
            params['orderId'] = str(order_id)
        if currency:
            params['currency'] = str(currency).upper()
        return self._request('GET', '/openApi/swap/v2/trade/allFillOrders', params=params)

    def get_all_orders(
        self,
        symbol: Optional[str] = None,
        order_id: Optional[str] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 100,
        currency: Optional[str] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            'limit': max(1, min(1000, int(limit))),
        }
        if symbol:
            params['symbol'] = str(symbol)
        if order_id:
            params['orderId'] = str(order_id)
        if start_time is not None:
            params['startTime'] = int(start_time)
        if end_time is not None:
            params['endTime'] = int(end_time)
        if currency:
            params['currency'] = str(currency).upper()
        return self._request('GET', '/openApi/swap/v2/trade/allOrders', params=params)

    def get_commission_rate(self) -> Dict[str, Any]:
        return self._request('GET', '/openApi/cswap/v1/user/commissionRate')

    def cancel_order(self, symbol: str, order_id: Optional[str] = None, client_order_id: Optional[str] = None) -> Dict[str, Any]:
        params: Dict[str, Any] = {'symbol': symbol}
        if order_id:
            params['orderId'] = str(order_id)
        if client_order_id:
            params['clientOrderId'] = str(client_order_id)
        return self._request('DELETE', '/openApi/swap/v2/trade/order', params=params)

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
        return raw.replace('-', '').replace('_', '').replace('.P', '').replace('/', '').replace('PERP', '')

    def _symbol_alias_candidates(self, symbol: str) -> List[str]:
        raw = str(symbol or '').strip().upper()
        normalized = self._normalize_symbol(raw)
        candidates: List[str] = []
        for value in (raw, normalized):
            if value and value not in candidates:
                candidates.append(value)
        if normalized.endswith('USDT'):
            base = normalized[:-4]
            for value in (
                f'{base}-USDT',
                f'{base}USDT',
                f'{base}2USDUSDT',
                f'NCSI{base}2USDUSDT',
                f'{base}(7*24)USDT',
                f'NCSI724{base}2USDUSDT',
            ):
                norm_value = self._normalize_symbol(value)
                if norm_value and norm_value not in candidates:
                    candidates.append(norm_value)
        return candidates

    def get_contract(self, symbol: str) -> Dict[str, Any]:
        raw_symbol = str(symbol or '').strip()
        normalized = self._normalize_symbol(raw_symbol)
        aliases = self._symbol_alias_candidates(raw_symbol)
        alias_set = set(aliases)
        exact_candidates: List[str] = []
        for value in (raw_symbol, normalized):
            if value and value not in exact_candidates:
                exact_candidates.append(value)
        if normalized.endswith('USDT'):
            dashed = normalized[:-4] + '-USDT'
            if dashed not in exact_candidates:
                exact_candidates.append(dashed)

        try:
            v3_symbols = self.get_symbols_v3()
            v3_rows = _bingx_rows(v3_symbols)
        except Exception:
            v3_rows = []
        v3_index = {
            self._normalize_symbol(str(item.get('symbol') or '').strip()): str(item.get('symbol') or '').strip()
            for item in v3_rows if isinstance(item, dict) and str(item.get('symbol') or '').strip()
        }
        resolved_symbol = v3_index.get(normalized)
        if resolved_symbol and resolved_symbol not in exact_candidates:
            exact_candidates.insert(0, resolved_symbol)

        for candidate in exact_candidates:
            result = self.get_contracts(symbol=candidate)
            contracts = result.get('data') or []
            if isinstance(contracts, dict) and contracts:
                contract_symbol = str(contracts.get('symbol') or '').strip()
                contract_display = str(contracts.get('displayName') or '').strip()
                contract_asset = str(contracts.get('asset') or '').strip()
                if (
                    contract_symbol and self._normalize_symbol(contract_symbol) in alias_set
                ) or self._normalize_symbol(contract_display) in alias_set or self._normalize_symbol(contract_asset) in alias_set:
                    return contracts
            if isinstance(contracts, list):
                for item in contracts:
                    item_symbol = str(item.get('symbol') or '').strip()
                    item_display = str(item.get('displayName') or '').strip()
                    item_asset = str(item.get('asset') or '').strip()
                    if self._normalize_symbol(item_symbol) in alias_set or self._normalize_symbol(item_display) in alias_set or self._normalize_symbol(item_asset) in alias_set:
                        return item

        result = self.get_contracts()
        contracts = _bingx_rows(result)
        ranked: List[tuple] = []
        for item in contracts:
            item_symbol = str(item.get('symbol') or '').strip()
            item_display = str(item.get('displayName') or '').strip()
            item_asset = str(item.get('asset') or '').strip()
            item_norm = self._normalize_symbol(item_symbol)
            display_norm = self._normalize_symbol(item_display)
            asset_norm = self._normalize_symbol(item_asset)
            if item_norm in alias_set or display_norm in alias_set or asset_norm in alias_set:
                score = 0
                if item_norm == normalized:
                    score += 100
                if display_norm == normalized:
                    score += 90
                if asset_norm == normalized:
                    score += 80
                if normalized and normalized in display_norm:
                    score += 60
                if normalized and normalized in asset_norm:
                    score += 50
                if normalized and normalized in item_norm:
                    score += 40
                if '724' in asset_norm or '724' in item_norm or '724' in display_norm:
                    score -= 10
                ranked.append((score, item))
        if ranked:
            ranked.sort(key=lambda pair: pair[0], reverse=True)
            return ranked[0][1]
        return {}

    def prepare_limit_order(self, symbol: str, side: str, qty: Any, price: Optional[Any] = None, qty_kind: str = 'contracts') -> Dict[str, Any]:
        contract = self.get_contract(symbol)
        if not contract:
            raise RuntimeError(f'BingX contract not found: {symbol}')

        price_precision = _safe_int(contract.get('pricePrecision'))
        quantity_precision = _safe_int(contract.get('quantityPrecision'))

        book = {}
        depth_plan = {}
        selected_price = price
        side_normalized = str(side or '').strip().lower()
        qty_kind_normalized = str(qty_kind or 'contracts').strip().lower()
        if side_normalized not in ('buy', 'sell'):
            raise ValueError(f'Unsupported side for BingX: {side}')

        contract_symbol = str(contract.get('symbol') or symbol)

        if selected_price in (None, ''):
            try:
                depth_plan = self.get_best_depth_price(contract_symbol, side_normalized, qty, qty_kind=qty_kind_normalized, limit=20)
                selected_price = depth_plan.get('selectedPrice')
            except Exception:
                depth_plan = {}
            book_result = self.get_book_ticker(contract_symbol)
            if book_result.get('code') != 0:
                raise RuntimeError(str(book_result.get('msg') or f"BingX bookTicker error {book_result.get('code')}"))
            book = book_result.get('data') or {}
            ticker = book.get('book_ticker') if isinstance(book.get('book_ticker'), dict) else book
            fallback_price = ticker.get('askPrice') if side_normalized == 'buy' else ticker.get('bidPrice')
            if fallback_price in (None, '', '0'):
                fallback_price = ticker.get('ask_price') if side_normalized == 'buy' else ticker.get('bid_price')
            if fallback_price in (None, '', '0'):
                fallback_price = (
                    ticker.get('price')
                    or ticker.get('lastPrice')
                    or ticker.get('last_price')
                    or contract.get('lastPrice')
                    or contract.get('last_price')
                    or contract.get('markPrice')
                    or contract.get('mark_price')
                )
            if selected_price in (None, '', '0'):
                selected_price = fallback_price
            if selected_price in (None, '', '0'):
                raise RuntimeError(
                    'BingX best quote is unavailable for '
                    f"{contract_symbol} | book={json.dumps(book, ensure_ascii=False)}"
                    f" | contract={json.dumps(contract, ensure_ascii=False)}"
                )

        price_text = _decimal_text(selected_price, price_precision)
        if Decimal(price_text) <= 0:
            raise ValueError('BingX price must be greater than 0')

        prepared = {
            'symbol': contract_symbol,
            'side': 'BUY' if side_normalized == 'buy' else 'SELL',
            'price': price_text,
            'contract': contract,
            'bookTicker': book,
            'depthPlan': depth_plan,
            'qtyKind': qty_kind_normalized,
        }
        if qty_kind_normalized in ('usdt', 'quote', 'quote_usdt', 'notional'):
            quote_order_qty = _decimal_text(qty, max(2, quantity_precision))
            if Decimal(quote_order_qty) <= 0:
                raise ValueError('BingX quoteOrderQty must be greater than 0')
            prepared['quoteOrderQty'] = quote_order_qty
            return prepared

        quantity_text = _decimal_text(qty, quantity_precision)
        if Decimal(quantity_text) <= 0:
            raise ValueError('BingX quantity must be greater than 0')
        prepared['quantity'] = quantity_text
        return prepared

    def place_limit_order(
        self,
        prepared: Dict[str, Any],
        position_side: Optional[str] = 'BOTH',
        reduce_only: Optional[bool] = None,
        client_order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            'symbol': prepared['symbol'],
            'side': prepared['side'],
            'type': 'LIMIT',
            'price': prepared['price'],
            'timeInForce': 'GTC',
        }
        if prepared.get('quoteOrderQty') not in (None, ''):
            params['quoteOrderQty'] = prepared['quoteOrderQty']
        else:
            params['quantity'] = prepared['quantity']
        normalized_position_side = str(position_side or '').upper().strip()
        if normalized_position_side:
            params['positionSide'] = normalized_position_side
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


def _decimal_text(value: Any, precision: int, rounding=ROUND_DOWN) -> str:
    dec = Decimal(str(value))
    if precision >= 0:
        quantum = Decimal('1').scaleb(-precision)
        dec = dec.quantize(quantum, rounding=rounding)
    text = format(dec, 'f')
    if '.' in text:
        text = text.rstrip('0').rstrip('.')
    return text or '0'


def _extract_order_row(payload: Dict[str, Any]) -> Dict[str, Any]:
    data = (payload or {}).get('data') or {}
    if isinstance(data, dict):
        if isinstance(data.get('order'), dict):
            return data.get('order') or {}
        return data
    rows = _bingx_rows(payload)
    return rows[0] if rows else {}
