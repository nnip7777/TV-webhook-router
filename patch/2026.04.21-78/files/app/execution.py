#!/usr/bin/env python3
import asyncio
import importlib.util
from typing import Any, Dict, List

from bingx_adapter import BingXBroker
from bybit_adapter import BybitBroker
from finam_adapter import FinamBroker
from schwab_adapter import SchwabBroker
from settings import SMART_EXECUTOR_PATH


DESTINATION_TIMEOUT_SECONDS = 25


def load_smart_executor_module():
    spec = importlib.util.spec_from_file_location('smart_order_executor', SMART_EXECUTOR_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


async def _execute_via_workspace_executor(payload: Dict[str, Any], destination: Dict[str, Any]) -> Dict[str, Any]:
    module = load_smart_executor_module()

    broker = destination['broker']
    symbol = destination['symbol']
    exchange = destination['exchange']
    side = str(destination.get('side', payload['side'])).lower()
    quantity = int(float(destination.get('qty', payload['qty'])))
    use_limit = str(destination.get('executionMode', 'maker')).lower() != 'market'
    request_payload = {
        'ticker': symbol,
        'side': side,
        'quantity': quantity,
        'exchange': exchange,
        'use_limit': use_limit,
    }

    try:
        results = await module.handle_tradingview_webhook(request_payload, broker=broker)
        return {
            'broker': broker,
            'symbol': symbol,
            'exchange': exchange,
            'qty': quantity,
            'request': request_payload,
            'results': results,
        }
    except Exception as e:
        return {
            'broker': broker,
            'symbol': symbol,
            'exchange': exchange,
            'qty': quantity,
            'request': request_payload,
            'error': str(e),
            'results': {'error': str(e)},
        }


async def _execute_bybit(payload: Dict[str, Any], destination: Dict[str, Any]) -> Dict[str, Any]:
    broker = destination['broker']
    symbol = destination['symbol']
    side = str(destination.get('side', payload['side'])).lower()
    quantity = destination.get('qty', payload['qty'])
    category = destination.get('category', 'linear')
    execution_mode = destination.get('executionMode', destination.get('mode', 'market'))
    reduce_only = bool(destination.get('reduceOnly', False))
    dry_run = bool(destination.get('dryRun', False) or payload.get('dryRun', False))
    request_payload = {
        'symbol': symbol,
        'side': side,
        'qty': quantity,
        'category': category,
        'executionMode': execution_mode,
        'reduceOnly': reduce_only,
    }

    if dry_run:
        return {
            'broker': broker,
            'symbol': symbol,
            'category': category,
            'dryRun': True,
            'request': request_payload,
            'wouldPlace': request_payload,
        }

    try:
        def _run() -> Dict[str, Any]:
            client = BybitBroker(testnet=bool(destination.get('testnet', False)))

            if str(execution_mode).lower() == 'limit':
                price = destination.get('price') or payload.get('price')
                request_payload['price'] = price
                return client.place_order(
                    symbol=symbol,
                    side=side,
                    qty=quantity,
                    category=category,
                    order_type='Limit',
                    price=price,
                    reduce_only=reduce_only,
                )
            return client.place_order(
                symbol=symbol,
                side=side,
                qty=quantity,
                category=category,
                order_type='Market',
                reduce_only=reduce_only,
            )

        result = await asyncio.to_thread(_run)

        return {
            'broker': broker,
            'symbol': symbol,
            'category': category,
            'qty': quantity,
            'request': request_payload,
            'results': result,
        }
    except Exception as e:
        return {
            'broker': broker,
            'symbol': symbol,
            'category': category,
            'qty': quantity,
            'request': request_payload,
            'error': str(e),
            'results': {'error': str(e)},
        }


async def _execute_bingx(payload: Dict[str, Any], destination: Dict[str, Any]) -> Dict[str, Any]:
    broker = destination['broker']
    symbol = destination['symbol']
    side = str(destination.get('side', payload['side'])).lower()
    quantity = destination.get('qty', payload['qty'])
    category = destination.get('category', 'swap')
    execution_mode = str(destination.get('executionMode', destination.get('mode', 'maker'))).lower()
    reduce_only = destination.get('reduceOnly') if 'reduceOnly' in destination else payload.get('reduceOnly')
    position_side = str(destination.get('positionSide', payload.get('positionSide', 'BOTH')) or 'BOTH').upper()
    dry_run = bool(destination.get('dryRun', False) or payload.get('dryRun', False))
    testnet = bool(destination.get('testnet', False))
    price = destination.get('price') or payload.get('price')
    client_order_id = destination.get('clientOrderId') or payload.get('clientOrderId')
    request_payload = {
        'symbol': symbol,
        'side': side,
        'qty': quantity,
        'category': category,
        'executionMode': execution_mode,
        'positionSide': position_side,
    }
    if reduce_only is not None:
        request_payload['reduceOnly'] = bool(reduce_only)
    if client_order_id:
        request_payload['clientOrderId'] = client_order_id
    if price not in (None, ''):
        request_payload['price'] = price

    if execution_mode == 'market':
        error = 'Only limit orders are supported by BingX broker'
        return {
            'broker': broker,
            'symbol': symbol,
            'category': category,
            'qty': quantity,
            'request': request_payload,
            'error': error,
            'results': {'error': error},
        }

    try:
        def _run() -> Dict[str, Any]:
            client = BingXBroker(testnet=testnet)
            prepared = client.prepare_limit_order(symbol=symbol, side=side, qty=quantity, price=price)
            request_payload['price'] = prepared['price']
            request_payload['symbol'] = prepared['symbol']
            request_payload['qty'] = prepared['quantity']
            request_payload['positionSide'] = position_side
            request_payload['bookTicker'] = prepared.get('bookTicker') or {}
            request_payload['contract'] = {
                'symbol': (prepared.get('contract') or {}).get('symbol'),
                'pricePrecision': (prepared.get('contract') or {}).get('pricePrecision'),
                'quantityPrecision': (prepared.get('contract') or {}).get('quantityPrecision'),
            }
            if dry_run:
                return {
                    'dryRun': True,
                    'wouldPlace': {
                        'symbol': prepared['symbol'],
                        'side': prepared['side'],
                        'type': 'LIMIT',
                        'quantity': prepared['quantity'],
                        'price': prepared['price'],
                        'timeInForce': 'GTC',
                        'positionSide': position_side,
                        'reduceOnly': None if reduce_only is None else bool(reduce_only),
                        'clientOrderId': client_order_id or '',
                    },
                    'bookTicker': prepared.get('bookTicker') or {},
                    'contract': prepared.get('contract') or {},
                }
            result = client.place_limit_order(
                prepared,
                position_side=position_side,
                reduce_only=bool(reduce_only) if reduce_only is not None else None,
                client_order_id=client_order_id,
            )
            message = str((result or {}).get('msg') or '')
            if position_side == 'BOTH' and 'Hedge mode' in message:
                retry_position_side = 'LONG' if side == 'buy' else 'SHORT'
                request_payload['positionSideRetry'] = retry_position_side
                result = client.place_limit_order(
                    prepared,
                    position_side=retry_position_side,
                    reduce_only=bool(reduce_only) if reduce_only is not None else None,
                    client_order_id=client_order_id,
                )
            return result

        result = await asyncio.to_thread(_run)
        business_error = ''
        if not dry_run and isinstance(result, dict) and result.get('code') not in (None, 0, '0'):
            business_error = str(result.get('msg') or result.get('code') or 'bingx_error')

        response_payload = {
            'broker': broker,
            'symbol': request_payload.get('symbol', symbol),
            'category': category,
            'qty': request_payload.get('qty', quantity),
            'request': request_payload,
            'results': result,
        }
        if dry_run:
            response_payload['dryRun'] = True
        if business_error:
            response_payload['error'] = business_error
        return response_payload
    except Exception as e:
        return {
            'broker': broker,
            'symbol': symbol,
            'category': category,
            'qty': quantity,
            'request': request_payload,
            'error': str(e),
            'results': {'error': str(e)},
        }


async def _execute_finam(payload: Dict[str, Any], destination: Dict[str, Any]) -> Dict[str, Any]:
    broker = destination['broker']
    symbol = destination['symbol']
    side = str(destination.get('side', payload['side'])).lower()
    quantity = destination.get('qty', payload['qty'])
    exchange = destination.get('exchange', 'MOEX')
    price = destination.get('price') or payload.get('price')
    dry_run = bool(destination.get('dryRun', False) or payload.get('dryRun', False))
    request_payload = {
        'symbol': symbol,
        'side': side,
        'qty': quantity,
        'exchange': exchange,
        'price': price,
        'dryRun': dry_run,
    }

    try:
        client = FinamBroker()
        result = await client.place_order(symbol=symbol, side=side, qty=quantity, exchange=exchange, price=price, dry_run=dry_run)
        return {
            'broker': broker,
            'symbol': symbol,
            'exchange': exchange,
            'qty': quantity,
            'request': request_payload,
            'results': result,
        }
    except Exception as e:
        return {
            'broker': broker,
            'symbol': symbol,
            'exchange': exchange,
            'qty': quantity,
            'request': request_payload,
            'error': str(e),
            'results': {'error': str(e)},
        }


async def _execute_schwab(payload: Dict[str, Any], destination: Dict[str, Any]) -> Dict[str, Any]:
    broker = destination['broker']
    symbol = destination['symbol']
    side = str(destination.get('side', payload['side'])).lower()
    quantity = destination.get('qty', payload['qty'])
    account_id = destination.get('account', 'primary')
    dry_run = bool(destination.get('dryRun', False) or payload.get('dryRun', False))
    request_payload = {
        'account': account_id,
        'symbol': symbol,
        'side': side,
        'qty': quantity,
        'dryRun': dry_run,
    }

    try:
        def _run() -> Dict[str, Any]:
            client = SchwabBroker()
            return client.place_equity_order(account_id=account_id, symbol=symbol, side=side, qty=quantity, dry_run=dry_run)

        result = await asyncio.to_thread(_run)
        return {
            'broker': broker,
            'symbol': symbol,
            'account': account_id,
            'qty': quantity,
            'request': request_payload,
            'results': result,
        }
    except Exception as e:
        return {
            'broker': broker,
            'symbol': symbol,
            'account': account_id,
            'qty': quantity,
            'request': request_payload,
            'error': str(e),
            'results': {'error': str(e)},
        }


async def _execute_destination(payload: Dict[str, Any], destination: Dict[str, Any]) -> Dict[str, Any]:
    broker = destination['broker']
    if broker == 'bybit':
        return await _execute_bybit(payload, destination)
    if broker == 'bingx':
        return await _execute_bingx(payload, destination)
    if broker == 'finam':
        return await _execute_finam(payload, destination)
    if broker == 'schwab':
        return await _execute_schwab(payload, destination)
    return await _execute_via_workspace_executor(payload, destination)


async def _execute_destination_with_timeout(payload: Dict[str, Any], destination: Dict[str, Any]) -> Dict[str, Any]:
    try:
        return await asyncio.wait_for(_execute_destination(payload, destination), timeout=DESTINATION_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        broker = destination.get('broker')
        symbol = destination.get('symbol')
        venue = destination.get('category') or destination.get('exchange') or destination.get('account')
        return {
            'broker': broker,
            'symbol': symbol,
            'category': destination.get('category'),
            'exchange': destination.get('exchange'),
            'account': destination.get('account'),
            'qty': destination.get('qty', payload.get('qty')),
            'request': {
                'symbol': symbol,
                'side': destination.get('side', payload.get('side')),
                'qty': destination.get('qty', payload.get('qty')),
                'venue': venue,
            },
            'error': f'timeout after {DESTINATION_TIMEOUT_SECONDS}s',
            'results': {'error': f'timeout after {DESTINATION_TIMEOUT_SECONDS}s'},
        }


async def execute_route(payload: Dict[str, Any], route: Dict[str, Any]) -> Dict[str, Any]:
    destinations: List[Dict[str, Any]] = route.get('destinations', [])
    results: List[Dict[str, Any]] = []
    for destination in destinations:
        results.append(await _execute_destination_with_timeout(payload, destination))

    return {
        'routeId': route.get('id'),
        'routeName': route.get('name'),
        'destinations': results,
    }


def execute_route_sync(payload: Dict[str, Any], route: Dict[str, Any]) -> Dict[str, Any]:
    return asyncio.run(execute_route(payload, route))
