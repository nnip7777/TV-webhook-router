#!/usr/bin/env python3
import asyncio
import importlib.util
from typing import Any, Dict, List

from bybit_adapter import BybitBroker
from finam_adapter import FinamBroker
from schwab_adapter import SchwabBroker
from settings import SMART_EXECUTOR_PATH


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
        client = BybitBroker(testnet=bool(destination.get('testnet', False)))

        if str(execution_mode).lower() == 'limit':
            price = destination.get('price') or payload.get('price')
            request_payload['price'] = price
            result = client.place_order(
                symbol=symbol,
                side=side,
                qty=quantity,
                category=category,
                order_type='Limit',
                price=price,
                reduce_only=reduce_only,
            )
        else:
            result = client.place_order(
                symbol=symbol,
                side=side,
                qty=quantity,
                category=category,
                order_type='Market',
                reduce_only=reduce_only,
            )

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


async def _execute_finam(payload: Dict[str, Any], destination: Dict[str, Any]) -> Dict[str, Any]:
    broker = destination['broker']
    symbol = destination['symbol']
    side = str(destination.get('side', payload['side'])).lower()
    quantity = destination.get('qty', payload['qty'])
    exchange = destination.get('exchange', 'MOEX')
    price = destination.get('price') or payload.get('price')
    dry_run = bool(destination.get('dryRun', True) or payload.get('dryRun', False))
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
    dry_run = bool(destination.get('dryRun', True) or payload.get('dryRun', False))
    request_payload = {
        'account': account_id,
        'symbol': symbol,
        'side': side,
        'qty': quantity,
        'dryRun': dry_run,
    }

    try:
        client = SchwabBroker()
        result = client.place_equity_order(account_id=account_id, symbol=symbol, side=side, qty=quantity, dry_run=dry_run)
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
    if broker == 'finam':
        return await _execute_finam(payload, destination)
    if broker == 'schwab':
        return await _execute_schwab(payload, destination)
    return await _execute_via_workspace_executor(payload, destination)


async def execute_route(payload: Dict[str, Any], route: Dict[str, Any]) -> Dict[str, Any]:
    destinations: List[Dict[str, Any]] = route.get('destinations', [])
    results = []

    for destination in destinations:
        results.append(await _execute_destination(payload, destination))

    return {
        'routeId': route.get('id'),
        'routeName': route.get('name'),
        'destinations': results,
    }


def execute_route_sync(payload: Dict[str, Any], route: Dict[str, Any]) -> Dict[str, Any]:
    return asyncio.run(execute_route(payload, route))
