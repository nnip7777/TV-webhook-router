#!/usr/bin/env python3
"""
Умное размещение ордеров - Alor + Finam
Анализ стакана, лимитные ордера, пошаговое исполнение one-order-at-a-time
"""
import asyncio
import httpx
import json
import os
import uuid
from pathlib import Path
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Optional, Dict, List, Union, Any


def _raise_http_error(stage: str, resp: httpx.Response) -> None:
    body_text = ''
    try:
        body_text = resp.text[:400]
    except Exception:
        body_text = ''
    raise Exception(f"{stage} failed: {resp.status_code} | body={body_text}")


PROJECT_ROOT = Path(__file__).resolve().parent.parent
ALOR_CONFIG_PATH = Path(os.getenv('ALOR_CONFIG_PATH') or (PROJECT_ROOT / 'broker' / 'alor' / 'config.json')).expanduser()
FINAM_SECRET_PATH = Path(os.getenv('FINAM_SECRET_PATH') or (PROJECT_ROOT / 'broker' / 'finam' / 'token.secret')).expanduser()
FINAM_ACCOUNT_ID = os.getenv('FINAM_ACCOUNT_ID', '1915526')
ALOR_ALLOW_MARGIN = str(os.getenv('ALOR_ALLOW_MARGIN', 'true')).strip().lower() in ('1', 'true', 'yes', 'on')

# =============================================================================
# КОНФИГУРАЦИЯ
# =============================================================================

ALOR_CONFIG = {
    "refresh_token": None,
    "portfolio": "D95154",
    "client_id": "P095154"
}

FINAM_CONFIG = {
    "secret": FINAM_SECRET_PATH.read_text().strip() if FINAM_SECRET_PATH.exists() else None,
    "account_id": FINAM_ACCOUNT_ID,
    "token_id": None,
}

SMART_ORDER_CONFIG = {
    "max_spread_bps": 50,
    "order_book_depth": 10,
    "min_liquidity_pct": 0.3,
    "max_single_level_pct": 1.0,
    "position_poll_attempts": 8,
    "position_poll_delay_seconds": 0.7,
    "max_slices_per_phase": 12,
}


# =============================================================================
# ВСПОМОГАТЕЛЬНОЕ
# =============================================================================

def _to_decimal(value: Any, default: str = '0') -> Decimal:
    try:
        if isinstance(value, dict):
            if 'value' in value:
                value = value.get('value')
            elif 'units' in value:
                value = value.get('units')
        if value in (None, ''):
            value = default
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal(default)


def _to_int(value: Any, default: int = 0) -> int:
    try:
        dec = _to_decimal(value, str(default))
        return int(dec.to_integral_value(rounding=ROUND_HALF_UP))
    except Exception:
        return default


def _normalize_side(side: str) -> str:
    raw = str(side or '').strip().lower()
    if raw in ('buy', 'long', '2long'):
        return 'buy'
    if raw in ('sell', 'short', '2short'):
        return 'sell'
    return raw


def _target_direction_from_side(side: str) -> str:
    raw = str(side or '').strip().lower()
    if raw in ('2long', 'long', 'buy'):
        return 'long'
    if raw in ('2short', 'short', 'sell'):
        return 'short'
    return ''


def _normalize_finam_symbol(symbol: str, board: str) -> str:
    raw_symbol = str(symbol or '').strip()
    raw_board = str(board or '').strip()
    if not raw_symbol:
        return raw_symbol
    if '@' in raw_symbol or not raw_board:
        return raw_symbol
    return f"{raw_symbol}@{raw_board}"


def _split_finam_symbol(symbol: str, board: str) -> tuple[str, str]:
    raw = str(symbol or '').strip()
    if '@' in raw:
        ticker, mic = raw.split('@', 1)
        return ticker, mic or board
    return raw, board


def _normalize_alor_exchange(exchange: str) -> str:
    raw = str(exchange or 'MOEX').strip().upper()
    if raw in ('FORTS', 'MOEX'):
        return 'MOEX'
    return raw


def _round_alor_price(symbol: str, price: float) -> float:
    tick_size = 0.01
    if symbol.startswith("NG"):
        tick_size = 0.001
    elif symbol.startswith("RTS"):
        tick_size = 0.5
    elif symbol.startswith("Si"):
        tick_size = 0.5

    rounded = round(price / tick_size) * tick_size
    if rounded <= 0:
        rounded = tick_size
    return rounded


def _round_price_for_broker(broker: str, symbol: str, price: Any) -> Optional[float]:
    if price in (None, ''):
        return None
    numeric = float(_to_decimal(price))
    if broker == 'alor':
        return _round_alor_price(symbol, numeric)
    return numeric


# =============================================================================
# ALOR API
# =============================================================================

class AlorClient:
    def __init__(self, refresh_token: str, portfolio: str, client_id: str):
        self.refresh_token = refresh_token
        self.portfolio = portfolio
        self.client_id = client_id
        self.access_token = None

    async def get_access_token(self) -> str:
        url = f"https://oauth.alor.ru/refresh?token={self.refresh_token}"
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(url)
            if resp.status_code == 200:
                data = resp.json()
                self.access_token = data.get('AccessToken')
                return self.access_token
            _raise_http_error('Alor auth', resp)

    async def _headers(self) -> Dict:
        if not self.access_token:
            await self.get_access_token()
        return {"Authorization": f"Bearer {self.access_token}"}

    async def get_orderbook(self, exchange: str, symbol: str, depth: int = 20) -> Dict:
        exchange = _normalize_alor_exchange(exchange)
        url = f"https://api.alor.ru/md/v2/orderbooks/{exchange}/{symbol}?depth={depth}"
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(url, headers=await self._headers())
            if resp.status_code == 200:
                return resp.json()
            _raise_http_error('Alor orderbook', resp)

    async def get_positions(self) -> List[Dict[str, Any]]:
        url = f"https://api.alor.ru/md/v2/clients/{self.client_id}/positions"
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(url, headers=await self._headers())
            if resp.status_code == 200:
                data = resp.json()
                return data if isinstance(data, list) else []
            _raise_http_error('Alor positions', resp)

    async def get_orders(self) -> List[Dict[str, Any]]:
        url = f"https://api.alor.ru/md/v2/clients/{self.client_id}/orders"
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(url, headers=await self._headers())
            if resp.status_code == 200:
                data = resp.json()
                return data if isinstance(data, list) else []
            _raise_http_error('Alor orders', resp)

    async def place_limit_order(self, symbol: str, exchange: str, side: str,
                                 quantity: int, price: float,
                                 comment: str = "") -> Dict:
        url = "https://api.alor.ru/commandapi/warptrans/TRADE/v2/client/orders/actions/limit"
        headers = await self._headers()
        headers["X-REQID"] = str(uuid.uuid4())

        exchange = _normalize_alor_exchange(exchange)
        body = {
            "side": side,
            "type": "limit",
            "quantity": int(quantity),
            "price": _round_alor_price(symbol, price),
            "instrument": {
                "symbol": symbol,
                "exchange": exchange
            },
            "user": {
                "portfolio": self.portfolio
            },
            "allowMargin": ALOR_ALLOW_MARGIN,
            "comment": comment
        }

        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(url, headers=headers, json=body)
            if resp.status_code == 200:
                return resp.json()
            _raise_http_error('Alor limit order', resp)


# =============================================================================
# FINAM API
# =============================================================================

class FinamClient:
    def __init__(self, secret: str, account_id: str):
        self.secret = secret
        self.account_id = account_id
        self.jwt_token = None

    async def get_jwt_token(self) -> str:
        url = "https://api.finam.ru/v1/sessions"
        headers = {"Content-Type": "application/json"}
        body = {"secret": self.secret}

        async with httpx.AsyncClient(http2=True, timeout=30) as client:
            resp = await client.post(url, headers=headers, json=body)
            if resp.status_code == 200:
                data = resp.json()
                self.jwt_token = data.get('token')
                return self.jwt_token
            _raise_http_error('Finam auth', resp)

    async def _headers(self) -> Dict:
        if not self.jwt_token:
            await self.get_jwt_token()
        return {"Authorization": f"Bearer {self.jwt_token}"}

    async def get_orderbook(self, board: str, symbol: str) -> Dict:
        ticker, mic = _split_finam_symbol(symbol, board)
        symbol_ref = f"{ticker}@{mic}" if mic else ticker
        url = f"https://api.finam.ru/v1/instruments/{symbol_ref}/orderbook"
        async with httpx.AsyncClient(http2=True, timeout=30) as client:
            resp = await client.get(url, headers=await self._headers())
            if resp.status_code == 200:
                return resp.json()
            _raise_http_error('Finam orderbook', resp)

    async def get_account(self) -> Dict:
        url = f"https://api.finam.ru/v1/accounts/{self.account_id}"
        async with httpx.AsyncClient(http2=True, timeout=30) as client:
            resp = await client.get(url, headers=await self._headers())
            if resp.status_code == 200:
                return resp.json()
            _raise_http_error('Finam account', resp)

    async def get_orders(self) -> List[Dict[str, Any]]:
        url = f"https://api.finam.ru/v1/accounts/{self.account_id}/orders"
        async with httpx.AsyncClient(http2=True, timeout=30) as client:
            resp = await client.get(url, headers=await self._headers())
            if resp.status_code == 200:
                data = resp.json()
                return list((data or {}).get('orders') or [])
            _raise_http_error('Finam orders', resp)

    async def place_order(self, symbol: str, board: str, side: str,
                          quantity: int, price: Optional[float] = None,
                          order_type: str = "ORDER_TYPE_LIMIT") -> Dict:
        url = f"https://api.finam.ru/v1/accounts/{self.account_id}/orders"
        headers = await self._headers()
        headers["Content-Type"] = "application/json"
        symbol = _normalize_finam_symbol(symbol, board)

        body = {
            "symbol": symbol,
            "side": side,
            "quantity": {"value": str(int(quantity))},
            "type": order_type,
            "timeInForce": "TIME_IN_FORCE_DAY",
        }

        if price is not None and order_type == "ORDER_TYPE_LIMIT":
            body["limitPrice"] = {"value": str(price)}

        async with httpx.AsyncClient(http2=True, timeout=30) as client:
            resp = await client.post(url, headers=headers, json=body)
            if resp.status_code == 200:
                return resp.json()
            _raise_http_error('Finam order', resp)


# =============================================================================
# УМНОЕ РАЗМЕЩЕНИЕ
# =============================================================================

class SmartOrderExecutor:
    def __init__(self, client: Union[AlorClient, FinamClient], broker: str):
        self.client = client
        self.broker = broker
        self.config = SMART_ORDER_CONFIG

    async def get_current_position_qty(self, symbol: str, exchange: str) -> int:
        if self.broker == 'alor':
            positions = await self.client.get_positions()
            for pos in positions:
                if str(pos.get('symbol') or '').strip().upper() == str(symbol).strip().upper():
                    return _to_int(pos.get('qty'))
            return 0

        account = await self.client.get_account()
        positions = list((account or {}).get('positions') or [])
        normalized_symbol = _normalize_finam_symbol(symbol, exchange).upper()
        short_symbol = str(symbol or '').strip().upper()
        for pos in positions:
            pos_symbol = str(pos.get('symbol') or '').strip().upper()
            if pos_symbol not in (normalized_symbol, short_symbol):
                continue
            return _to_int((pos.get('quantity') or {}).get('value'))
        return 0

    async def get_orders_snapshot(self) -> List[Dict[str, Any]]:
        try:
            return await self.client.get_orders()
        except Exception:
            return []

    def _extract_levels(self, orderbook: Dict[str, Any], side: str) -> List[Dict[str, Any]]:
        if self.broker == 'alor':
            levels = list(orderbook.get('asks' if side == 'buy' else 'bids', []) or [])
            return [{
                'price': _to_decimal(level.get('price')),
                'volume': abs(_to_decimal(level.get('volume'))),
            } for level in levels]

        book = orderbook or {}
        levels = list(book.get('asks' if side == 'buy' else 'bids', []) or [])
        normalized = []
        for level in levels:
            price = level.get('price') if isinstance(level, dict) else None
            volume = None
            if isinstance(level, dict):
                volume = level.get('volume') or level.get('quantity') or level.get('qty')
            normalized.append({
                'price': _to_decimal(price),
                'volume': abs(_to_decimal(volume)),
            })
        return normalized

    def analyze_orderbook(self, symbol: str, orderbook: Dict, side: str) -> Dict:
        bids = self._extract_levels(orderbook, 'sell')
        asks = self._extract_levels(orderbook, 'buy')
        best_bid = bids[0]['price'] if bids else Decimal('0')
        best_ask = asks[0]['price'] if asks else Decimal('0')

        if side == 'buy':
            active = asks
            price = best_ask
            liquidity = sum(level['volume'] for level in asks[:5])
        else:
            active = bids
            price = best_bid
            liquidity = sum(level['volume'] for level in bids[:5])

        return {
            'price': float(price) if price > 0 else None,
            'best_bid': float(best_bid) if best_bid > 0 else None,
            'best_ask': float(best_ask) if best_ask > 0 else None,
            'spread': float(best_ask - best_bid) if best_bid > 0 and best_ask > 0 else 0.0,
            'liquidity': float(liquidity),
            'levels': active,
        }

    def calculate_smart_price(self, analysis: Dict, side: str) -> Optional[float]:
        if not analysis.get('price'):
            return None
        return analysis['price']

    def choose_slice_qty(self, analysis: Dict[str, Any], remaining_qty: int) -> int:
        levels = list(analysis.get('levels') or [])
        if not levels:
            return max(1, int(remaining_qty))
        best_volume = _to_int(levels[0].get('volume'), 0)
        if best_volume <= 0:
            best_volume = remaining_qty
        return max(1, min(int(remaining_qty), best_volume))

    async def wait_for_position_change(self, symbol: str, exchange: str, before_qty: int, side: str, submitted_qty: int) -> Dict[str, Any]:
        desired_dir = 1 if side == 'buy' else -1
        attempts = int(self.config.get('position_poll_attempts', 8))
        delay = float(self.config.get('position_poll_delay_seconds', 0.7))
        last_qty = before_qty
        for _ in range(attempts):
            await asyncio.sleep(delay)
            current_qty = await self.get_current_position_qty(symbol, exchange)
            last_qty = current_qty
            delta = current_qty - before_qty
            if delta == 0:
                continue
            filled = abs(delta)
            if filled > submitted_qty:
                filled = submitted_qty
            if delta * desired_dir > 0 or before_qty == 0 or abs(current_qty) < abs(before_qty):
                return {
                    'filled_qty': int(filled),
                    'after_qty': int(current_qty),
                    'changed': True,
                }
        return {
            'filled_qty': max(0, min(submitted_qty, abs(last_qty - before_qty))),
            'after_qty': int(last_qty),
            'changed': last_qty != before_qty,
        }

    async def place_one_limit_order(self, symbol: str, exchange: str, side: str, quantity: int, comment: str = '') -> Dict[str, Any]:
        orderbook = await self.client.get_orderbook(exchange, symbol)
        analysis = self.analyze_orderbook(symbol, orderbook, side)
        order_price = self.calculate_smart_price(analysis, side)
        if order_price is None:
            return {
                'status': 'error',
                'error': 'no_price_in_orderbook',
                'analysis': analysis,
            }

        slice_qty = self.choose_slice_qty(analysis, quantity)
        rounded_price = _round_price_for_broker(self.broker, symbol, order_price)
        before_qty = await self.get_current_position_qty(symbol, exchange)
        orders_before = await self.get_orders_snapshot()

        if self.broker == 'alor':
            result = await self.client.place_limit_order(
                symbol=symbol,
                exchange=exchange,
                side=side,
                quantity=slice_qty,
                price=rounded_price,
                comment=comment or 'Smart order',
            )
        else:
            result = await self.client.place_order(
                symbol=symbol,
                board=exchange,
                side='SIDE_BUY' if side == 'buy' else 'SIDE_SELL',
                quantity=slice_qty,
                price=rounded_price,
                order_type='ORDER_TYPE_LIMIT',
            )

        position_wait = await self.wait_for_position_change(symbol, exchange, before_qty, side, slice_qty)
        orders_after = await self.get_orders_snapshot()
        return {
            'status': 'success',
            'order': {
                'quantity': slice_qty,
                'price': rounded_price,
                'side': side,
            },
            'analysis': analysis,
            'beforeQty': before_qty,
            'afterQty': position_wait.get('after_qty'),
            'filledQty': position_wait.get('filled_qty', 0),
            'positionChanged': bool(position_wait.get('changed')),
            'ordersBeforeCount': len(orders_before),
            'ordersAfterCount': len(orders_after),
            'result': result,
        }

    async def execute_sequential_limit_phase(self, symbol: str, exchange: str, side: str, quantity: int, phase: str) -> Dict[str, Any]:
        remaining = int(quantity)
        steps: List[Dict[str, Any]] = []
        slices_limit = int(self.config.get('max_slices_per_phase', 12))
        for step_no in range(1, slices_limit + 1):
            if remaining <= 0:
                break
            step = await self.place_one_limit_order(symbol, exchange, side, remaining, comment=f'{phase} #{step_no}')
            steps.append(step)
            if step.get('status') != 'success':
                break
            filled_qty = _to_int(step.get('filledQty'), 0)
            if filled_qty <= 0:
                break
            remaining = max(0, remaining - filled_qty)

        return {
            'phase': phase,
            'requestedQty': int(quantity),
            'remainingQty': int(remaining),
            'filledQty': int(quantity) - int(remaining),
            'steps': steps,
            'status': 'completed' if remaining <= 0 else ('partial' if steps else 'error'),
        }

    async def execute_target_direction(self, symbol: str, exchange: str, target_direction: str, open_qty: int) -> Dict[str, Any]:
        current_qty = await self.get_current_position_qty(symbol, exchange)
        close_phase = None
        open_phase = None
        decision = 'flat_open'

        if target_direction == 'long':
            if current_qty < 0:
                decision = 'close_short_then_open_long'
                close_phase = await self.execute_sequential_limit_phase(symbol, exchange, 'buy', abs(current_qty), 'close-opposite')
                current_qty = await self.get_current_position_qty(symbol, exchange)
                if current_qty < 0:
                    return {
                        'status': 'error',
                        'decision': decision,
                        'targetDirection': target_direction,
                        'beforeQty': current_qty,
                        'closePhase': close_phase,
                        'error': 'opposite_position_not_closed',
                    }
                open_phase = await self.execute_sequential_limit_phase(symbol, exchange, 'buy', open_qty, 'open-target')
            elif current_qty > 0:
                decision = 'add_long'
                open_phase = await self.execute_sequential_limit_phase(symbol, exchange, 'buy', open_qty, 'add-target')
            else:
                open_phase = await self.execute_sequential_limit_phase(symbol, exchange, 'buy', open_qty, 'open-target')
        elif target_direction == 'short':
            if current_qty > 0:
                decision = 'close_long_then_open_short'
                close_phase = await self.execute_sequential_limit_phase(symbol, exchange, 'sell', abs(current_qty), 'close-opposite')
                current_qty = await self.get_current_position_qty(symbol, exchange)
                if current_qty > 0:
                    return {
                        'status': 'error',
                        'decision': decision,
                        'targetDirection': target_direction,
                        'beforeQty': current_qty,
                        'closePhase': close_phase,
                        'error': 'opposite_position_not_closed',
                    }
                open_phase = await self.execute_sequential_limit_phase(symbol, exchange, 'sell', open_qty, 'open-target')
            elif current_qty < 0:
                decision = 'add_short'
                open_phase = await self.execute_sequential_limit_phase(symbol, exchange, 'sell', open_qty, 'add-target')
            else:
                open_phase = await self.execute_sequential_limit_phase(symbol, exchange, 'sell', open_qty, 'open-target')
        else:
            raise Exception(f'unsupported target direction: {target_direction}')

        after_qty = await self.get_current_position_qty(symbol, exchange)
        return {
            'status': 'success',
            'decision': decision,
            'targetDirection': target_direction,
            'beforeQty': current_qty,
            'afterQty': after_qty,
            'closePhase': close_phase,
            'openPhase': open_phase,
        }

    async def execute_smart_order(self, symbol: str, exchange: str,
                                   side: str, quantity: int,
                                   use_limit: bool = True,
                                   signal_mode: str = '',
                                   target_direction: str = '') -> Dict[str, Any]:
        print(f"\n{'='*60}")
        print(f"УМНОЕ РАЗМЕЩЕНИЕ: {side.upper()} {quantity} {symbol}")
        print(f"{'='*60}")

        if not use_limit:
            error_text = "Only limit orders are supported by smart_order_executor"
            return {"status": "error", "error": error_text}

        signal_mode = str(signal_mode or '').strip().lower()
        target_direction = str(target_direction or '').strip().lower() or _target_direction_from_side(side)
        normalized_side = _normalize_side(side)

        if signal_mode == 'target-direction' or target_direction in ('long', 'short'):
            return await self.execute_target_direction(symbol, exchange, target_direction, int(quantity))

        phase_result = await self.execute_sequential_limit_phase(symbol, exchange, normalized_side, int(quantity), 'single-signal')
        return {
            'status': 'success' if phase_result.get('filledQty', 0) > 0 else phase_result.get('status'),
            'decision': 'single-order',
            'phase': phase_result,
            'afterQty': await self.get_current_position_qty(symbol, exchange),
        }


# =============================================================================
# TRADINGVIEW WEBHOOK Handler
# =============================================================================

async def handle_tradingview_webhook(webhook_data: Dict, broker: str = "alor"):
    print(f"\n📡 TradingView Webhook: {webhook_data}")

    ticker = webhook_data.get('ticker', 'SBER')
    side = str(webhook_data.get('side', 'buy')).lower()
    quantity = int(float(webhook_data.get('quantity', 10)))
    exchange = webhook_data.get('exchange', 'MOEX')
    use_limit = webhook_data.get('use_limit', True)
    signal_mode = str(webhook_data.get('signalMode') or '').strip().lower()
    target_direction = str(webhook_data.get('targetDirection') or '').strip().lower()

    if broker == "alor":
        with open(ALOR_CONFIG_PATH, 'r') as f:
            config = json.load(f)
        client = AlorClient(config['refresh_token'], config['portfolio'], config.get('client_id', 'P095154'))
    else:
        if not FINAM_CONFIG['secret']:
            raise Exception("Finam secret not configured")
        client = FinamClient(FINAM_CONFIG['secret'], FINAM_CONFIG['account_id'])

    executor = SmartOrderExecutor(client, broker)
    results = await executor.execute_smart_order(
        symbol=ticker,
        exchange=exchange,
        side=side,
        quantity=quantity,
        use_limit=use_limit,
        signal_mode=signal_mode,
        target_direction=target_direction,
    )

    return results


# =============================================================================
# MAIN
# =============================================================================

async def main():
    print("="*60)
    print("УМНОЕ РАЗМЕЩЕНИЕ ОРДЕРОВ - Alor + Finam")
    print("="*60)

    with open(ALOR_CONFIG_PATH, 'r') as f:
        alor_config = json.load(f)

    print("\n[TEST] Alor - стакан SBER...")
    alor_client = AlorClient(alor_config['refresh_token'], alor_config['portfolio'], alor_config.get('client_id', 'P095154'))
    try:
        orderbook = await alor_client.get_orderbook('MOEX', 'SBER')
        print(f"✅ Стакан получен!")
        print(f"   Бидов: {len(orderbook.get('bids', []))}")
        print(f"   Асков: {len(orderbook.get('asks', []))}")
    except Exception as e:
        print(f"❌ Ошибка: {e}")

    if FINAM_CONFIG['secret']:
        print("\n[TEST] Finam - подключение...")
        finam_client = FinamClient(FINAM_CONFIG['secret'], FINAM_CONFIG['account_id'])
        try:
            account = await finam_client.get_account()
            print(f"✅ Подключено к счёту {FINAM_CONFIG['account_id']}")
            print(f"   Позиций: {len((account or {}).get('positions') or [])}")
        except Exception as e:
            print(f"❌ Ошибка: {e}")
    else:
        print("\n[TEST] Finam - secret не настроен, пропускаем")

    print("\n" + "="*60)
    print("ГОТОВО К РАБОТЕ")
    print("="*60)
    print("\nПример вебхука TradingView:")
    print(json.dumps({
        "ticker": "SBER",
        "side": "2long",
        "quantity": 1,
        "exchange": "MOEX",
        "signalMode": "target-direction",
        "targetDirection": "long",
        "use_limit": True
    }, indent=2))

if __name__ == "__main__":
    asyncio.run(main())
