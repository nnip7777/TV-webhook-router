#!/usr/bin/env python3
"""
Умное размещение ордеров - Alor + Finam
Анализ стакана, лимитные ордера, разбивка заявок
"""
import asyncio
import httpx
import json
import os
import uuid
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, List, Union


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
    "refresh_token": None,  # Загрузим из файла
    "portfolio": "D95154",
    "client_id": "P095154"
}

FINAM_CONFIG = {
    "secret": FINAM_SECRET_PATH.read_text().strip() if FINAM_SECRET_PATH.exists() else None,
    "account_id": FINAM_ACCOUNT_ID,
    "token_id": None,
}

# Настройки умного размещения
SMART_ORDER_CONFIG = {
    "max_spread_bps": 50,  # Максимальный спред для лимитки (0.5%)
    "order_book_depth": 10,  # Глубина анализа стакана
    "min_liquidity_pct": 0.3,  # Мин. доля в стакане (30%)
    "split_threshold": 100000,  # Порог разбивки заявки (руб)
    "split_parts": 5,  # На сколько частей разбивать
}

# =============================================================================
# ALOR API
# =============================================================================

def _normalize_alor_exchange(exchange: str) -> str:
    raw = str(exchange or 'MOEX').strip().upper()
    if raw in ('FORTS', 'MOEX'): 
        return 'MOEX'
    return raw


class AlorClient:
    def __init__(self, refresh_token: str, portfolio: str):
        self.refresh_token = refresh_token
        self.portfolio = portfolio
        self.access_token = None
        self.token_expires = 0
    
    async def get_access_token(self) -> str:
        """Получить Access Token из Refresh Token"""
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
        """Получить стакан"""
        exchange = _normalize_alor_exchange(exchange)
        url = f"https://api.alor.ru/md/v2/orderbooks/{exchange}/{symbol}?depth={depth}"
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(url, headers=await self._headers())
            if resp.status_code == 200:
                return resp.json()
            _raise_http_error('Alor orderbook', resp)
    
    async def get_portfolio(self) -> Dict:
        """Получить портфель"""
        # TODO: найти правильный endpoint
        url = f"https://api.alor.ru/md/v2/Portfolios/{self.portfolio}/summary"
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(url, headers=await self._headers())
            if resp.status_code == 200:
                return resp.json()
            return None
    
    async def place_limit_order(self, symbol: str, exchange: str, side: str, 
                                 quantity: int, price: float, 
                                 comment: str = "") -> Dict:
        """Выставить лимитную заявку"""
        url = "https://api.alor.ru/commandapi/warptrans/TRADE/v2/client/orders/actions/limit"
        headers = await self._headers()
        headers["X-REQID"] = str(uuid.uuid4())
        
        exchange = _normalize_alor_exchange(exchange)
        body = {
            "side": side,  # "buy" или "sell"
            "type": "limit",
            "quantity": quantity,
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
    
    async def place_market_order(self, symbol: str, exchange: str, side: str,
                                  quantity: int, comment: str = "") -> Dict:
        """Выставить рыночную заявку"""
        url = "https://api.alor.ru/commandapi/warptrans/TRADE/v2/client/orders/actions/market"
        headers = await self._headers()
        headers["X-REQID"] = str(uuid.uuid4())
        
        exchange = _normalize_alor_exchange(exchange)
        body = {
            "side": side,
            "type": "market",
            "quantity": quantity,
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
            _raise_http_error('Alor market order', resp)

# =============================================================================
# FINAM API
# =============================================================================

def _normalize_finam_symbol(symbol: str, board: str) -> str:
    raw_symbol = str(symbol or '').strip()
    raw_board = str(board or '').strip()
    if not raw_symbol:
        return raw_symbol
    if '@' in raw_symbol or not raw_board:
        return raw_symbol
    return f"{raw_symbol}@{raw_board}"

class FinamClient:
    def __init__(self, secret: str, account_id: str):
        self.secret = secret
        self.account_id = account_id
        self.jwt_token = None
    
    async def get_jwt_token(self) -> str:
        """Получить JWT из secret"""
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
        """Получить стакан"""
        url = f"https://api.finam.ru/v1/orderbooks/{board}/{symbol}"
        async with httpx.AsyncClient(http2=True, timeout=30) as client:
            resp = await client.get(url, headers=await self._headers())
            if resp.status_code == 200:
                return resp.json()
            _raise_http_error('Finam orderbook', resp)
    
    async def get_account(self) -> Dict:
        """Получить информацию о счёте"""
        url = f"https://api.finam.ru/v1/accounts/{self.account_id}"
        async with httpx.AsyncClient(http2=True, timeout=30) as client:
            resp = await client.get(url, headers=await self._headers())
            if resp.status_code == 200:
                return resp.json()
            _raise_http_error('Finam account', resp)
    
    async def place_order(self, symbol: str, board: str, side: str,
                          quantity: int, price: Optional[float] = None,
                          order_type: str = "ORDER_TYPE_LIMIT") -> Dict:
        """Выставить заявку"""
        url = f"https://api.finam.ru/v1/accounts/{self.account_id}/orders"
        headers = await self._headers()
        headers["Content-Type"] = "application/json"
        symbol = _normalize_finam_symbol(symbol, board)

        body = {
            "symbol": symbol,
            "side": side,  # "SIDE_BUY" или "SIDE_SELL"
            "quantity": {"value": str(quantity)},
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
    
    def analyze_orderbook(self, symbol: str, orderbook: Dict, side: str) -> Dict:
        """Анализ стакана для определения оптимальной цены"""
        if self.broker == "alor":
            bids = orderbook.get('bids', [])
            asks = orderbook.get('asks', [])
        else:  # finam
            bids = orderbook.get('bids', [])
            asks = orderbook.get('asks', [])
        
        if side == "buy":
            # Для покупки: смотрим аски
            if not asks:
                return {"price": None, "liquidity": 0}
            
            best_ask = asks[0]['price']
            total_ask_volume = sum(a.get('volume', 0) for a in asks[:5])
            
            # Предлагаем цену на уровне лучшего аска или чуть выше
            return {
                "price": _round_alor_price(symbol, best_ask),
                "best_bid": bids[0]['price'] if bids else None,
                "best_ask": best_ask,
                "spread": best_ask - (bids[0]['price'] if bids else best_ask),
                "liquidity": total_ask_volume
            }
        else:  # sell
            # Для продажи: смотрим биды
            if not bids:
                return {"price": None, "liquidity": 0}
            
            best_bid = bids[0]['price']
            total_bid_volume = sum(b.get('volume', 0) for b in bids[:5])
            
            return {
                "price": _round_alor_price(symbol, best_bid),
                "best_bid": best_bid,
                "best_ask": asks[0]['price'] if asks else best_bid,
                "spread": (asks[0]['price'] if asks else best_bid) - best_bid,
                "liquidity": total_bid_volume
            }
    
    def calculate_smart_price(self, analysis: Dict, side: str) -> float:
        """Рассчитать цену лимитки по лучшему текущему уровню стакана."""
        if not analysis.get('price'):
            return None
        return analysis['price']
    
    def split_order(self, symbol: str, quantity: int, price: float, total_value: float) -> List[Dict]:
        """Разбить крупную заявку на части без изменения цены размещения."""
        normalized_price = _round_alor_price(symbol, price)
        if total_value < self.config['split_threshold']:
            return [{"quantity": quantity, "price": normalized_price}]
        
        parts = self.config['split_parts']
        qty_per_part = quantity // parts
        remainder = quantity % parts
        
        orders = []
        for i in range(parts):
            qty = qty_per_part + (1 if i < remainder else 0)
            orders.append({
                "quantity": qty,
                "price": normalized_price
            })
        
        return orders
    
    async def execute_smart_order(self, symbol: str, exchange: str, 
                                   side: str, quantity: int,
                                   use_limit: bool = True) -> List[Dict]:
        """Умное исполнение ордера"""
        print(f"\n{'='*60}")
        print(f"УМНОЕ РАЗМЕЩЕНИЕ: {side.upper()} {quantity} {symbol}")
        print(f"{'='*60}")
        
        # 1. Получаем стакан
        print("\n1. Получаем стакан...")
        if self.broker == "alor":
            orderbook = await self.client.get_orderbook(exchange, symbol)
        else:
            orderbook = await self.client.get_orderbook(exchange, symbol)
        
        # 2. Анализируем
        print("2. Анализируем стакан...")
        analysis = self.analyze_orderbook(symbol, orderbook, side)
        print(f"   Лучший бид: {analysis.get('best_bid')}")
        print(f"   Лучший аск: {analysis.get('best_ask')}")
        print(f"   Спред: {analysis.get('spread', 0):.4f}")
        print(f"   Ликвидность: {analysis.get('liquidity', 0)}")
        
        # 3. Рассчитываем цену
        if use_limit:
            price = self.calculate_smart_price(analysis, side)
            print(f"\n3. Лимитная цена: {price}")
        else:
            price = analysis['price']
            print(f"\n3. Рыночное исполнение по {price}")
        
        # 4. Разбиваем заявку если крупная
        total_value = quantity * price if price else 0
        orders_to_place = self.split_order(symbol, quantity, price, total_value)
        print(f"\n4. Заявок к размещению: {len(orders_to_place)}")
        
        # 5. Размещаем
        print("\n5. Размещаем заявки...")
        results = []
        for i, order in enumerate(orders_to_place):
            try:
                if self.broker == "alor":
                    result = await self.client.place_limit_order(
                        symbol=symbol,
                        exchange=exchange,
                        side=side,
                        quantity=order['quantity'],
                        price=order['price'],
                        comment=f"Smart order {i+1}/{len(orders_to_place)}"
                    )
                else:
                    result = await self.client.place_order(
                        symbol=symbol,
                        board=exchange,
                        side="SIDE_BUY" if side == "buy" else "SIDE_SELL",
                        quantity=order['quantity'],
                        price=order['price']
                    )
                
                results.append({
                    "status": "success",
                    "order": order,
                    "result": result
                })
                print(f"   ✅ Заявка {i+1}: {order['quantity']} @ {order['price']}")
                
            except Exception as e:
                results.append({
                    "status": "error",
                    "order": order,
                    "error": str(e)
                })
                print(f"   ❌ Заявка {i+1}: {e}")
        
        return results

# =============================================================================
# TRADINGVIEW WEBHOOK Handler
# =============================================================================

async def handle_tradingview_webhook(webhook_data: Dict, broker: str = "alor"):
    """Обработка вебхука от TradingView"""
    print(f"\n📡 TradingView Webhook: {webhook_data}")
    
    # Парсим сигнал
    ticker = webhook_data.get('ticker', 'SBER')
    side = webhook_data.get('side', 'buy').lower()
    quantity = int(webhook_data.get('quantity', 10))
    exchange = webhook_data.get('exchange', 'MOEX')
    use_limit = webhook_data.get('use_limit', True)
    
    # Инициализируем клиента
    if broker == "alor":
        with open(ALOR_CONFIG_PATH, 'r') as f:
            config = json.load(f)
        client = AlorClient(config['refresh_token'], config['portfolio'])
    else:
        if not FINAM_CONFIG['secret']:
            raise Exception("Finam secret not configured")
        client = FinamClient(FINAM_CONFIG['secret'], FINAM_CONFIG['account_id'])
    
    # Умное размещение
    executor = SmartOrderExecutor(client, broker)
    results = await executor.execute_smart_order(
        symbol=ticker,
        exchange=exchange,
        side=side,
        quantity=quantity,
        use_limit=use_limit
    )
    
    return results

# =============================================================================
# MAIN
# =============================================================================

async def main():
    print("="*60)
    print("УМНОЕ РАЗМЕЩЕНИЕ ОРДЕРОВ - Alor + Finam")
    print("="*60)
    
    # Загружаем конфиг Alor
    with open(ALOR_CONFIG_PATH, 'r') as f:
        alor_config = json.load(f)
    
    # Тест: получаем стакан Alor
    print("\n[TEST] Alor - стакан SBER...")
    alor_client = AlorClient(alor_config['refresh_token'], alor_config['portfolio'])
    try:
        orderbook = await alor_client.get_orderbook('MOEX', 'SBER')
        print(f"✅ Стакан получен!")
        print(f"   Бидов: {len(orderbook.get('bids', []))}")
        print(f"   Асков: {len(orderbook.get('asks', []))}")
        if orderbook.get('bids'):
            print(f"   Лучший бид: {orderbook['bids'][0]}")
        if orderbook.get('asks'):
            print(f"   Лучший аск: {orderbook['asks'][0]}")
    except Exception as e:
        print(f"❌ Ошибка: {e}")
    
    # Тест: Finam (если есть secret)
    if FINAM_CONFIG['secret']:
        print("\n[TEST] Finam - подключение...")
        finam_client = FinamClient(FINAM_CONFIG['secret'], FINAM_CONFIG['account_id'])
        try:
            account = await finam_client.get_account()
            print(f"✅ Подключено к счёту {FINAM_CONFIG['account_id']}")
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
        "side": "buy",
        "quantity": 100,
        "exchange": "MOEX",
        "use_limit": True
    }, indent=2))

if __name__ == "__main__":
    asyncio.run(main())

def _round_alor_price(symbol: str, price: float) -> float:
    """Normalize price to the expected Alor tick size for the symbol."""
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
