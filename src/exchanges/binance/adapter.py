import httpx
import json
import asyncio
import time
import websockets
import logging
from typing import List, AsyncGenerator, Dict, Any, Optional, Callable
from src.exchanges.base import BaseExchange
from src.models import (
    Ticker, BookTicker, MarkPrice, OrderBook, Candle, Trade, AggTrade,
    MarketType, SymbolInfo, OpenInterest, FundingRate, Liquidation, LongShortRatio
)

logger = logging.getLogger("binance_adapter")

CANDLE_INTERVALS          = ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"]
FUTURES_METRICS_INTERVALS = ["5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"]


class BinanceAdapter(BaseExchange):
    def __init__(self):
        super().__init__()
        self.http_client = httpx.AsyncClient(timeout=30.0)
        
        self._backoff_until = 0.0
        self._backoff_lock = asyncio.Lock()

        self.rest_urls = {
            MarketType.SPOT: "https://api.binance.com",
            MarketType.LINEAR: "https://fapi.binance.com",
            MarketType.INVERSE: "https://dapi.binance.com"
        }
        self.ws_urls = {
            MarketType.SPOT: "wss://stream.binance.com:9443/ws",
            MarketType.LINEAR: "wss://fstream.binance.com/ws",
            MarketType.INVERSE: "wss://dstream.binance.com/ws"
        }

    async def shutdown(self):
        await self.http_client.aclose()

    @property
    def name(self) -> str:
        return "binance"

    @property
    def supported_market_types(self) -> List[MarketType]:
        return [MarketType.SPOT, MarketType.LINEAR, MarketType.INVERSE]

    def get_capabilities(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "markets": {
                MarketType.SPOT: {
                    "candles":          {"rest": True,  "ws": False, "intervals": CANDLE_INTERVALS},
                    "ticker":           {"rest": True,  "ws": True},
                    "book_ticker":      {"rest": True,  "ws": True},
                    "trades":           {"rest": True,  "ws": True},
                    "agg_trades":       {"rest": True,  "ws": True},
                    "orderbook":        {"rest": True,  "ws": True},
                    "mark_price":       {"rest": False, "ws": False},
                    "open_interest":    {"rest": False, "ws": False},
                    "funding_rate":     {"rest": False, "ws": False},
                    "long_short_ratio": {"rest": False, "ws": False},
                    "liquidations":     {"rest": False, "ws": False},
                },
                MarketType.LINEAR: {
                    "candles":          {"rest": True, "ws": False, "intervals": CANDLE_INTERVALS},
                    "ticker":           {"rest": True, "ws": True},
                    "book_ticker":      {"rest": True, "ws": True},
                    "trades":           {"rest": True, "ws": True},
                    "agg_trades":       {"rest": True, "ws": True},
                    "orderbook":        {"rest": True, "ws": True},
                    "mark_price":       {"rest": True, "ws": True},
                    "open_interest":    {"rest": True, "ws": False, "intervals": FUTURES_METRICS_INTERVALS},
                    "funding_rate":     {"rest": True, "ws": False},
                    "long_short_ratio": {"rest": True, "ws": False, "intervals": FUTURES_METRICS_INTERVALS},
                    "liquidations":     {"rest": False, "ws": True},
                },
                MarketType.INVERSE: {
                    "candles":          {"rest": True, "ws": False, "intervals": CANDLE_INTERVALS},
                    "ticker":           {"rest": True, "ws": True},
                    "book_ticker":      {"rest": True, "ws": True},
                    "trades":           {"rest": True, "ws": True},
                    "agg_trades":       {"rest": True, "ws": True},
                    "orderbook":        {"rest": True, "ws": True},
                    "mark_price":       {"rest": True, "ws": True},
                    "open_interest":    {"rest": True, "ws": False, "intervals": FUTURES_METRICS_INTERVALS},
                    "funding_rate":     {"rest": True, "ws": False},
                    "long_short_ratio": {"rest": True, "ws": False, "intervals": FUTURES_METRICS_INTERVALS},
                    "liquidations":     {"rest": False, "ws": True},
                },
            }
        }

    def _get_rest_url(self, market_type: MarketType) -> str:
        if market_type not in self.rest_urls:
            raise ValueError(f"Market type {market_type} not supported by Binance adapter.")
        return self.rest_urls[market_type]

    def normalize_symbol(self, symbol: str) -> str:
        return symbol.replace("/", "").replace("-", "").upper()

    def get_api_symbol(self, symbol: str, market_type: MarketType) -> str:
        s = self.normalize_symbol(symbol)
        if market_type == MarketType.INVERSE and not s.endswith("_PERP"):
            return f"{s}_PERP"
        return s

    def get_model_symbol(self, api_symbol: str, market_type: MarketType) -> str:
        s = api_symbol.upper()
        if s.endswith("_PERP"):
            s = s[:-5]
        return s

    def get_stream_symbol(self, symbol: str, market_type: MarketType) -> str:
        return self.get_api_symbol(symbol, market_type).lower()

    def _interval_to_ms(self, interval: str) -> int:
        if not interval: return 0
        unit = interval[-1]
        try:
            val = int(interval[:-1])
        except (ValueError, TypeError):
            return 0
        
        if unit == 's': return val * 1000
        if unit == 'm': return val * 60 * 1000
        if unit == 'h': return val * 60 * 60 * 1000
        if unit == 'd': return val * 24 * 60 * 60 * 1000
        if unit == 'w': return val * 7 * 24 * 60 * 60 * 1000
        if unit == 'M': return val * 30 * 24 * 60 * 60 * 1000
        
        return 0

    async def _make_request(self, method: str, url: str, params: Optional[Dict] = None) -> Any:
        retries = 3
        while retries > 0:
            now = time.time()
            if now < self._backoff_until:
                wait_time = self._backoff_until - now + 0.1
                await asyncio.sleep(wait_time)

            try:
                resp = await self.http_client.request(method, url, params=params)
                
                used_weight = int(resp.headers.get("x-mbx-used-weight-1m", 0))

                if used_weight > 1150:
                    async with self._backoff_lock:
                        if time.time() > self._backoff_until:
                            self._backoff_until = time.time() + 2
                    logger.warning(f"High API Weight: {used_weight}/1200. Pausing all requests for 2s...")

                if resp.status_code == 200:
                    return resp.json()

                if resp.status_code in [429, 418]:
                    retry_after = int(resp.headers.get("Retry-After", 5))
                    
                    async with self._backoff_lock:
                        current_wait = max(self._backoff_until, time.time() + retry_after)
                        self._backoff_until = current_wait
                    
                    logger.error(f"Rate Limit Hit ({resp.status_code}). Global backoff set for {retry_after}s.")
                    retries -= 1
                    continue

                resp.raise_for_status()

            except httpx.HTTPStatusError as e:
                if e.response.status_code in [400, 404, 405]:
                    try:
                        err_msg = e.response.json().get("msg", "Unknown API Error")
                    except Exception:
                        err_msg = e.response.text
                    raise ValueError(f"Binance API Error ({e.response.status_code}): {err_msg}")
                else:
                    raise e

            except Exception as e:
                if isinstance(e, ValueError):
                    raise e
                logger.error(f"Request Error: {e}")
                retries -= 1
                await asyncio.sleep(1)

        raise Exception(f"Max retries exceeded for {url}")

    async def _paginate(self, fetch_func: Callable, start: Optional[int], total_limit: int, limit_per_req: int) -> List[Any]:
        results = []
        current_start = start
        max_requests = 50
        request_count = 0

        while len(results) < total_limit and request_count < max_requests:
            req_size = limit_per_req
            batch = await fetch_func(current_start, req_size)

            if not batch: break
            
            batch.sort(key=lambda x: x.timestamp)
            results.extend(batch)
            request_count += 1
            last_item = batch[-1]

            if hasattr(last_item, 'timestamp'):
                next_start = last_item.timestamp + 1
                if current_start and next_start <= current_start: break
                current_start = next_start
            else: break

        return results[:total_limit]

    async def _paginate_backwards(self, fetch_func_by_end: Callable, total_limit: int, limit_per_req: int) -> List[Any]:
        chunks = []
        collected_count = 0
        current_end = None
        max_requests = 50
        request_count = 0

        while collected_count < total_limit and request_count < max_requests:
            req_size = limit_per_req
            try:
                batch = await fetch_func_by_end(current_end, req_size)
            except (httpx.HTTPStatusError, ValueError):
                break

            if not batch: break
            
            batch.sort(key=lambda x: x.timestamp)
            chunks.append(batch)
            collected_count += len(batch)
            request_count += 1
            first_item = batch[0]

            if hasattr(first_item, 'timestamp'):
                current_end = first_item.timestamp - 1
            else:
                break

        chunks.reverse()
        final_results = [item for chunk in chunks for item in chunk]
        final_results.sort(key=lambda x: x.timestamp)
        return final_results[-total_limit:]

    async def _ws_connect(self, market_type: MarketType, params: list) -> AsyncGenerator[Dict, None]:
        url = self.ws_urls[market_type]
        reconnect_delay = 1

        while True:
            try:
                logger.info(f"Connecting to Binance ({market_type}) WS: {url}")
                async with websockets.connect(url) as ws:
                    logger.info(f"Binance ({market_type}) WS Connected.")
                    await ws.send(json.dumps({"method": "SUBSCRIBE", "params": params, "id": 1}))
                    
                    reconnect_delay = 1
                    
                    async for msg in ws:
                        data = json.loads(msg)
                        if "result" in data: continue
                        yield data
            
            except asyncio.CancelledError:
                logger.info(f"WS connection cancelled for {market_type}")
                break
            except Exception as e:
                logger.warning(f"WS Disconnected ({market_type}): {e}. Reconnecting in {reconnect_delay}s...")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 30)

    async def get_exchange_info(self, market_type: MarketType) -> List[SymbolInfo]:
        base_url = self._get_rest_url(market_type)
        endpoint = "/api/v3/exchangeInfo" if market_type == MarketType.SPOT else "/fapi/v1/exchangeInfo"
        if market_type == MarketType.INVERSE: endpoint = "/dapi/v1/exchangeInfo"

        data = await self._make_request("GET", f"{base_url}{endpoint}")
        results = []
        
        for s in data["symbols"]:
            status = s.get("status") or s.get("contractStatus")
            if status != "TRADING": continue
            if market_type != MarketType.SPOT and s.get("contractType") != "PERPETUAL": continue

            min_qty = 0.0
            max_qty = 0.0
            min_notional = 0.0

            for f in s.get("filters", []):
                if f["filterType"] == "LOT_SIZE":
                    min_qty, max_qty = float(f["minQty"]), float(f["maxQty"])
                if f["filterType"] in ["MIN_NOTIONAL", "NOTIONAL"]:
                    min_notional = float(f.get("minNotional", 0) or f.get("notional", 0))

            results.append(SymbolInfo(
                symbol=self.get_model_symbol(s["symbol"], market_type),
                native_symbol=s["symbol"],
                base_asset=s["baseAsset"],
                quote_asset=s["quoteAsset"],
                price_precision=s.get("quotePrecision", 8),
                quantity_precision=s.get("baseAssetPrecision", 8),
                min_qty=min_qty, max_qty=max_qty, min_notional=min_notional,
                status=status
            ))

        return results

    async def get_markets(self, market_type: MarketType) -> List[str]:
        info = await self.get_exchange_info(market_type)
        return [s.symbol for s in info]

    async def get_ticker(self, market_type: MarketType, symbol: str) -> Ticker:
        base_url = self._get_rest_url(market_type)
        api_symbol = self.get_api_symbol(symbol, market_type)
        
        endpoint = "/api/v3/ticker/24hr" if market_type == MarketType.SPOT else "/fapi/v1/ticker/24hr"
        if market_type == MarketType.INVERSE: endpoint = "/dapi/v1/ticker/24hr"

        data = await self._make_request("GET", f"{base_url}{endpoint}", params={"symbol": api_symbol})
        if isinstance(data, list): data = data[0]

        return Ticker(
            symbol=self.get_model_symbol(data["symbol"], market_type),
            price=float(data["lastPrice"]),
            open_24h=float(data["openPrice"]),
            high_24h=float(data["highPrice"]),
            low_24h=float(data["lowPrice"]),
            volume_24h=float(data["volume"]),
            quote_volume_24h=float(data.get("quoteVolume", 0)),
            price_change_percent=float(data["priceChangePercent"]),
            timestamp=self.normalize_timestamp(data["closeTime"])
        )

    async def get_book_ticker(self, market_type: MarketType, symbol: str) -> BookTicker:
        base_url = self._get_rest_url(market_type)
        api_symbol = self.get_api_symbol(symbol, market_type)
        
        endpoint = "/api/v3/ticker/bookTicker" if market_type == MarketType.SPOT else "/fapi/v1/ticker/bookTicker"
        if market_type == MarketType.INVERSE: endpoint = "/dapi/v1/ticker/bookTicker"

        data = await self._make_request("GET", f"{base_url}{endpoint}", params={"symbol": api_symbol})
        if isinstance(data, list): data = data[0]

        return BookTicker(
            symbol=self.get_model_symbol(data["symbol"], market_type),
            bid_price=float(data["bidPrice"]),
            bid_qty=float(data["bidQty"]),
            ask_price=float(data["askPrice"]),
            ask_qty=float(data["askQty"]),
            timestamp=self.normalize_timestamp(data.get("time") or int(time.time()*1000))
        )

    async def get_mark_price(self, market_type: MarketType, symbol: str) -> MarkPrice:
        if market_type == MarketType.SPOT: raise NotImplementedError("Mark Price not available for Spot markets.")
        base_url = self._get_rest_url(market_type)
        api_symbol = self.get_api_symbol(symbol, market_type)
        
        endpoint = "/fapi/v1/premiumIndex" if market_type == MarketType.LINEAR else "/dapi/v1/premiumIndex"

        data = await self._make_request("GET", f"{base_url}{endpoint}", params={"symbol": api_symbol})
        if isinstance(data, list): data = data[0]

        return MarkPrice(
            symbol=self.get_model_symbol(data["symbol"], market_type),
            mark_price=float(data["markPrice"]),
            index_price=float(data["indexPrice"]),
            funding_rate=float(data["lastFundingRate"]),
            next_funding_time=self.normalize_timestamp(data.get("nextFundingTime") or 0),
            timestamp=self.normalize_timestamp(data["time"])
        )

    async def get_orderbook(self, market_type: MarketType, symbol: str, depth: int = 20) -> OrderBook:
        base_url = self._get_rest_url(market_type)
        api_symbol = self.get_api_symbol(symbol, market_type)
        
        path = "/api/v3/depth" if market_type == MarketType.SPOT else "/fapi/v1/depth"
        if market_type == MarketType.INVERSE: path = "/dapi/v1/depth"

        data = await self._make_request("GET", f"{base_url}{path}", params={"symbol": api_symbol, "limit": depth})
        ts = data.get("E") or data.get("T") or int(time.time() * 1000)

        return OrderBook(
            symbol=self.get_model_symbol(api_symbol, market_type),
            bids=[[float(p), float(q)] for p, q in data["bids"]],
            asks=[[float(p), float(q)] for p, q in data["asks"]],
            timestamp=self.normalize_timestamp(ts)
        )

    async def get_trades(self, market_type: MarketType, symbol: str, limit: int = 100) -> List[Trade]:
        base_url = self._get_rest_url(market_type)
        api_symbol = self.get_api_symbol(symbol, market_type)
        
        path = "/api/v3/trades" if market_type == MarketType.SPOT else "/fapi/v1/trades"
        if market_type == MarketType.INVERSE: path = "/dapi/v1/trades"

        data = await self._make_request("GET", f"{base_url}{path}", params={"symbol": api_symbol, "limit": limit})

        return [Trade(
            id=str(t["id"]),
            price=float(t["price"]),
            qty=float(t["qty"]),
            side="sell" if t["isBuyerMaker"] else "buy",
            timestamp=self.normalize_timestamp(t["time"])
        ) for t in data]

    async def get_agg_trades(self, market_type: MarketType, symbol: str, start_time: Optional[int] = None, limit: int = 500) -> List[AggTrade]:
        base_url = self._get_rest_url(market_type)
        api_symbol = self.get_api_symbol(symbol, market_type)
        
        path = "/api/v3/aggTrades" if market_type == MarketType.SPOT else "/fapi/v1/aggTrades"
        if market_type == MarketType.INVERSE: path = "/dapi/v1/aggTrades"

        async def fetch_batch(s, l):
            p = {"symbol": api_symbol, "limit": l}
            if s: p["startTime"] = s
            data = await self._make_request("GET", f"{base_url}{path}", params=p)
            return [AggTrade(
                agg_id=str(t["a"]),
                price=float(t["p"]),
                qty=float(t["q"]),
                first_trade_id=str(t["f"]),
                last_trade_id=str(t["l"]),
                side="sell" if t["m"] else "buy",
                timestamp=self.normalize_timestamp(t["T"])
            ) for t in data]

        if start_time:
            return await self._paginate(fetch_batch, start_time, limit, 1000)
        return await fetch_batch(start_time, limit)

    async def get_candles(self, market_type: MarketType, symbol: str, interval: str, start_time: Optional[int] = None, limit: int = 100) -> List[Candle]:
        base_url = self._get_rest_url(market_type)
        api_symbol = self.get_api_symbol(symbol, market_type)
        
        path = "/api/v3/klines" if market_type == MarketType.SPOT else "/fapi/v1/klines"
        if market_type == MarketType.INVERSE: path = "/dapi/v1/klines"

        async def fetch_batch(s, l):
            p = {"symbol": api_symbol, "interval": interval, "limit": l}
            if s: p["startTime"] = s
            data = await self._make_request("GET", f"{base_url}{path}", params=p)
            return [Candle(
                timestamp=self.normalize_timestamp(k[0]),
                open=float(k[1]),
                high=float(k[2]),
                low=float(k[3]),
                close=float(k[4]),
                volume=float(k[5])
            ) for k in data]

        MAX_PER_REQ = 1500 if market_type != MarketType.SPOT else 1000
        current_start = start_time

        if limit > MAX_PER_REQ and current_start is None:
            interval_ms = self._interval_to_ms(interval)
            if interval_ms > 0:
                duration_ms = limit * interval_ms
                current_start = int(time.time() * 1000) - duration_ms - interval_ms

        if current_start is not None and current_start < 0:
            current_start = None

        if current_start or limit > MAX_PER_REQ:
            return await self._paginate(fetch_batch, current_start, limit, MAX_PER_REQ)
        else:
            return await fetch_batch(current_start, limit)

    async def get_open_interest(self, market_type: MarketType, symbol: str, period: str = "1h", start_time: Optional[int] = None, limit: int = 30) -> List[OpenInterest]:
        if market_type == MarketType.SPOT:
            raise NotImplementedError("Open Interest not available for Spot")

        base_url = self._get_rest_url(market_type)
        endpoint = "/futures/data/openInterestHist"
        
        raw_symbol = self.normalize_symbol(symbol).replace("_PERP", "")
        model_symbol = self.get_model_symbol(raw_symbol, market_type)

        sym_key = "symbol" if market_type == MarketType.LINEAR else "pair"

        if start_time:
            async def fetch_batch_forward(s, l):
                p = {sym_key: raw_symbol, "period": period, "limit": l, "startTime": s}
                if market_type == MarketType.INVERSE:
                    p["contractType"] = "PERPETUAL"
                data = await self._make_request("GET", f"{base_url}{endpoint}", params=p)
                return [OpenInterest(
                    symbol=model_symbol,
                    open_interest=float(i["sumOpenInterest"]),
                    value_usd=float(i["sumOpenInterestValue"]),
                    timestamp=self.normalize_timestamp(i["timestamp"])
                ) for i in data]

            return await self._paginate(fetch_batch_forward, start_time, limit, 500)

        async def fetch_batch_backward(end_ts, l):
            p = {sym_key: raw_symbol, "period": period, "limit": l}
            if end_ts: p["endTime"] = end_ts
            if market_type == MarketType.INVERSE:
                p["contractType"] = "PERPETUAL"
            data = await self._make_request("GET", f"{base_url}{endpoint}", params=p)
            return [OpenInterest(
                symbol=model_symbol,
                open_interest=float(i["sumOpenInterest"]),
                value_usd=float(i["sumOpenInterestValue"]),
                timestamp=self.normalize_timestamp(i["timestamp"])
            ) for i in data]

        return await self._paginate_backwards(fetch_batch_backward, limit, 500)

    async def get_long_short_ratio(self, market_type: MarketType, symbol: str, period: str = "5m", start_time: Optional[int] = None, limit: int = 30) -> List[LongShortRatio]:
        if market_type == MarketType.SPOT:
            raise NotImplementedError("L/S Ratio not available for Spot")

        base_url = self._get_rest_url(market_type)
        endpoint = "/futures/data/globalLongShortAccountRatio"
        
        raw_symbol = self.normalize_symbol(symbol).replace("_PERP", "")
        model_symbol = self.get_model_symbol(raw_symbol, market_type)

        sym_key = "symbol" if market_type == MarketType.LINEAR else "pair"

        if start_time:
            async def fetch_batch_forward(s, l):
                p = {"period": period, "limit": l, "startTime": s}
                p[sym_key] = raw_symbol
                data = await self._make_request("GET", f"{base_url}{endpoint}", params=p)
                return [LongShortRatio(
                    symbol=model_symbol,
                    ratio=float(i["longShortRatio"]),
                    long_account=float(i["longAccount"]),
                    short_account=float(i["shortAccount"]),
                    timestamp=self.normalize_timestamp(i["timestamp"])
                ) for i in data]

            return await self._paginate(fetch_batch_forward, start_time, limit, 500)

        async def fetch_batch_backward(end_ts, l):
            p = {"period": period, "limit": l}
            if end_ts: p["endTime"] = end_ts
            p[sym_key] = raw_symbol
            data = await self._make_request("GET", f"{base_url}{endpoint}", params=p)
            return [LongShortRatio(
                symbol=model_symbol,
                ratio=float(i["longShortRatio"]),
                long_account=float(i["longAccount"]),
                short_account=float(i["shortAccount"]),
                timestamp=self.normalize_timestamp(i["timestamp"])
            ) for i in data]

        return await self._paginate_backwards(fetch_batch_backward, limit, 500)

    async def get_funding_rate(self, market_type: MarketType, symbol: str, start_time: Optional[int] = None, limit: int = 100) -> List[FundingRate]:
        if market_type == MarketType.SPOT: raise NotImplementedError("Funding Rate not available for Spot")
        base_url = self._get_rest_url(market_type)
        endpoint = "/fapi/v1/fundingRate" if market_type == MarketType.LINEAR else "/dapi/v1/fundingRate"
        
        api_symbol = self.get_api_symbol(symbol, market_type)

        async def fetch_batch(s, l):
            p = {"symbol": api_symbol, "limit": l}
            if s: p["startTime"] = s
            data = await self._make_request("GET", f"{base_url}{endpoint}", params=p)
            return [FundingRate(
                symbol=i["symbol"],
                rate=float(i["fundingRate"]),
                timestamp=self.normalize_timestamp(i["fundingTime"])
            ) for i in data]

        MAX_PER_REQ = 1000
        current_start = start_time

        if limit > MAX_PER_REQ and current_start is None:
            interval_ms = 8 * 60 * 60 * 1000
            duration_ms = limit * interval_ms
            current_start = int(time.time() * 1000) - duration_ms - interval_ms

        if current_start is not None and current_start < 0:
            current_start = None

        if current_start or limit > MAX_PER_REQ:
            return await self._paginate(fetch_batch, current_start, limit, MAX_PER_REQ)
        else:
            return await fetch_batch(current_start, limit)

    async def get_liquidations(self, market_type: MarketType, symbol: str, start_time: Optional[int] = None, limit: int = 100) -> List[Liquidation]:
        if market_type == MarketType.SPOT: raise NotImplementedError("Liquidations not available for Spot")
        base_url = self._get_rest_url(market_type)
        endpoint = "/fapi/v1/allForceOrders" if market_type == MarketType.LINEAR else "/dapi/v1/allForceOrders"
        
        api_symbol = self.get_api_symbol(symbol, market_type)

        async def fetch_batch(s, l):
            p = {"symbol": api_symbol, "limit": l}
            if s: p["startTime"] = s
            try:
                data = await self._make_request("GET", f"{base_url}{endpoint}", params=p)
            except (httpx.HTTPStatusError, ValueError):
                return []
            return [Liquidation(
                symbol=self.get_model_symbol(i["symbol"], market_type),
                side=i["side"].lower(),
                price=float(i["price"]),
                qty=float(i["origQty"]),
                timestamp=self.normalize_timestamp(i["time"])
            ) for i in data]

        if start_time:
            return await self._paginate(fetch_batch, start_time, limit, 100)
        return await fetch_batch(start_time, limit)

    async def stream_ticker(self, market_type: MarketType, symbol: str) -> AsyncGenerator[Ticker, None]:
        s_norm = self.get_stream_symbol(symbol, market_type)
        async for data in self._ws_connect(market_type, [f"{s_norm}@ticker"]):
            yield Ticker(
                symbol=self.get_model_symbol(data["s"], market_type),
                price=float(data["c"]),
                open_24h=float(data["o"]),
                high_24h=float(data["h"]),
                low_24h=float(data["l"]),
                volume_24h=float(data["v"]),
                quote_volume_24h=float(data["q"]),
                price_change_percent=float(data["P"]),
                timestamp=self.normalize_timestamp(data["E"])
            )

    async def stream_book_ticker(self, market_type: MarketType, symbol: str) -> AsyncGenerator[BookTicker, None]:
        s_norm = self.get_stream_symbol(symbol, market_type)
        async for data in self._ws_connect(market_type, [f"{s_norm}@bookTicker"]):
            yield BookTicker(
                symbol=self.get_model_symbol(data["s"], market_type),
                bid_price=float(data["b"]),
                bid_qty=float(data["B"]),
                ask_price=float(data["a"]),
                ask_qty=float(data["A"]),
                timestamp=self.normalize_timestamp(data.get("T") or data.get("E") or int(time.time()*1000))
            )

    async def stream_mark_price(self, market_type: MarketType, symbol: str) -> AsyncGenerator[MarkPrice, None]:
        if market_type == MarketType.SPOT: raise NotImplementedError()
        s_norm = self.get_stream_symbol(symbol, market_type)
        async for data in self._ws_connect(market_type, [f"{s_norm}@markPrice"]):
            yield MarkPrice(
                symbol=self.get_model_symbol(data["s"], market_type),
                mark_price=float(data["p"]), 
                index_price=float(data["i"]),
                funding_rate=float(data["r"]), 
                next_funding_time=self.normalize_timestamp(data["T"]),
                timestamp=self.normalize_timestamp(data["E"])
            )

    async def stream_agg_trades(self, market_type: MarketType, symbol: str) -> AsyncGenerator[AggTrade, None]:
        s_norm = self.get_stream_symbol(symbol, market_type)
        async for data in self._ws_connect(market_type, [f"{s_norm}@aggTrade"]):
            yield AggTrade(
                agg_id=str(data["a"]), 
                price=float(data["p"]), 
                qty=float(data["q"]),
                first_trade_id=str(data["f"]), 
                last_trade_id=str(data["l"]),
                side="sell" if data["m"] else "buy", 
                timestamp=self.normalize_timestamp(data["T"])
            )

    async def stream_trades(self, market_type: MarketType, symbol: str) -> AsyncGenerator[Trade, None]:
        s_norm = self.get_stream_symbol(symbol, market_type)
        async for data in self._ws_connect(market_type, [f"{s_norm}@trade"]):
            yield Trade(
                id=str(data["t"]), 
                price=float(data["p"]), 
                qty=float(data["q"]),
                side="sell" if data["m"] else "buy", 
                timestamp=self.normalize_timestamp(data["T"])
            )

    async def stream_orderbook(self, market_type: MarketType, symbol: str, depth: int = 20, update_speed: str = "100ms") -> AsyncGenerator[OrderBook, None]:
        s_norm = self.get_stream_symbol(symbol, market_type)
        target_depth = depth if depth in [5, 10, 20] else 20
        if market_type == MarketType.SPOT:
            topic = f"{s_norm}@depth{target_depth}" if update_speed == "1000ms" else f"{s_norm}@depth{target_depth}@100ms"
        else:
            topic = f"{s_norm}@depth{target_depth}@{update_speed}" if update_speed in ["250ms", "500ms"] else f"{s_norm}@depth{target_depth}@100ms"
        async for data in self._ws_connect(market_type, [topic]):
            bids = data.get("bids") or data.get("b") or []
            asks = data.get("asks") or data.get("a") or []
            ts = data.get("E") or data.get("T") or int(time.time() * 1000)
            yield OrderBook(
                symbol=symbol.upper(), 
                bids=[[float(p), float(q)] for p, q in bids],
                asks=[[float(p), float(q)] for p, q in asks], 
                timestamp=self.normalize_timestamp(ts)
            )

    async def stream_liquidations(self, market_type: MarketType, symbol: str) -> AsyncGenerator[Liquidation, None]:
        if market_type == MarketType.SPOT: raise NotImplementedError("Liquidations not available for Spot")
        s_norm = self.get_stream_symbol(symbol, market_type)
        async for data in self._ws_connect(market_type, [f"{s_norm}@forceOrder"]):
            o = data.get("o", {})
            if not o: continue
            yield Liquidation(
                symbol=self.get_model_symbol(o["s"], market_type), 
                side=o["S"].lower(), 
                price=float(o["p"]), 
                qty=float(o["q"]),
                timestamp=self.normalize_timestamp(data["E"])
            )