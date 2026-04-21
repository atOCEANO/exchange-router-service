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

logger = logging.getLogger("bybit_adapter")

CANDLE_INTERVALS  = ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d", "1w", "1M"]
METRICS_INTERVALS = ["5m", "15m", "30m", "1h", "4h", "1d"]


class BybitAdapter(BaseExchange):
    def __init__(self):
        super().__init__()
        self.http_client = httpx.AsyncClient(timeout=30.0)
        
        self._backoff_until = 0.0
        self._backoff_lock = asyncio.Lock()
        
        self.rest_url = "https://api.bybit.com"
        self.ws_urls = {
            MarketType.SPOT: "wss://stream.bybit.com/v5/public/spot",
            MarketType.LINEAR: "wss://stream.bybit.com/v5/public/linear",
            MarketType.INVERSE: "wss://stream.bybit.com/v5/public/inverse"
        }

    async def shutdown(self):
        await self.http_client.aclose()

    def normalize_timestamp(self, ts: Any) -> int:
        try:
            val = int(float(ts))
            if val < 100_000_000_000:
                return val * 1000
            return val
        except (ValueError, TypeError):
            raise ValueError(f"Invalid Bybit timestamp: {ts}")

    @property
    def name(self) -> str:
        return "bybit"

    @property
    def supported_market_types(self) -> List[MarketType]:
        return [MarketType.SPOT, MarketType.LINEAR, MarketType.INVERSE]

    def normalize_symbol(self, symbol: str) -> str:
        return symbol.upper()

    def get_api_symbol(self, symbol: str, market_type: MarketType) -> str:
        s = self.normalize_symbol(symbol)
        if s.endswith("_PERP"):
            return s.replace("_PERP", "")
        return s

    def get_model_symbol(self, api_symbol: str, market_type: MarketType) -> str:
        s = api_symbol.upper()
        if s.endswith("_PERP"):
            s = s[:-5]
        return s

    def _get_category(self, market_type: MarketType) -> str:
        if market_type == MarketType.SPOT: return "spot"
        if market_type == MarketType.LINEAR: return "linear"
        if market_type == MarketType.INVERSE: return "inverse"
        raise ValueError(f"Unknown market type: {market_type}")

    def _interval_to_ms(self, interval: str) -> int:
        if not interval: return 0
        unit = interval[-1]
        try:
            val = int(interval[:-1])
        except (ValueError, TypeError):
            return 0
        
        if unit == 'm': return val * 60 * 1000
        if unit == 'h': return val * 60 * 60 * 1000
        if unit == 'd': return val * 24 * 60 * 60 * 1000
        if unit == 'w': return val * 7 * 24 * 60 * 60 * 1000
        if unit == 'M': return val * 30 * 24 * 60 * 60 * 1000
        
        return 0

    def _map_candle_interval(self, interval: str) -> str:
        if interval == "1d": return "D"
        if interval == "1w": return "W"
        if interval == "1M": return "M"
        if interval.endswith("m"): return interval[:-1]
        if interval.endswith("h"): return str(int(interval[:-1]) * 60)
        return interval

    def _map_metric_interval(self, interval: str) -> str:
        if interval.endswith("m"): return f"{interval[:-1]}min"
        return interval

    def get_capabilities(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "markets": {
                MarketType.SPOT: {
                    "candles":          {"rest": True,  "ws": False, "intervals": CANDLE_INTERVALS},
                    "ticker":           {"rest": True,  "ws": True},
                    "book_ticker":      {"rest": True,  "ws": True},
                    "trades":           {"rest": True,  "ws": True},
                    "agg_trades":       {"rest": False, "ws": False},
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
                    "agg_trades":       {"rest": False, "ws": False},
                    "orderbook":        {"rest": True, "ws": True},
                    "mark_price":       {"rest": True, "ws": False},
                    "open_interest":    {"rest": True, "ws": False, "intervals": METRICS_INTERVALS},
                    "funding_rate":     {"rest": True, "ws": False},
                    "long_short_ratio": {"rest": True, "ws": False, "intervals": METRICS_INTERVALS},
                    "liquidations":     {"rest": False, "ws": True},
                },
                MarketType.INVERSE: {
                    "candles":          {"rest": True, "ws": False, "intervals": CANDLE_INTERVALS},
                    "ticker":           {"rest": True, "ws": True},
                    "book_ticker":      {"rest": True, "ws": True},
                    "trades":           {"rest": True, "ws": True},
                    "agg_trades":       {"rest": False, "ws": False},
                    "orderbook":        {"rest": True, "ws": True},
                    "mark_price":       {"rest": True, "ws": False},
                    "open_interest":    {"rest": True, "ws": False, "intervals": METRICS_INTERVALS},
                    "funding_rate":     {"rest": True, "ws": False},
                    "long_short_ratio": {"rest": True, "ws": False, "intervals": METRICS_INTERVALS},
                    "liquidations":     {"rest": False, "ws": True},
                },
            }
        }

    async def _make_request(self, method: str, endpoint: str, params: Optional[Dict] = None) -> Any:
        url = f"{self.rest_url}{endpoint}"
        retries = 3
        while retries > 0:
            now = time.time()
            if now < self._backoff_until:
                wait_time = self._backoff_until - now
                if wait_time > 30:
                    raise ValueError(f"Bybit IP ban in effect. Retry in {wait_time:.0f}s.")
                await asyncio.sleep(wait_time + 0.1)

            try:
                resp = await self.http_client.request(method, url, params=params)

                remaining = int(resp.headers.get("X-Bapi-Limit-Status", 100))
                if remaining < 10:
                    reset_time_ms = int(resp.headers.get("X-Bapi-Limit-Reset-Timestamp", int(time.time() * 1000) + 5000))
                    reset_time = float(reset_time_ms) / 1000.0
                    async with self._backoff_lock:
                        self._backoff_until = max(self._backoff_until, reset_time)
                    logger.warning(f"Bybit rate limit low ({remaining} remaining). Pausing until reset.")

                if resp.status_code == 200:
                    data = resp.json()
                    if data["retCode"] == 10006:
                        async with self._backoff_lock:
                            self._backoff_until = max(self._backoff_until, time.time() + 2)
                        logger.warning(f"Bybit soft rate limit (10006). Retrying after 2s.")
                        retries -= 1
                        continue
                    if data["retCode"] != 0:
                        raise ValueError(f"Bybit API Error ({data['retCode']}): {data['retMsg']}")
                    return data["result"]

                if resp.status_code == 403:
                    ban_until = time.time() + 600
                    async with self._backoff_lock:
                        self._backoff_until = max(self._backoff_until, ban_until)
                    logger.error(f"Bybit IP ban (403). All requests blocked for 10 min.")
                    raise ValueError("Bybit IP ban triggered (403). Retry in 10 minutes.")

                if resp.status_code == 429:
                    async with self._backoff_lock:
                        self._backoff_until = max(self._backoff_until, time.time() + 5)
                    logger.error(f"Bybit rate limit (429). Backing off 5s.")
                    retries -= 1
                    continue

                resp.raise_for_status()

            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP Error: {e}")
                raise e
            except Exception as e:
                if isinstance(e, ValueError): raise e
                logger.error(f"Request Error: {e}")
                retries -= 1
                await asyncio.sleep(1)

        raise Exception(f"Max retries exceeded for {url}")

    async def _paginate_time(self, fetch_func: Callable, start: Optional[int], total_limit: int, limit_per_req: int) -> List[Any]:
        results = []
        seen_timestamps = set()
        current_start = start
        max_requests = 50
        req_count = 0
        last_ts_seen = -1

        while len(results) < total_limit and req_count < max_requests:
            try:
                batch = await fetch_func(current_start, limit_per_req)
            except (httpx.HTTPStatusError, ValueError) as e:
                logger.warning(f"Bybit _paginate_time: stopping early due to error: {e}")
                break
            if not batch: break
            
            batch.sort(key=lambda x: x.timestamp)
            
            new_items = []
            for item in batch:
                if item.timestamp not in seen_timestamps:
                    seen_timestamps.add(item.timestamp)
                    new_items.append(item)
            
            if not new_items: break

            results.extend(new_items)
            req_count += 1
            
            last_item = batch[-1]
            if hasattr(last_item, 'timestamp'):
                if len(batch) < limit_per_req: break
                if last_item.timestamp <= last_ts_seen: break
                last_ts_seen = last_item.timestamp
                current_start = last_item.timestamp + 1
            else:
                break
        
        return sorted(results, key=lambda x: x.timestamp)[:total_limit]

    async def _paginate_backwards(self, fetch_func_by_end: Callable, total_limit: int, limit_per_req: int) -> List[Any]:
        chunks = []
        collected_count = 0
        current_end = int(time.time() * 1000)
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

    async def _paginate_cursor(self, fetch_func_cursor: Callable, total_limit: int, limit_per_req: int) -> List[Any]:
        results = []
        seen_timestamps = set()
        cursor = None
        max_requests = 50
        req_count = 0

        while len(results) < total_limit and req_count < max_requests:
            batch, next_cursor = await fetch_func_cursor(cursor, limit_per_req)
            if not batch: break
            
            new_items = []
            for item in batch:
                if item.timestamp not in seen_timestamps:
                    seen_timestamps.add(item.timestamp)
                    new_items.append(item)

            results.extend(new_items)
            req_count += 1
            
            if not next_cursor or next_cursor == cursor: break
            cursor = next_cursor

        return sorted(results, key=lambda x: x.timestamp)[:total_limit]

    async def _ws_connect(self, market_type: MarketType, topics: list) -> AsyncGenerator[Dict, None]:
        url = self.ws_urls[market_type]
        reconnect_delay = 1
        
        while True:
            try:
                async with websockets.connect(url) as ws:
                    logger.info(f"WS Connected: {market_type} - {topics}")
                    req_id = f"sub-{int(time.time())}"
                    await ws.send(json.dumps({"op": "subscribe", "args": topics, "req_id": req_id}))
                    last_ping = time.time()
                    reconnect_delay = 1

                    async for msg in ws:
                        if time.time() - last_ping > 20:
                            await ws.send(json.dumps({"op": "ping"}))
                            last_ping = time.time()

                        try:
                            data = json.loads(msg)
                            if data.get("op") == "subscribe" and not data.get("success"):
                                logger.error(f"Sub failed: {data}")
                            if "topic" in data:
                                yield data
                        except json.JSONDecodeError: pass
                            
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"WS Disconnected ({e}). Reconnecting in {reconnect_delay}s...")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 30)

    async def get_exchange_info(self, market_type: MarketType) -> List[SymbolInfo]:
        cat = self._get_category(market_type)
        params: dict = {"category": cat, "limit": 1000}
        if market_type == MarketType.LINEAR:
            params["contractType"] = "LinearPerpetual"
        elif market_type == MarketType.INVERSE:
            params["contractType"] = "InversePerpetual"
        data = await self._make_request("GET", "/v5/market/instruments-info", params)
        results = []
        for s in data["list"]:
            if s["status"] != "Trading": continue
            if market_type == MarketType.LINEAR and s.get("contractType") != "LinearPerpetual": continue
            if market_type == MarketType.INVERSE and s.get("contractType") != "InversePerpetual": continue
            min_qty, max_qty, min_notional = 0.0, 0.0, 0.0
            if "lotSizeFilter" in s:
                min_qty = float(s["lotSizeFilter"].get("minOrderQty", 0))
                max_qty = float(s["lotSizeFilter"].get("maxOrderQty", 0))
                min_notional = float(s["lotSizeFilter"].get("minNotionalValue", 0))
            results.append(SymbolInfo(
                symbol=s["symbol"],
                native_symbol=s["symbol"],
                base_asset=s["baseCoin"],
                quote_asset=s["quoteCoin"],
                price_precision=int(len(s.get("priceFilter", {}).get("tickSize", "0.01").split(".")[-1])),
                quantity_precision=int(len(s.get("lotSizeFilter", {}).get("qtyStep", "0.001").split(".")[-1])),
                min_qty=min_qty, max_qty=max_qty, min_notional=min_notional,
                status=s["status"]
            ))
        return results

    async def get_markets(self, market_type: MarketType) -> List[str]:
        info = await self.get_exchange_info(market_type)
        return [s.symbol for s in info]

    async def get_ticker(self, market_type: MarketType, symbol: str) -> Ticker:
        cat = self._get_category(market_type)
        api_symbol = self.get_api_symbol(symbol, market_type)
        
        data = await self._make_request("GET", "/v5/market/tickers", {"category": cat, "symbol": api_symbol})
        if not data["list"]: raise ValueError(f"Symbol {api_symbol} not found")
        t = data["list"][0]
        
        return Ticker(
            symbol=self.get_model_symbol(t["symbol"], market_type),
            price=float(t["lastPrice"]),
            open_24h=float(t["prevPrice24h"]),
            high_24h=float(t["highPrice24h"]),
            low_24h=float(t["lowPrice24h"]),
            volume_24h=float(t["volume24h"]),
            quote_volume_24h=float(t["turnover24h"]),
            price_change_percent=float(t["price24hPcnt"]) * 100,
            timestamp=self.normalize_timestamp(t.get("ts") or int(time.time() * 1000))
        )

    async def get_book_ticker(self, market_type: MarketType, symbol: str) -> BookTicker:
        cat = self._get_category(market_type)
        api_symbol = self.get_api_symbol(symbol, market_type)
        
        data = await self._make_request("GET", "/v5/market/tickers", {"category": cat, "symbol": api_symbol})
        t = data["list"][0]
        return BookTicker(
            symbol=self.get_model_symbol(t["symbol"], market_type),
            bid_price=float(t.get("bid1Price", 0)),
            bid_qty=float(t.get("bid1Size", 0)),
            ask_price=float(t.get("ask1Price", 0)),
            ask_qty=float(t.get("ask1Size", 0)),
            timestamp=int(time.time() * 1000)
        )

    async def get_mark_price(self, market_type: MarketType, symbol: str) -> MarkPrice:
        if market_type == MarketType.SPOT: raise NotImplementedError("Spot has no mark price")
        cat = self._get_category(market_type)
        api_symbol = self.get_api_symbol(symbol, market_type)
        
        data = await self._make_request("GET", "/v5/market/tickers", {"category": cat, "symbol": api_symbol})
        t = data["list"][0]
        return MarkPrice(
            symbol=self.get_model_symbol(t["symbol"], market_type),
            mark_price=float(t["markPrice"]),
            index_price=float(t["indexPrice"]),
            funding_rate=float(t.get("fundingRate", 0)),
            next_funding_time=int(t.get("nextFundingTime", 0)),
            timestamp=self.normalize_timestamp(t.get("ts") or int(time.time() * 1000))
        )

    async def get_orderbook(self, market_type: MarketType, symbol: str, depth: int = 20) -> OrderBook:
        cat = self._get_category(market_type)
        api_symbol = self.get_api_symbol(symbol, market_type)
        
        valid_limit = 50
        if depth <= 1: valid_limit = 1
        elif depth <= 50: valid_limit = 50
        elif depth <= 200: valid_limit = 200
        elif market_type != MarketType.SPOT and depth <= 500: valid_limit = 500
        
        data = await self._make_request("GET", "/v5/market/orderbook", {"category": cat, "symbol": api_symbol, "limit": valid_limit})
        return OrderBook(
            symbol=self.get_model_symbol(data["s"], market_type),
            bids=[[float(p), float(q)] for p, q in data["b"]],
            asks=[[float(p), float(q)] for p, q in data["a"]],
            timestamp=self.normalize_timestamp(data["ts"])
        )

    async def get_trades(self, market_type: MarketType, symbol: str, limit: int = 100) -> List[Trade]:
        cat = self._get_category(market_type)
        api_symbol = self.get_api_symbol(symbol, market_type)
        
        req_limit = min(limit, 60 if market_type == MarketType.SPOT else 1000)
        data = await self._make_request("GET", "/v5/market/recent-trade", {"category": cat, "symbol": api_symbol, "limit": req_limit})
        return [Trade(
            id=t["execId"],
            price=float(t["price"]),
            qty=float(t["size"]),
            side=t["side"].lower(),
            timestamp=self.normalize_timestamp(t["time"])
        ) for t in data["list"]]

    async def get_agg_trades(self, market_type: MarketType, symbol: str, start_time: Optional[int] = None, limit: int = 500) -> List[AggTrade]:
        trades = await self.get_trades(market_type, symbol, limit)
        return [AggTrade(
            agg_id=t.id, price=t.price, qty=t.qty, first_trade_id=t.id, last_trade_id=t.id,
            side=t.side, timestamp=t.timestamp
        ) for t in trades]

    async def get_candles(self, market_type: MarketType, symbol: str, interval: str, start_time: Optional[int] = None, limit: int = 100) -> List[Candle]:
        cat = self._get_category(market_type)
        api_symbol = self.get_api_symbol(symbol, market_type)
        
        bybit_interval = self._map_candle_interval(interval)

        MAX_PER_REQ = 1000
        
        calc_start_time = start_time
        if calc_start_time is None and limit > MAX_PER_REQ:
             ms = self._interval_to_ms(interval)
             if ms > 0:
                 now_ms = int(time.time() * 1000)
                 duration_ms = limit * ms
                 raw_start = now_ms - duration_ms
                 
                 if interval.endswith("m") or interval.endswith("h"):
                     calc_start_time = raw_start - (raw_start % ms)
                 else:
                     calc_start_time = raw_start

        async def fetch(s, l):
            p = {"category": cat, "symbol": api_symbol, "interval": bybit_interval, "limit": min(l, MAX_PER_REQ)}
            if s: p["start"] = s
            data = await self._make_request("GET", "/v5/market/kline", p)
            if not data["list"]: return []
            
            parsed = [Candle(
                timestamp=self.normalize_timestamp(k[0]),
                open=float(k[1]), high=float(k[2]), low=float(k[3]), close=float(k[4]), volume=float(k[5])
            ) for k in data["list"]]
            
            return sorted(parsed, key=lambda x: x.timestamp)

        return await self._paginate_time(fetch, calc_start_time, limit, MAX_PER_REQ)

    async def get_open_interest(self, market_type: MarketType, symbol: str, period: str = "1h", start_time: Optional[int] = None, limit: int = 30) -> List[OpenInterest]:
        if market_type == MarketType.SPOT: raise NotImplementedError()
        cat = self._get_category(market_type)
        api_symbol = self.get_api_symbol(symbol, market_type)
        
        interval = self._map_metric_interval(period)

        if start_time:
            async def fetch_cursor(cursor, l):
                p = {"category": cat, "symbol": api_symbol, "intervalTime": interval, "limit": min(l, 200)}
                if cursor: p["cursor"] = cursor
                if start_time and not cursor: p["startTime"] = start_time
                
                data = await self._make_request("GET", "/v5/market/open-interest", p)
                
                res_list = [OpenInterest(
                    symbol=self.get_model_symbol(api_symbol, market_type),
                    open_interest=float(i["openInterest"]),
                    value_usd=0.0,
                    timestamp=self.normalize_timestamp(i["timestamp"])
                ) for i in data["list"]]
                
                return sorted(res_list, key=lambda x: x.timestamp), data.get("nextPageCursor")
            return await self._paginate_cursor(fetch_cursor, limit, 200)

        async def fetch_backwards(end_ts, l):
            p = {"category": cat, "symbol": api_symbol, "intervalTime": interval, "limit": min(l, 200)}
            if end_ts: p["endTime"] = end_ts
            
            data = await self._make_request("GET", "/v5/market/open-interest", p)
            
            res_list = [OpenInterest(
                symbol=self.get_model_symbol(api_symbol, market_type),
                open_interest=float(i["openInterest"]),
                value_usd=0.0,
                timestamp=self.normalize_timestamp(i["timestamp"])
            ) for i in data["list"]]
            
            return sorted(res_list, key=lambda x: x.timestamp)

        return await self._paginate_backwards(fetch_backwards, limit, 200)

    async def get_funding_rate(self, market_type: MarketType, symbol: str, start_time: Optional[int] = None, limit: int = 100) -> List[FundingRate]:
        if market_type == MarketType.SPOT: raise NotImplementedError()
        cat = self._get_category(market_type)
        api_symbol = self.get_api_symbol(symbol, market_type)

        if start_time:
            async def fetch(s, l):
                p = {"category": cat, "symbol": api_symbol, "limit": min(l, 200)}
                if s:
                    p["startTime"] = s
                    p["endTime"] = int(time.time() * 1000)
                data = await self._make_request("GET", "/v5/market/funding/history", p)

                parsed = [FundingRate(
                    symbol=self.get_model_symbol(api_symbol, market_type),
                    rate=float(i["fundingRate"]),
                    timestamp=self.normalize_timestamp(i["fundingRateTimestamp"])
                ) for i in data.get("list", [])]
                return sorted(parsed, key=lambda x: x.timestamp)
            return await self._paginate_time(fetch, start_time, limit, 200)

        async def fetch_backwards(end_ts, l):
            p = {"category": cat, "symbol": api_symbol, "limit": min(l, 200)}
            if end_ts: p["endTime"] = end_ts
            data = await self._make_request("GET", "/v5/market/funding/history", p)

            parsed = [FundingRate(
                symbol=self.get_model_symbol(api_symbol, market_type),
                rate=float(i["fundingRate"]),
                timestamp=self.normalize_timestamp(i["fundingRateTimestamp"])
            ) for i in data.get("list", [])]
            return sorted(parsed, key=lambda x: x.timestamp)

        return await self._paginate_backwards(fetch_backwards, limit, 200)

    async def get_long_short_ratio(self, market_type: MarketType, symbol: str, period: str = "5m", start_time: Optional[int] = None, limit: int = 30) -> List[LongShortRatio]:
        if market_type == MarketType.SPOT: raise NotImplementedError()
        cat = self._get_category(market_type)
        api_symbol = self.get_api_symbol(symbol, market_type)
        interval = self._map_metric_interval(period)
        
        req_limit = min(limit, 500)
        p = {"category": cat, "symbol": api_symbol, "period": interval, "limit": req_limit}
        
        data = await self._make_request("GET", "/v5/market/account-ratio", p)
        
        results = [LongShortRatio(
            symbol=self.get_model_symbol(api_symbol, market_type),
            ratio=float(i["buyRatio"]) / float(i["sellRatio"]) if float(i["sellRatio"]) > 0 else 0,
            long_account=float(i["buyRatio"]),
            short_account=float(i["sellRatio"]),
            timestamp=self.normalize_timestamp(i["timestamp"])
        ) for i in data["list"]]
        
        return sorted(results, key=lambda x: x.timestamp)

    async def stream_ticker(self, market_type: MarketType, symbol: str) -> AsyncGenerator[Ticker, None]:
        api_symbol = self.get_api_symbol(symbol, market_type)
        topic = f"tickers.{api_symbol}"
        async for msg in self._ws_connect(market_type, [topic]):
            d = msg["data"]
            yield Ticker(
                symbol=self.get_model_symbol(d.get("symbol", api_symbol), market_type),
                price=float(d.get("lastPrice", 0)),
                open_24h=float(d.get("prevPrice24h", 0)),
                high_24h=float(d.get("highPrice24h", 0)),
                low_24h=float(d.get("lowPrice24h", 0)),
                volume_24h=float(d.get("volume24h", 0)),
                quote_volume_24h=float(d.get("turnover24h", 0)),
                price_change_percent=float(d.get("price24hPcnt", 0)) * 100,
                timestamp=self.normalize_timestamp(msg.get("ts", time.time()))
            )

    async def stream_book_ticker(self, market_type: MarketType, symbol: str) -> AsyncGenerator[BookTicker, None]:
        api_symbol = self.get_api_symbol(symbol, market_type)
        topic = f"orderbook.1.{api_symbol}"
        async for msg in self._ws_connect(market_type, [topic]):
            d = msg["data"]
            bids = d.get("b", [])
            asks = d.get("a", [])
            if not bids or not asks: continue
            yield BookTicker(
                symbol=self.get_model_symbol(d.get("s", api_symbol), market_type),
                bid_price=float(bids[0][0]),
                bid_qty=float(bids[0][1]),
                ask_price=float(asks[0][0]),
                ask_qty=float(asks[0][1]),
                timestamp=self.normalize_timestamp(msg.get("ts"))
            )

    async def stream_trades(self, market_type: MarketType, symbol: str) -> AsyncGenerator[Trade, None]:
        api_symbol = self.get_api_symbol(symbol, market_type)
        topic = f"publicTrade.{api_symbol}"
        async for msg in self._ws_connect(market_type, [topic]):
            for t in msg.get("data", []):
                yield Trade(
                    id=t["i"],
                    price=float(t["p"]),
                    qty=float(t["v"]),
                    side=t["S"].lower(),
                    timestamp=self.normalize_timestamp(t["T"])
                )

    async def stream_orderbook(self, market_type: MarketType, symbol: str, depth: int = 20, update_speed: str = "100ms") -> AsyncGenerator[OrderBook, None]:
        api_symbol = self.get_api_symbol(symbol, market_type)
        lvl = 50
        if depth <= 1: lvl = 1
        elif depth <= 50: lvl = 50
        elif depth <= 200: lvl = 200
        else: lvl = 500
        topic = f"orderbook.{lvl}.{api_symbol}"
        async for msg in self._ws_connect(market_type, [topic]):
            d = msg["data"]
            yield OrderBook(
                symbol=self.get_model_symbol(d.get("s", api_symbol), market_type),
                bids=[[float(p), float(q)] for p, q in d.get("b", [])],
                asks=[[float(p), float(q)] for p, q in d.get("a", [])],
                timestamp=self.normalize_timestamp(msg.get("ts"))
            )

    async def stream_liquidations(self, market_type: MarketType, symbol: str) -> AsyncGenerator[Liquidation, None]:
        if market_type == MarketType.SPOT: raise NotImplementedError()
        api_symbol = self.get_api_symbol(symbol, market_type)
        topic = f"allLiquidation.{api_symbol}"
        async for msg in self._ws_connect(market_type, [topic]):
            for d in msg.get("data", []):
                yield Liquidation(
                    symbol=self.get_model_symbol(d.get("s", api_symbol), market_type),
                    side=d.get("S", "").lower(),
                    price=float(d.get("p", 0)),
                    qty=float(d.get("v", 0)),
                    timestamp=self.normalize_timestamp(d.get("T", msg.get("ts")))
                )