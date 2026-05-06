import httpx
import json
import asyncio
import time
import uuid
import websockets
import logging
from typing import List, AsyncGenerator, Dict, Any, Optional, Callable, Tuple
from src.exchanges.base import BaseExchange
from src.models import (
    Ticker, BookTicker, MarkPrice, OrderBook, Candle, Trade, AggTrade,
    MarketType, SymbolInfo, OpenInterest, FundingRate
)


logger = logging.getLogger("kucoin_adapter")


class KucoinAdapter(BaseExchange):
    SPOT_QUOTES = ["USDT", "USDC", "TUSD", "USD", "BTC", "ETH", "EUR", "GBP", "DAI", "KCS"]


    def __init__(self):
        super().__init__()
        self.http_client = httpx.AsyncClient(timeout=30.0)

        self._backoff_until = 0.0
        self._backoff_lock = asyncio.Lock()

        self.spot_rest_url = "https://api.kucoin.com"
        self.fut_rest_url = "https://api-futures.kucoin.com"

        self._spot_symbol_map: Dict[str, str] = {}
        self._spot_symbol_map_lock = asyncio.Lock()
        self._learned_quotes: List[str] = []

        self._capabilities = self._build_capabilities()


    async def shutdown(self):
        await self.http_client.aclose()


    @property
    def name(self) -> str:
        return "kucoin"


    @property
    def supported_market_types(self) -> List[MarketType]:
        return [MarketType.SPOT, MarketType.LINEAR, MarketType.INVERSE]


    def get_capabilities(self) -> Dict[str, Any]:
        return self._capabilities


    def _build_capabilities(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "markets": {
                MarketType.SPOT: {
                    "ticker": {
                        "rest": True,
                        "ws":   True,
                    },
                    "book_ticker": {
                        "rest": True,
                        "ws":   True,
                    },
                    "orderbook": {
                        "rest":      True,
                        "ws":        True,
                        "depths":    [20, 100],
                        "max_depth": 100,
                    },
                    "trades": {
                        "rest":      True,
                        "ws":        True,
                        "max_limit": 100,
                    },
                    "agg_trades": {
                        "rest":         False,
                        "ws":           False,
                        "paginated":    False,
                        "max_limit":    None,
                        "retention_ms": None,
                    },
                    "candles": {
                        "rest":         True,
                        "ws":           False,
                        "paginated":    True,
                        "max_limit":    None,
                        "retention_ms": None,
                        "intervals":    ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "1w"],
                    },
                    "mark_price": {
                        "rest": False,
                        "ws":   False,
                    },
                    "funding_rate": {
                        "rest":         False,
                        "ws":           False,
                        "paginated":    False,
                        "max_limit":    None,
                        "retention_ms": None,
                    },
                    "open_interest": {
                        "rest":         False,
                        "ws":           False,
                        "paginated":    False,
                        "max_limit":    None,
                        "retention_ms": None,
                        "intervals":    None,
                    },
                    "liquidations": {
                        "rest":         False,
                        "ws":           False,
                        "paginated":    False,
                        "max_limit":    None,
                        "retention_ms": None,
                    },
                    "long_short_ratio": {
                        "rest":         False,
                        "ws":           False,
                        "paginated":    False,
                        "max_limit":    None,
                        "retention_ms": None,
                        "intervals":    None,
                    },
                },
                MarketType.LINEAR: {
                    "ticker": {
                        "rest": True,
                        "ws":   True,
                    },
                    "book_ticker": {
                        "rest": True,
                        "ws":   True,
                    },
                    "orderbook": {
                        "rest":      True,
                        "ws":        True,
                        "depths":    [20, 100],
                        "max_depth": 100,
                    },
                    "trades": {
                        "rest":      True,
                        "ws":        True,
                        "max_limit": 100,
                    },
                    "agg_trades": {
                        "rest":         False,
                        "ws":           False,
                        "paginated":    False,
                        "max_limit":    None,
                        "retention_ms": None,
                    },
                    "candles": {
                        "rest":         True,
                        "ws":           False,
                        "paginated":    True,
                        "max_limit":    None,
                        "retention_ms": None,
                        "intervals":    ["1m", "5m", "15m", "30m", "1h", "2h", "4h", "8h", "12h", "1d", "1w"],
                    },
                    "mark_price": {
                        "rest": True,
                        "ws":   True,
                    },
                    "funding_rate": {
                        "rest":         True,
                        "ws":           False,
                        "paginated":    True,
                        "max_limit":    None,
                        "retention_ms": None,
                    },
                    "open_interest": {
                        "rest":         True,
                        "ws":           False,
                        "paginated":    True,
                        "max_limit":    None,
                        "retention_ms": 7 * 24 * 60 * 60 * 1000,
                        "intervals":    ["5m", "15m", "30m", "1h", "4h", "1d"],
                    },
                    "liquidations": {
                        "rest":         False,
                        "ws":           False,
                        "paginated":    False,
                        "max_limit":    None,
                        "retention_ms": None,
                    },
                    "long_short_ratio": {
                        "rest":         False,
                        "ws":           False,
                        "paginated":    False,
                        "max_limit":    None,
                        "retention_ms": None,
                        "intervals":    None,
                    },
                },
                MarketType.INVERSE: {
                    "ticker": {
                        "rest": True,
                        "ws":   True,
                    },
                    "book_ticker": {
                        "rest": True,
                        "ws":   True,
                    },
                    "orderbook": {
                        "rest":      True,
                        "ws":        True,
                        "depths":    [20, 100],
                        "max_depth": 100,
                    },
                    "trades": {
                        "rest":      True,
                        "ws":        True,
                        "max_limit": 100,
                    },
                    "agg_trades": {
                        "rest":         False,
                        "ws":           False,
                        "paginated":    False,
                        "max_limit":    None,
                        "retention_ms": None,
                    },
                    "candles": {
                        "rest":         True,
                        "ws":           False,
                        "paginated":    True,
                        "max_limit":    None,
                        "retention_ms": None,
                        "intervals":    ["1m", "5m", "15m", "30m", "1h", "2h", "4h", "8h", "12h", "1d", "1w"],
                    },
                    "mark_price": {
                        "rest": True,
                        "ws":   True,
                    },
                    "funding_rate": {
                        "rest":         True,
                        "ws":           False,
                        "paginated":    True,
                        "max_limit":    None,
                        "retention_ms": None,
                    },
                    "open_interest": {
                        "rest":         True,
                        "ws":           False,
                        "paginated":    True,
                        "max_limit":    None,
                        "retention_ms": 7 * 24 * 60 * 60 * 1000,
                        "intervals":    ["5m", "15m", "30m", "1h", "4h", "1d"],
                    },
                    "liquidations": {
                        "rest":         False,
                        "ws":           False,
                        "paginated":    False,
                        "max_limit":    None,
                        "retention_ms": None,
                    },
                    "long_short_ratio": {
                        "rest":         False,
                        "ws":           False,
                        "paginated":    False,
                        "max_limit":    None,
                        "retention_ms": None,
                        "intervals":    None,
                    },
                },
            },
        }


    def normalize_symbol(self, symbol: str) -> str:
        s = symbol.upper().replace("/", "").replace("-", "")
        if s.endswith("USDTM"): return s[:-1]
        if s.endswith("USDM"): return s[:-1]
        return s


    def get_api_symbol(self, symbol: str, market_type: MarketType) -> str:
        s = symbol.upper().replace("/", "").replace("-", "")
        if market_type == MarketType.SPOT:
            for q in self._learned_quotes or self.SPOT_QUOTES:
                if s.endswith(q) and len(s) > len(q):
                    return f"{s[:-len(q)]}-{q}"
            return s
        if not s.endswith("M"):
            s = f"{s}M"
        return s


    def get_model_symbol(self, api_symbol: str, market_type: MarketType) -> str:
        return self.normalize_symbol(api_symbol)


    async def _ensure_spot_symbol_map(self) -> None:
        if self._spot_symbol_map:
            return
        async with self._spot_symbol_map_lock:
            if self._spot_symbol_map:
                return
            data = await self._make_request("GET", self.spot_rest_url, "/api/v1/symbols")
            built: Dict[str, str] = {}
            quotes: set = set()
            for s in data or []:
                if not s.get("enableTrading"):
                    continue
                native = s.get("symbol")
                base = s.get("baseCurrency", "")
                quote = s.get("quoteCurrency", "")
                if not native or not base or not quote:
                    continue
                built[f"{base}{quote}".upper()] = native
                quotes.add(quote.upper())
            self._spot_symbol_map = built
            self._learned_quotes  = sorted(quotes, key=len, reverse=True)


    async def _resolve_symbol(self, symbol: str, market_type: MarketType) -> str:
        if market_type != MarketType.SPOT:
            return self.get_api_symbol(symbol, market_type)
        flat = symbol.upper().replace("/", "").replace("-", "")
        try:
            await self._ensure_spot_symbol_map()
        except Exception as e:
            logger.warning(f"Could not load {self.name} spot symbol map: {e}. Falling back to SPOT_QUOTES heuristic.")
            return self.get_api_symbol(symbol, market_type)
        native = self._spot_symbol_map.get(flat)
        if native:
            return native
        raise ValueError(f"Symbol {symbol} not listed on KuCoin spot")


    def _rest_base(self, market_type: MarketType) -> str:
        return self.spot_rest_url if market_type == MarketType.SPOT else self.fut_rest_url


    def _map_spot_interval(self, interval: str) -> str:
        mapping = {
            "1m": "1min", "3m": "3min", "5m": "5min", "15m": "15min", "30m": "30min",
            "1h": "1hour", "2h": "2hour", "4h": "4hour", "6h": "6hour", "8h": "8hour", "12h": "12hour",
            "1d": "1day", "1w": "1week",
        }
        return mapping.get(interval, interval)


    def _map_futures_granularity(self, interval: str) -> int:
        mapping = {
            "1m": 1, "5m": 5, "15m": 15, "30m": 30,
            "1h": 60, "2h": 120, "4h": 240, "8h": 480, "12h": 720,
            "1d": 1440, "1w": 10080,
        }
        return mapping.get(interval, 60)


    def _map_oi_interval(self, period: str) -> str:
        mapping = {
            "5m": "5min", "15m": "15min", "30m": "30min",
            "1h": "1hour", "4h": "4hour", "1d": "1day",
        }
        return mapping.get(period, period)


    def _interval_to_ms(self, interval: str) -> int:
        if not interval:
            return 0
        unit = interval[-1]
        try:
            val = int(interval[:-1])
        except (ValueError, TypeError):
            return 0
        if unit == "m": return val * 60 * 1000
        if unit == "h": return val * 60 * 60 * 1000
        if unit == "d": return val * 24 * 60 * 60 * 1000
        if unit == "w": return val * 7 * 24 * 60 * 60 * 1000
        return 0


    @staticmethod
    def _ns_to_ms(ts) -> int:
        try:
            v = int(ts)
        except (ValueError, TypeError):
            return 0
        return v // 1_000_000 if v > 10**14 else v


    @staticmethod
    def _s_to_ms(ts) -> int:
        try:
            v = int(ts)
        except (ValueError, TypeError):
            return 0
        return v * 1000 if v < 10**11 else v


    @staticmethod
    def _precision(value: str) -> int:
        if not value or "." not in str(value):
            return 0
        return len(str(value).split(".")[1].rstrip("0"))


    async def _make_request(self, method: str, base: str, endpoint: str, params: Optional[Dict] = None) -> Any:
        url = f"{base}{endpoint}"
        retries = 3
        while retries > 0:
            now = time.time()
            if now < self._backoff_until:
                wait_time = self._backoff_until - now
                if wait_time > 30:
                    raise ValueError(f"KuCoin backoff active. Retry in {wait_time:.0f}s.")
                await asyncio.sleep(wait_time + 0.1)

            try:
                resp = await self.http_client.request(method, url, params=params)

                if resp.status_code == 429:
                    async with self._backoff_lock:
                        self._backoff_until = max(self._backoff_until, time.time() + 5)
                    logger.error("KuCoin rate limit (429). Backing off 5s.")
                    retries -= 1
                    continue

                if resp.status_code == 200:
                    body = resp.json()
                    code = body.get("code", "200000")
                    if code != "200000":
                        raise ValueError(f"KuCoin API Error ({code}): {body.get('msg', 'Unknown error')}")
                    return body.get("data")

                resp.raise_for_status()

            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP Error: {e}")
                raise e
            except Exception as e:
                if isinstance(e, ValueError):
                    raise e
                logger.error(f"Request Error: {e}")
                retries -= 1
                await asyncio.sleep(1)

        raise Exception(f"Max retries exceeded for {url}")


    async def _paginate_backwards(self, fetch_func_by_end: Callable, total_limit: int, limit_per_req: int) -> List[Any]:
        chunks = []
        seen = set()
        collected = 0
        current_end = None
        max_requests = 100
        req_count = 0

        while collected < total_limit and req_count < max_requests:
            try:
                batch = await fetch_func_by_end(current_end, limit_per_req)
            except (httpx.HTTPStatusError, ValueError):
                break
            if not batch:
                break

            batch.sort(key=lambda x: x.timestamp)
            new_items = []
            for x in batch:
                key = x.model_dump_json()
                if key not in seen:
                    seen.add(key)
                    new_items.append(x)
            if not new_items:
                break

            chunks.append(new_items)
            collected += len(new_items)
            req_count += 1
            next_end = batch[0].timestamp - 1
            if next_end <= 0 or (current_end is not None and next_end >= current_end):
                break
            current_end = next_end

        if req_count >= max_requests and collected < total_limit:
            logger.warning(f"KuCoin pagination hit {max_requests}-page safety cap with {collected}/{total_limit} records; result truncated")

        chunks.reverse()
        out = [item for chunk in chunks for item in chunk]
        out.sort(key=lambda x: x.timestamp)
        return out[-total_limit:]


    async def _get_bullet(self, market_type: MarketType) -> Tuple[str, str, int]:
        base = self._rest_base(market_type)
        url = f"{base}/api/v1/bullet-public"
        resp = await self.http_client.post(url)
        body = resp.json()
        if body.get("code") != "200000":
            raise ValueError(f"KuCoin bullet token error: {body}")
        data = body["data"]
        server = data["instanceServers"][0]
        return server["endpoint"], data["token"], int(server.get("pingInterval", 18000))


    async def _ws_connect(self, market_type: MarketType, topic: str) -> AsyncGenerator[Dict, None]:
        reconnect_delay = 1
        while True:
            try:
                endpoint, token, ping_interval_ms = await self._get_bullet(market_type)
                connect_id = str(uuid.uuid4())
                url = f"{endpoint}?token={token}&connectId={connect_id}"
                ping_secs = max(ping_interval_ms / 1000 - 2, 5)

                async with websockets.connect(url, ping_interval=None) as ws:
                    logger.info(f"KuCoin WS connected ({market_type.value}): {topic}")
                    await asyncio.wait_for(ws.recv(), timeout=10)

                    sub_id = str(int(time.time() * 1000))
                    await ws.send(json.dumps({
                        "id": sub_id,
                        "type": "subscribe",
                        "topic": topic,
                        "privateChannel": False,
                        "response": True,
                    }))
                    last_ping = time.time()
                    reconnect_delay = 1

                    while True:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=ping_secs)
                        except asyncio.TimeoutError:
                            await ws.send(json.dumps({"id": sub_id, "type": "ping"}))
                            last_ping = time.time()
                            continue

                        try:
                            data = json.loads(msg)
                        except json.JSONDecodeError:
                            continue

                        t = data.get("type")
                        if t in ("pong", "ack", "welcome"):
                            continue
                        if t == "error":
                            logger.error(f"KuCoin WS error: {data}")
                            continue
                        if t == "message" and "data" in data:
                            yield data

                        if time.time() - last_ping > ping_secs:
                            await ws.send(json.dumps({"id": sub_id, "type": "ping"}))
                            last_ping = time.time()

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"KuCoin WS disconnected ({e}). Reconnecting in {reconnect_delay}s...")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 30)


    async def get_ticker(self, market_type: MarketType, symbol: str) -> Ticker:
        api_symbol = await self._resolve_symbol(symbol, market_type)
        model_sym = self.get_model_symbol(api_symbol, market_type)

        if market_type == MarketType.SPOT:
            t = await self._make_request("GET", self.spot_rest_url, "/api/v1/market/stats", {"symbol": api_symbol})
            if not t:
                raise ValueError(f"Symbol {api_symbol} not found")

            last = float(t.get("last") or 0)
            change_pct = float(t.get("changeRate") or 0) * 100

            return Ticker(
                symbol=model_sym,
                price=last,
                open_24h=last - float(t.get("changePrice") or 0),
                high_24h=float(t.get("high") or 0),
                low_24h=float(t.get("low") or 0),
                volume_24h=float(t.get("vol") or 0),
                quote_volume_24h=float(t.get("volValue") or 0),
                price_change_percent=change_pct,
                timestamp=int(t.get("time") or 0),
            )

        t = await self._make_request("GET", self.fut_rest_url, "/api/v1/ticker", {"symbol": api_symbol})
        if not t:
            raise ValueError(f"Symbol {api_symbol} not found")

        last = float(t.get("price") or 0)

        return Ticker(
            symbol=model_sym,
            price=last,
            open_24h=0.0,
            high_24h=0.0,
            low_24h=0.0,
            volume_24h=0.0,
            quote_volume_24h=0.0,
            price_change_percent=0.0,
            timestamp=self._ns_to_ms(t.get("ts") or 0),
        )


    async def get_book_ticker(self, market_type: MarketType, symbol: str) -> BookTicker:
        api_symbol = await self._resolve_symbol(symbol, market_type)
        model_sym = self.get_model_symbol(api_symbol, market_type)

        if market_type == MarketType.SPOT:
            data = await self._make_request("GET", self.spot_rest_url, "/api/v1/market/orderbook/level1", {"symbol": api_symbol})
            if not data:
                raise ValueError(f"Book ticker not found for {api_symbol}")

            return BookTicker(
                symbol=model_sym,
                bid_price=float(data.get("bestBid") or 0),
                bid_qty=float(data.get("bestBidSize") or 0),
                ask_price=float(data.get("bestAsk") or 0),
                ask_qty=float(data.get("bestAskSize") or 0),
                timestamp=int(data.get("time") or 0),
            )

        t = await self._make_request("GET", self.fut_rest_url, "/api/v1/ticker", {"symbol": api_symbol})
        if not t:
            raise ValueError(f"Book ticker not found for {api_symbol}")

        return BookTicker(
            symbol=model_sym,
            bid_price=float(t.get("bestBidPrice") or 0),
            bid_qty=float(t.get("bestBidSize") or 0),
            ask_price=float(t.get("bestAskPrice") or 0),
            ask_qty=float(t.get("bestAskSize") or 0),
            timestamp=self._ns_to_ms(t.get("ts") or 0),
        )


    async def get_orderbook(self, market_type: MarketType, symbol: str, depth: int = 20) -> OrderBook:
        api_symbol = await self._resolve_symbol(symbol, market_type)
        model_sym = self.get_model_symbol(api_symbol, market_type)

        ob_caps = self.get_capabilities()["markets"][market_type]["orderbook"]
        discrete_depth_bucket = next((d for d in ob_caps["depths"] if d >= depth), ob_caps["max_depth"])

        if market_type == MarketType.SPOT:
            endpoint = f"/api/v1/market/orderbook/level2_{discrete_depth_bucket}"
            data = await self._make_request("GET", self.spot_rest_url, endpoint, {"symbol": api_symbol})
            if not data:
                raise ValueError(f"Orderbook not found for {api_symbol}")
            ts = int(data.get("time") or 0)
        else:
            endpoint = f"/api/v1/level2/depth{discrete_depth_bucket}"
            data = await self._make_request("GET", self.fut_rest_url, endpoint, {"symbol": api_symbol})
            if not data:
                raise ValueError(f"Orderbook not found for {api_symbol}")
            ts = self._ns_to_ms(data.get("ts") or 0)

        return OrderBook(
            symbol=model_sym,
            bids=[[float(p), float(q)] for p, q, *_ in data.get("bids", [])][:depth],
            asks=[[float(p), float(q)] for p, q, *_ in data.get("asks", [])][:depth],
            timestamp=ts,
        )


    async def get_trades(self, market_type: MarketType, symbol: str, limit: int = 100) -> List[Trade]:
        limit = min(limit, self.get_capabilities()["markets"][market_type]["trades"]["max_limit"])
        api_symbol = await self._resolve_symbol(symbol, market_type)

        if market_type == MarketType.SPOT:
            data = await self._make_request("GET", self.spot_rest_url, "/api/v1/market/histories", {"symbol": api_symbol})
            trades = [Trade(
                id=str(t.get("sequence") or t.get("tradeId") or ""),
                price=float(t["price"]),
                qty=float(t["size"]),
                side=t["side"],
                timestamp=self._ns_to_ms(t.get("time") or 0),
            ) for t in (data or [])]
        else:
            data = await self._make_request("GET", self.fut_rest_url, "/api/v1/trade/history", {"symbol": api_symbol})
            trades = [Trade(
                id=str(t.get("tradeId") or t.get("sequence") or ""),
                price=float(t["price"]),
                qty=float(t["size"]),
                side=t["side"],
                timestamp=self._ns_to_ms(t.get("ts") or 0),
            ) for t in (data or [])]

        trades.sort(key=lambda t: t.timestamp)

        return trades[-limit:]


    async def get_agg_trades(self, market_type: MarketType, symbol: str, start_time: Optional[int] = None, limit: int = 500) -> List[AggTrade]:
        raise NotImplementedError(f"{self.name} does not support agg_trades for {market_type.value}")


    async def get_candles(self, market_type: MarketType, symbol: str, interval: str, start_time: Optional[int] = None, limit: int = 100) -> List[Candle]:
        api_symbol = await self._resolve_symbol(symbol, market_type)

        if market_type == MarketType.SPOT:
            return await self._get_candles_spot(api_symbol, interval, start_time, limit)
        return await self._get_candles_futures(api_symbol, interval, start_time, limit)


    async def _get_candles_spot(self, api_symbol: str, interval: str, start_time: Optional[int], limit: int) -> List[Candle]:
        ktype = self._map_spot_interval(interval)
        interval_ms = self._interval_to_ms(interval)
        per_req = 1500

        def _parse(rows):
            out = []
            if not isinstance(rows, list):
                return out
            for r in rows:
                if not isinstance(r, list) or len(r) < 6:
                    continue
                try:
                    ts = self._s_to_ms(r[0])
                    if ts <= 0:
                        continue
                    out.append(Candle(
                        timestamp=ts,
                        open=float(r[1]),
                        close=float(r[2]),
                        high=float(r[3]),
                        low=float(r[4]),
                        volume=float(r[5]),
                    ))
                except (ValueError, TypeError):
                    continue
            return sorted(out, key=lambda x: x.timestamp)

        async def fetch(end_ts, l):
            anchor = end_ts if end_ts is not None else start_time
            params = {"symbol": api_symbol, "type": ktype}
            if anchor:
                end_s = (anchor // 1000) + 1
                window_s = max(interval_ms * per_req // 1000, 60)
                params["startAt"] = max(end_s - window_s, 0)
                params["endAt"] = end_s
            return _parse(await self._make_request("GET", self.spot_rest_url, "/api/v1/market/candles", params))

        if start_time is None and limit <= per_req:
            return (await fetch(None, limit))[-limit:]
        return await self._paginate_backwards(fetch, limit, per_req)


    async def _get_candles_futures(self, api_symbol: str, interval: str, start_time: Optional[int], limit: int) -> List[Candle]:
        granularity = self._map_futures_granularity(interval)
        per_req = 500

        def _parse(rows):
            out = []
            if not isinstance(rows, list):
                return out
            for r in rows:
                if not isinstance(r, list) or len(r) < 6:
                    continue
                try:
                    ts = int(r[0])
                    if ts <= 0:
                        continue
                    out.append(Candle(
                        timestamp=ts,
                        open=float(r[1]),
                        high=float(r[2]),
                        low=float(r[3]),
                        close=float(r[4]),
                        volume=float(r[5]),
                    ))
                except (ValueError, TypeError):
                    continue
            return sorted(out, key=lambda x: x.timestamp)

        async def fetch(end_ts, l):
            anchor = end_ts if end_ts is not None else start_time
            params = {"symbol": api_symbol, "granularity": granularity}
            if anchor:
                params["to"] = anchor + 1
            return _parse(await self._make_request("GET", self.fut_rest_url, "/api/v1/kline/query", params))

        if start_time is None and limit <= per_req:
            return (await fetch(None, limit))[-limit:]
        return await self._paginate_backwards(fetch, limit, per_req)


    async def get_mark_price(self, market_type: MarketType, symbol: str) -> MarkPrice:
        if market_type == MarketType.SPOT:
            raise NotImplementedError(f"{self.name} does not support mark_price for {market_type.value}")
        api_symbol = await self._resolve_symbol(symbol, market_type)
        model_sym = self.get_model_symbol(api_symbol, market_type)

        m = await self._make_request("GET", self.fut_rest_url, f"/api/v1/mark-price/{api_symbol}/current")
        if not m:
            raise ValueError(f"Mark price not found for {api_symbol}")

        funding_rate = 0.0
        next_funding = 0
        try:
            contract = await self._make_request("GET", self.fut_rest_url, f"/api/v1/contracts/{api_symbol}")
            if contract:
                funding_rate = float(contract.get("fundingFeeRate") or 0)
                next_funding = int(contract.get("nextFundingRateTime") or 0)
        except (httpx.HTTPStatusError, ValueError):
            pass

        return MarkPrice(
            symbol=model_sym,
            mark_price=float(m.get("value") or 0),
            index_price=float(m.get("indexPrice") or 0),
            funding_rate=funding_rate,
            next_funding_time=next_funding,
            timestamp=int(m.get("timePoint") or 0),
        )


    async def get_funding_rate(self, market_type: MarketType, symbol: str, start_time: Optional[int] = None, limit: int = 100) -> List[FundingRate]:
        if market_type == MarketType.SPOT:
            raise NotImplementedError(f"{self.name} does not support funding_rate for {market_type.value}")
        api_symbol = await self._resolve_symbol(symbol, market_type)
        model_sym = self.get_model_symbol(api_symbol, market_type)
        funding_window_ms = 8 * 60 * 60 * 1000
        per_req = 100

        def _parse(rows):
            out = []
            for r in rows or []:
                try:
                    ts = int(r.get("timepoint") or r.get("timePoint") or 0)
                    if ts <= 0:
                        continue
                    out.append(FundingRate(
                        symbol=model_sym,
                        rate=float(r.get("fundingRate") or 0),
                        timestamp=ts,
                    ))
                except (ValueError, TypeError):
                    continue
            return sorted(out, key=lambda x: x.timestamp)

        async def fetch(end_ts, l):
            anchor = end_ts if end_ts is not None else start_time
            now_ms = int(time.time() * 1000)
            to_ms = (anchor + 1) if anchor else now_ms
            from_ms = max(to_ms - funding_window_ms * per_req, 0)
            params = {"symbol": api_symbol, "from": from_ms, "to": to_ms}
            return _parse(await self._make_request("GET", self.fut_rest_url, "/api/v1/contract/funding-rates", params))

        if start_time is not None or limit > per_req:
            return await self._paginate_backwards(fetch, limit, per_req)
        return (await fetch(None, limit))[-limit:]


    async def get_open_interest(self, market_type: MarketType, symbol: str, period: str = "1h", start_time: Optional[int] = None, limit: int = 30) -> List[OpenInterest]:
        if market_type == MarketType.SPOT:
            raise NotImplementedError(f"{self.name} does not support open_interest for {market_type.value}")
        api_symbol = await self._resolve_symbol(symbol, market_type)
        model_sym = self.get_model_symbol(api_symbol, market_type)
        interval = self._map_oi_interval(period)
        period_ms = self._interval_to_ms(period)
        per_req = 200

        def _parse(rows):
            out = []
            for r in rows or []:
                try:
                    ts = int(r.get("ts") or 0)
                    if ts <= 0:
                        continue
                    out.append(OpenInterest(
                        symbol=model_sym,
                        open_interest=float(r.get("openInterest") or 0),
                        value_usd=0.0,
                        timestamp=ts,
                    ))
                except (ValueError, TypeError):
                    continue
            return sorted(out, key=lambda x: x.timestamp)

        async def fetch(end_ts, l):
            anchor = end_ts if end_ts is not None else start_time
            now_ms = int(time.time() * 1000)
            raw_end = (anchor + 1) if anchor else now_ms
            end_ms = raw_end - (raw_end % period_ms) if period_ms else raw_end
            start_ms = max(end_ms - period_ms * per_req, 0) if period_ms else 0
            params = {
                "symbol":   api_symbol,
                "interval": interval,
                "startAt":  start_ms,
                "endAt":    end_ms,
                "pageSize": min(l, per_req),
            }
            return _parse(await self._make_request("GET", self.spot_rest_url, "/api/ua/v1/market/open-interest", params))

        if start_time is not None or limit > per_req:
            return await self._paginate_backwards(fetch, limit, per_req)
        return (await fetch(None, limit))[-limit:]


    async def get_exchange_info(self, market_type: MarketType) -> List[SymbolInfo]:
        if market_type == MarketType.SPOT:
            data = await self._make_request("GET", self.spot_rest_url, "/api/v1/symbols")
            results = []
            for s in data or []:
                if not s.get("enableTrading"):
                    continue
                api_sym = s["symbol"]
                results.append(SymbolInfo(
                    symbol=self.get_model_symbol(api_sym, market_type),
                    native_symbol=api_sym,
                    base_asset=s.get("baseCurrency", ""),
                    quote_asset=s.get("quoteCurrency", ""),
                    price_precision=self._precision(s.get("priceIncrement", "0")),
                    quantity_precision=self._precision(s.get("baseIncrement", "0")),
                    min_qty=float(s.get("baseMinSize") or 0),
                    max_qty=float(s.get("baseMaxSize") or 0),
                    min_notional=float(s.get("minFunds") or s.get("quoteMinSize") or 0),
                ))
            return results

        data = await self._make_request("GET", self.fut_rest_url, "/api/v1/contracts/active")
        results = []
        for s in data or []:
            if s.get("status") != "Open":
                continue
            is_inv = bool(s.get("isInverse"))
            if market_type == MarketType.LINEAR and is_inv:
                continue
            if market_type == MarketType.INVERSE and not is_inv:
                continue
            api_sym = s["symbol"]
            results.append(SymbolInfo(
                symbol=self.get_model_symbol(api_sym, market_type),
                native_symbol=api_sym,
                base_asset=s.get("baseCurrency", ""),
                quote_asset=s.get("quoteCurrency", ""),
                price_precision=self._precision(str(s.get("tickSize", "0"))),
                quantity_precision=self._precision(str(s.get("lotSize", "0"))),
                min_qty=float(s.get("lotSize") or 0),
                max_qty=float(s.get("maxOrderQty") or 0),
                min_notional=0.0,
            ))
        return results


    async def get_markets(self, market_type: MarketType) -> List[str]:
        info = await self.get_exchange_info(market_type)
        return [s.symbol for s in info]


    async def stream_ticker(self, market_type: MarketType, symbol: str) -> AsyncGenerator[Ticker, None]:
        api_symbol = await self._resolve_symbol(symbol, market_type)
        model_sym = self.get_model_symbol(api_symbol, market_type)

        if market_type == MarketType.SPOT:
            topic = f"/market/ticker:{api_symbol}"
            async for msg in self._ws_connect(market_type, topic):
                d = msg.get("data") or {}
                last = float(d.get("price") or 0)
                yield Ticker(
                    symbol=model_sym,
                    price=last,
                    open_24h=0.0,
                    high_24h=0.0,
                    low_24h=0.0,
                    volume_24h=float(d.get("size") or 0),
                    quote_volume_24h=0.0,
                    price_change_percent=0.0,
                    timestamp=int(d.get("time") or 0),
                )
            return

        topic = f"/contractMarket/tickerV2:{api_symbol}"
        async for msg in self._ws_connect(market_type, topic):
            d = msg.get("data") or {}
            best_bid = float(d.get("bestBidPrice") or 0)
            best_ask = float(d.get("bestAskPrice") or 0)
            mid = (best_bid + best_ask) / 2 if (best_bid and best_ask) else max(best_bid, best_ask)
            yield Ticker(
                symbol=model_sym,
                price=mid,
                open_24h=0.0,
                high_24h=0.0,
                low_24h=0.0,
                volume_24h=0.0,
                quote_volume_24h=0.0,
                price_change_percent=0.0,
                timestamp=self._ns_to_ms(d.get("ts") or 0),
            )


    async def stream_book_ticker(self, market_type: MarketType, symbol: str) -> AsyncGenerator[BookTicker, None]:
        api_symbol = await self._resolve_symbol(symbol, market_type)
        model_sym = self.get_model_symbol(api_symbol, market_type)

        if market_type == MarketType.SPOT:
            topic = f"/spotMarket/level1:{api_symbol}"
            async for msg in self._ws_connect(market_type, topic):
                d = msg.get("data") or {}
                bids = d.get("bids") or []
                asks = d.get("asks") or []
                if not bids or not asks:
                    continue
                yield BookTicker(
                    symbol=model_sym,
                    bid_price=float(bids[0]),
                    bid_qty=float(bids[1]) if len(bids) > 1 else 0.0,
                    ask_price=float(asks[0]),
                    ask_qty=float(asks[1]) if len(asks) > 1 else 0.0,
                    timestamp=self._ns_to_ms(d.get("timestamp") or d.get("time") or 0),
                )
            return

        topic = f"/contractMarket/tickerV2:{api_symbol}"
        async for msg in self._ws_connect(market_type, topic):
            d = msg.get("data") or {}
            yield BookTicker(
                symbol=model_sym,
                bid_price=float(d.get("bestBidPrice") or 0),
                bid_qty=float(d.get("bestBidSize") or 0),
                ask_price=float(d.get("bestAskPrice") or 0),
                ask_qty=float(d.get("bestAskSize") or 0),
                timestamp=self._ns_to_ms(d.get("ts") or 0),
            )


    async def stream_orderbook(self, market_type: MarketType, symbol: str, depth: int = 20, update_speed: str = "100ms") -> AsyncGenerator[OrderBook, None]:
        api_symbol = await self._resolve_symbol(symbol, market_type)
        model_sym = self.get_model_symbol(api_symbol, market_type)
        size = 5 if depth <= 5 else 50
        prefix = "/spotMarket" if market_type == MarketType.SPOT else "/contractMarket"
        topic = f"{prefix}/level2Depth{size}:{api_symbol}"

        async for msg in self._ws_connect(market_type, topic):
            d = msg.get("data") or {}
            ts = d.get("timestamp") or d.get("ts") or d.get("time") or 0
            yield OrderBook(
                symbol=model_sym,
                bids=[[float(p), float(q)] for p, q, *_ in d.get("bids", [])][:depth],
                asks=[[float(p), float(q)] for p, q, *_ in d.get("asks", [])][:depth],
                timestamp=self._ns_to_ms(ts),
            )


    async def stream_trades(self, market_type: MarketType, symbol: str) -> AsyncGenerator[Trade, None]:
        api_symbol = await self._resolve_symbol(symbol, market_type)

        if market_type == MarketType.SPOT:
            topic = f"/market/match:{api_symbol}"
            async for msg in self._ws_connect(market_type, topic):
                d = msg.get("data") or {}
                yield Trade(
                    id=str(d.get("tradeId") or d.get("sequence") or ""),
                    price=float(d.get("price") or 0),
                    qty=float(d.get("size") or 0),
                    side=d.get("side", "buy"),
                    timestamp=self._ns_to_ms(d.get("time") or 0),
                )
            return

        topic = f"/contractMarket/execution:{api_symbol}"
        async for msg in self._ws_connect(market_type, topic):
            d = msg.get("data") or {}
            yield Trade(
                id=str(d.get("tradeId") or d.get("sequence") or ""),
                price=float(d.get("price") or 0),
                qty=float(d.get("size") or 0),
                side=d.get("side", "buy"),
                timestamp=self._ns_to_ms(d.get("ts") or 0),
            )


    async def stream_mark_price(self, market_type: MarketType, symbol: str) -> AsyncGenerator[MarkPrice, None]:
        if market_type == MarketType.SPOT:
            raise NotImplementedError(f"{self.name} does not support stream_mark_price for {market_type.value}")
        api_symbol = await self._resolve_symbol(symbol, market_type)
        model_sym = self.get_model_symbol(api_symbol, market_type)
        topic = f"/contract/instrument:{api_symbol}"

        async for msg in self._ws_connect(market_type, topic):
            d = msg.get("data") or {}
            subject = msg.get("subject", "")
            mark = float(d.get("markPrice") or 0)
            if not mark and subject != "mark.index.price":
                continue
            yield MarkPrice(
                symbol=model_sym,
                mark_price=mark,
                index_price=float(d.get("indexPrice") or 0),
                funding_rate=float(d.get("fundingRate") or 0),
                next_funding_time=int(d.get("nextFundingTime") or d.get("granularity") or 0),
                timestamp=self._ns_to_ms(d.get("timestamp") or d.get("ts") or 0),
            )
