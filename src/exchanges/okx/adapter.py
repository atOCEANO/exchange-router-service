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


logger = logging.getLogger("okx_adapter")


class OkxAdapter(BaseExchange):
    SPOT_QUOTES = ["USDT", "USDC", "USD", "BTC", "ETH", "EUR", "GBP", "DAI"]


    def __init__(self):
        super().__init__()
        self.http_client = httpx.AsyncClient(timeout=30.0)

        self._backoff_until = 0.0
        self._backoff_lock = asyncio.Lock()

        self.rest_url = "https://www.okx.com"
        self.ws_url = "wss://ws.okx.com:8443/ws/v5/public"

        self._spot_symbol_map: Dict[str, str] = {}
        self._spot_symbol_map_lock = asyncio.Lock()
        self._learned_quotes: List[str] = []

        self._capabilities = self._build_capabilities()


    async def shutdown(self):
        await self.http_client.aclose()


    @property
    def name(self) -> str:
        return "okx"


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
                        "depths":    None,
                        "max_depth": 400,
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
                        "intervals":    ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d", "1w", "1M"],
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
                        "depths":    None,
                        "max_depth": 400,
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
                        "intervals":    ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d", "1w", "1M"],
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
                        "retention_ms": 30 * 24 * 60 * 60 * 1000,
                        "intervals":    ["5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"],
                    },
                    "liquidations": {
                        "rest":         True,
                        "ws":           True,
                        "paginated":    True,
                        "max_limit":    None,
                        "retention_ms": 7 * 24 * 60 * 60 * 1000,
                    },
                    "long_short_ratio": {
                        "rest":         True,
                        "ws":           False,
                        "paginated":    True,
                        "max_limit":    None,
                        "retention_ms": 30 * 24 * 60 * 60 * 1000,
                        "intervals":    ["5m", "1h", "1d"],
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
                        "depths":    None,
                        "max_depth": 400,
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
                        "intervals":    ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d", "1w", "1M"],
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
                        "retention_ms": 30 * 24 * 60 * 60 * 1000,
                        "intervals":    ["5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"],
                    },
                    "liquidations": {
                        "rest":         True,
                        "ws":           True,
                        "paginated":    True,
                        "max_limit":    None,
                        "retention_ms": 7 * 24 * 60 * 60 * 1000,
                    },
                    "long_short_ratio": {
                        "rest":         True,
                        "ws":           False,
                        "paginated":    True,
                        "max_limit":    None,
                        "retention_ms": 30 * 24 * 60 * 60 * 1000,
                        "intervals":    ["5m", "1h", "1d"],
                    },
                },
            },
        }


    def normalize_symbol(self, symbol: str) -> str:
        return symbol.upper().replace("-SWAP", "").replace("-", "").replace("/", "")


    def get_api_symbol(self, symbol: str, market_type: MarketType) -> str:
        s = self.normalize_symbol(symbol)
        if market_type == MarketType.LINEAR:
            for q in ("USDT", "USDC"):
                if s.endswith(q) and len(s) > len(q):
                    return f"{s[:-len(q)]}-{q}-SWAP"
            return f"{s}-USDT-SWAP"
        if market_type == MarketType.INVERSE:
            if s.endswith("USD") and len(s) > 3:
                return f"{s[:-3]}-USD-SWAP"
            return f"{s}-USD-SWAP"
        for q in self._learned_quotes or self.SPOT_QUOTES:
            if s.endswith(q) and len(s) > len(q):
                return f"{s[:-len(q)]}-{q}"
        return s


    def get_model_symbol(self, api_symbol: str, market_type: MarketType) -> str:
        return api_symbol.upper().replace("-SWAP", "").replace("-", "")


    async def _ensure_spot_symbol_map(self) -> None:
        if self._spot_symbol_map:
            return
        async with self._spot_symbol_map_lock:
            if self._spot_symbol_map:
                return
            data = await self._make_request("GET", "/api/v5/public/instruments", {"instType": "SPOT"})
            built: Dict[str, str] = {}
            quotes: set = set()
            for s in data or []:
                if s.get("state") != "live":
                    continue
                native = s.get("instId")
                base = s.get("baseCcy", "")
                quote = s.get("quoteCcy", "")
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
        logger.warning(f"{self.name}: symbol {flat} not in spot map; using SPOT_QUOTES heuristic.")
        return self.get_api_symbol(symbol, market_type)


    def _base_ccy(self, symbol: str, market_type: MarketType) -> str:
        s = self.normalize_symbol(symbol)
        if market_type == MarketType.INVERSE:
            return s[:-3] if s.endswith("USD") and len(s) > 3 else s
        for q in ("USDT", "USDC", "USD", "EUR", "GBP", "DAI"):
            if s.endswith(q) and len(s) > len(q):
                return s[:-len(q)]
        return s


    def _inst_type(self, market_type: MarketType) -> str:
        return "SPOT" if market_type == MarketType.SPOT else "SWAP"


    def _map_candle_interval(self, interval: str) -> str:
        if interval in {"1h", "2h", "4h", "6h", "8h", "12h", "1d", "1w"}:
            return interval.upper()
        return interval


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
        if unit == "M": return val * 30 * 24 * 60 * 60 * 1000
        return 0


    @staticmethod
    def _precision(value: str) -> int:
        if not value or "." not in value:
            return 0
        return len(value.split(".")[1].rstrip("0"))


    async def _make_request(self, method: str, endpoint: str, params: Optional[Dict] = None) -> Any:
        url = f"{self.rest_url}{endpoint}"
        retries = 3
        while retries > 0:
            now = time.time()
            if now < self._backoff_until:
                wait_time = self._backoff_until - now
                if wait_time > 30:
                    raise ValueError(f"OKX backoff active. Retry in {wait_time:.0f}s.")
                await asyncio.sleep(wait_time + 0.1)

            try:
                resp = await self.http_client.request(method, url, params=params)

                if resp.status_code == 429:
                    async with self._backoff_lock:
                        self._backoff_until = max(self._backoff_until, time.time() + 5)
                    logger.error("OKX rate limit (429). Backing off 5s.")
                    retries -= 1
                    continue

                if resp.status_code == 200:
                    body = resp.json()
                    code = body.get("code", "0")
                    if code != "0":
                        raise ValueError(f"OKX API Error ({code}): {body.get('msg', 'Unknown error')}")
                    return body.get("data", [])

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
            logger.warning(f"OKX pagination hit {max_requests}-page safety cap with {collected}/{total_limit} records; result truncated")

        chunks.reverse()
        out = [item for chunk in chunks for item in chunk]
        out.sort(key=lambda x: x.timestamp)
        return out[-total_limit:]


    async def _ws_connect(self, args: list) -> AsyncGenerator[Dict, None]:
        reconnect_delay = 1
        while True:
            try:
                async with websockets.connect(self.ws_url, ping_interval=None) as ws:
                    logger.info(f"OKX WS connected: {args}")
                    await ws.send(json.dumps({"op": "subscribe", "args": args}))
                    last_ping = time.time()
                    reconnect_delay = 1

                    while True:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=25)
                        except asyncio.TimeoutError:
                            await ws.send("ping")
                            last_ping = time.time()
                            continue

                        if msg == "pong":
                            continue

                        try:
                            data = json.loads(msg)
                        except json.JSONDecodeError:
                            continue

                        if data.get("event") == "error":
                            logger.error(f"OKX WS error: {data}")
                            continue
                        if "data" in data:
                            yield data

                        if time.time() - last_ping > 25:
                            await ws.send("ping")
                            last_ping = time.time()

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"OKX WS disconnected ({e}). Reconnecting in {reconnect_delay}s...")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 30)


    async def get_ticker(self, market_type: MarketType, symbol: str) -> Ticker:
        api_symbol = await self._resolve_symbol(symbol, market_type)

        data = await self._make_request("GET", "/api/v5/market/ticker", {"instId": api_symbol})
        if not data:
            raise ValueError(f"Symbol {api_symbol} not found")
        t = data[0]

        last = float(t.get("last") or 0)
        open_24h = float(t.get("open24h") or 0)
        pct = ((last - open_24h) / open_24h * 100) if open_24h else 0.0

        return Ticker(
            symbol=self.get_model_symbol(t["instId"], market_type),
            price=last,
            open_24h=open_24h,
            high_24h=float(t.get("high24h") or 0),
            low_24h=float(t.get("low24h") or 0),
            volume_24h=float(t.get("vol24h") or 0),
            quote_volume_24h=float(t.get("volCcy24h") or 0),
            price_change_percent=pct,
            timestamp=self.normalize_timestamp(t["ts"]),
        )


    async def get_book_ticker(self, market_type: MarketType, symbol: str) -> BookTicker:
        api_symbol = await self._resolve_symbol(symbol, market_type)

        data = await self._make_request("GET", "/api/v5/market/ticker", {"instId": api_symbol})
        if not data:
            raise ValueError(f"Symbol {api_symbol} not found")
        t = data[0]

        return BookTicker(
            symbol=self.get_model_symbol(t["instId"], market_type),
            bid_price=float(t.get("bidPx") or 0),
            bid_qty=float(t.get("bidSz") or 0),
            ask_price=float(t.get("askPx") or 0),
            ask_qty=float(t.get("askSz") or 0),
            timestamp=self.normalize_timestamp(t["ts"]),
        )


    async def get_orderbook(self, market_type: MarketType, symbol: str, depth: int = 20) -> OrderBook:
        api_symbol = await self._resolve_symbol(symbol, market_type)
        max_depth = self.get_capabilities()["markets"][market_type]["orderbook"]["max_depth"]
        sz = max(1, min(depth, max_depth))

        data = await self._make_request("GET", "/api/v5/market/books", {"instId": api_symbol, "sz": sz})
        if not data:
            raise ValueError(f"Orderbook not found for {api_symbol}")
        b = data[0]

        return OrderBook(
            symbol=self.get_model_symbol(api_symbol, market_type),
            bids=[[float(p), float(q)] for p, q, *_ in b.get("bids", [])],
            asks=[[float(p), float(q)] for p, q, *_ in b.get("asks", [])],
            timestamp=self.normalize_timestamp(b["ts"]),
        )


    async def get_trades(self, market_type: MarketType, symbol: str, limit: int = 100) -> List[Trade]:
        api_symbol = await self._resolve_symbol(symbol, market_type)
        max_limit = self.get_capabilities()["markets"][market_type]["trades"]["max_limit"]
        req_limit = min(limit, max_limit)

        data = await self._make_request("GET", "/api/v5/market/trades", {"instId": api_symbol, "limit": req_limit})

        trades = [Trade(
            id=str(t["tradeId"]),
            price=float(t["px"]),
            qty=float(t["sz"]),
            side=t["side"],
            timestamp=self.normalize_timestamp(t["ts"]),
        ) for t in data]
        trades.sort(key=lambda t: t.timestamp)

        return trades[-limit:]


    async def get_agg_trades(self, market_type: MarketType, symbol: str, start_time: Optional[int] = None, limit: int = 500) -> List[AggTrade]:
        raise NotImplementedError(f"{self.name} does not support agg_trades for {market_type.value}")


    async def get_candles(self, market_type: MarketType, symbol: str, interval: str, start_time: Optional[int] = None, limit: int = 100) -> List[Candle]:
        api_symbol = await self._resolve_symbol(symbol, market_type)
        bar = self._map_candle_interval(interval)
        per_req = 300

        def _parse(rows):
            out = []
            if not isinstance(rows, list):
                return out
            for r in rows:
                if not isinstance(r, list) or len(r) < 6:
                    continue
                try:
                    ts = self.normalize_timestamp(r[0])
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
                except (ValueError, TypeError, KeyError):
                    continue
            return sorted(out, key=lambda x: x.timestamp)

        async def fetch_recent(end_ts, l):
            anchor = end_ts if end_ts is not None else start_time
            p = {"instId": api_symbol, "bar": bar, "limit": min(l, per_req)}
            if anchor:
                p["after"] = str(anchor + 1)
            return _parse(await self._make_request("GET", "/api/v5/market/candles", p))

        async def fetch_history(end_ts, l):
            anchor = end_ts if end_ts is not None else start_time
            p = {"instId": api_symbol, "bar": bar, "limit": min(l, 100)}
            if anchor:
                p["after"] = str(anchor + 1)
            return _parse(await self._make_request("GET", "/api/v5/market/history-candles", p))

        if start_time is None and limit <= per_req:
            return (await fetch_recent(None, limit))[-limit:]

        recent_batch = await fetch_recent(None, min(limit, per_req))
        if not recent_batch or len(recent_batch) >= limit:
            if recent_batch:
                return recent_batch[-limit:]
            return await self._paginate_backwards(fetch_history, limit, 100)

        oldest_recent = recent_batch[0].timestamp
        history_batch = await self._paginate_backwards(
            lambda end_ts, l: fetch_history(end_ts if end_ts is not None else oldest_recent - 1, l),
            limit - len(recent_batch),
            100,
        )
        combined = sorted({c.timestamp: c for c in history_batch + recent_batch}.values(), key=lambda x: x.timestamp)
        return combined[-limit:]


    async def get_mark_price(self, market_type: MarketType, symbol: str) -> MarkPrice:
        if market_type == MarketType.SPOT:
            raise NotImplementedError(f"{self.name} does not support mark_price for {market_type.value}")
        api_symbol = await self._resolve_symbol(symbol, market_type)
        mark_data = await self._make_request("GET", "/api/v5/public/mark-price", {"instType": "SWAP", "instId": api_symbol})
        if not mark_data:
            raise ValueError(f"Mark price not found for {api_symbol}")
        m = mark_data[0]

        funding_rate = 0.0
        next_funding = 0
        try:
            f_data = await self._make_request("GET", "/api/v5/public/funding-rate", {"instId": api_symbol})
            if f_data:
                f = f_data[0]
                funding_rate = float(f.get("fundingRate") or 0)
                next_funding = self.normalize_timestamp(f.get("nextFundingTime") or 0)
        except (httpx.HTTPStatusError, ValueError):
            pass

        index_price = 0.0
        try:
            i_data = await self._make_request("GET", "/api/v5/market/index-tickers", {"instId": api_symbol.replace("-SWAP", "")})
            if i_data:
                index_price = float(i_data[0].get("idxPx") or 0)
        except (httpx.HTTPStatusError, ValueError):
            pass

        return MarkPrice(
            symbol=self.get_model_symbol(m["instId"], market_type),
            mark_price=float(m["markPx"]),
            index_price=index_price,
            funding_rate=funding_rate,
            next_funding_time=next_funding,
            timestamp=self.normalize_timestamp(m["ts"]),
        )


    async def get_funding_rate(self, market_type: MarketType, symbol: str, start_time: Optional[int] = None, limit: int = 100) -> List[FundingRate]:
        if market_type == MarketType.SPOT:
            raise NotImplementedError(f"{self.name} does not support funding_rate for {market_type.value}")
        api_symbol = await self._resolve_symbol(symbol, market_type)
        per_req = 100

        def _parse(rows, sym):
            return sorted([FundingRate(
                symbol=sym,
                rate=float(r["fundingRate"]),
                timestamp=self.normalize_timestamp(r["fundingTime"]),
            ) for r in rows], key=lambda x: x.timestamp)

        model_sym = self.get_model_symbol(api_symbol, market_type)

        async def fetch_backward(end_ts, l):
            anchor = end_ts if end_ts is not None else start_time
            p = {"instId": api_symbol, "limit": min(l, per_req)}
            if anchor:
                p["after"] = str(anchor + 1)
            return _parse(await self._make_request("GET", "/api/v5/public/funding-rate-history", p), model_sym)

        if start_time is not None or limit > per_req:
            return await self._paginate_backwards(fetch_backward, limit, per_req)
        return (await fetch_backward(None, limit))[-limit:]


    async def get_open_interest(self, market_type: MarketType, symbol: str, period: str = "1h", start_time: Optional[int] = None, limit: int = 30) -> List[OpenInterest]:
        if market_type == MarketType.SPOT:
            raise NotImplementedError(f"{self.name} does not support open_interest for {market_type.value}")
        api_symbol = await self._resolve_symbol(symbol, market_type)
        model_sym = self.get_model_symbol(api_symbol, market_type)
        bar = self._map_candle_interval(period)
        per_req = 100

        def _parse(rows):
            out = []
            for r in rows:
                if not isinstance(r, list) or len(r) < 2:
                    continue
                try:
                    ts = self.normalize_timestamp(r[0])
                    if ts <= 0:
                        continue
                    oi = float(r[2]) if len(r) > 2 else float(r[1])
                    usd = float(r[3]) if len(r) > 3 else 0.0
                    out.append(OpenInterest(
                        symbol=model_sym,
                        open_interest=oi,
                        value_usd=usd,
                        timestamp=ts,
                    ))
                except (ValueError, TypeError):
                    continue
            return sorted(out, key=lambda x: x.timestamp)

        async def fetch_backward(end_ts, l):
            anchor = end_ts if end_ts is not None else start_time
            p = {"instId": api_symbol, "period": bar, "limit": min(l, per_req)}
            if anchor:
                p["end"] = str(anchor)
            return _parse(await self._make_request("GET", "/api/v5/rubik/stat/contracts/open-interest-history", p))

        if start_time is not None or limit > per_req:
            return await self._paginate_backwards(fetch_backward, limit, per_req)
        return (await fetch_backward(None, limit))[-limit:]


    async def get_liquidations(self, market_type: MarketType, symbol: str, start_time: Optional[int] = None, limit: int = 100) -> List[Liquidation]:
        if market_type == MarketType.SPOT:
            raise NotImplementedError(f"{self.name} does not support liquidations for {market_type.value}")
        api_symbol = await self._resolve_symbol(symbol, market_type)
        model_sym = self.get_model_symbol(api_symbol, market_type)
        uly = api_symbol.replace("-SWAP", "")
        per_req = 100

        def _parse(rows):
            out = []
            for entry in rows:
                if entry.get("instId") != api_symbol:
                    continue
                for d in entry.get("details", []):
                    side = d.get("side", "").lower()
                    if side not in ("buy", "sell"):
                        continue
                    out.append(Liquidation(
                        symbol=model_sym,
                        side=side,
                        price=float(d["bkPx"]),
                        qty=float(d["sz"]),
                        timestamp=self.normalize_timestamp(d["ts"]),
                    ))
            return sorted(out, key=lambda x: x.timestamp)

        async def fetch_backward(end_ts, l):
            anchor = end_ts if end_ts is not None else start_time
            p = {"instType": "SWAP", "uly": uly, "state": "filled", "limit": min(l, per_req)}
            if anchor:
                p["after"] = str(anchor + 1)
            try:
                rows = await self._make_request("GET", "/api/v5/public/liquidation-orders", p)
            except (httpx.HTTPStatusError, ValueError):
                return []
            return _parse(rows)

        if start_time is not None or limit > per_req:
            return await self._paginate_backwards(fetch_backward, limit, per_req)
        return (await fetch_backward(None, limit))[-limit:]


    async def get_long_short_ratio(self, market_type: MarketType, symbol: str, period: str = "5m", start_time: Optional[int] = None, limit: int = 30) -> List[LongShortRatio]:
        if market_type == MarketType.SPOT:
            raise NotImplementedError(f"{self.name} does not support long_short_ratio for {market_type.value}")
        ccy = self._base_ccy(symbol, market_type)
        model_sym = self.get_model_symbol(self.get_api_symbol(symbol, market_type), market_type)
        bar = self._map_candle_interval(period)
        per_req = 100

        def _parse(rows):
            out = []
            for r in rows:
                if not isinstance(r, list) or len(r) < 2:
                    continue
                try:
                    ts = self.normalize_timestamp(r[0])
                    if ts <= 0:
                        continue
                    ratio = float(r[1])
                    long_acct = ratio / (ratio + 1) if ratio > 0 else 0.0
                    short_acct = 1 / (ratio + 1) if ratio > 0 else 0.0
                    out.append(LongShortRatio(
                        symbol=model_sym,
                        ratio=ratio,
                        long_account=long_acct,
                        short_account=short_acct,
                        timestamp=ts,
                    ))
                except (ValueError, TypeError):
                    continue
            return sorted(out, key=lambda x: x.timestamp)

        async def fetch_backward(end_ts, l):
            anchor = end_ts if end_ts is not None else start_time
            p = {"ccy": ccy, "period": bar, "limit": min(l, per_req)}
            if anchor:
                p["end"] = str(anchor)
            return _parse(await self._make_request("GET", "/api/v5/rubik/stat/contracts/long-short-account-ratio", p))

        if start_time is not None or limit > per_req:
            return await self._paginate_backwards(fetch_backward, limit, per_req)
        return (await fetch_backward(None, limit))[-limit:]


    async def get_exchange_info(self, market_type: MarketType) -> List[SymbolInfo]:
        params = {"instType": self._inst_type(market_type)}

        data = await self._make_request("GET", "/api/v5/public/instruments", params)

        results = []
        for s in data:
            if s.get("state") != "live":
                continue
            if market_type == MarketType.LINEAR and s.get("ctType") != "linear":
                continue
            if market_type == MarketType.INVERSE and s.get("ctType") != "inverse":
                continue

            api_sym = s["instId"]

            if market_type == MarketType.SPOT:
                base = s.get("baseCcy", "")
                quote = s.get("quoteCcy", "")
            else:
                uly_parts = s.get("uly", "").split("-")
                if len(uly_parts) >= 2:
                    base, quote = uly_parts[0], uly_parts[1]
                else:
                    base, quote = "", ""

            results.append(SymbolInfo(
                symbol=self.get_model_symbol(api_sym, market_type),
                native_symbol=api_sym,
                base_asset=base,
                quote_asset=quote,
                price_precision=self._precision(s.get("tickSz", "0")),
                quantity_precision=self._precision(s.get("lotSz", "0")),
                min_qty=float(s.get("minSz") or 0),
                max_qty=float(s.get("maxLmtSz") or s.get("maxMktSz") or 0),
                min_notional=0.0,
            ))
        return results


    async def get_markets(self, market_type: MarketType) -> List[str]:
        info = await self.get_exchange_info(market_type)
        return [s.symbol for s in info]


    async def stream_ticker(self, market_type: MarketType, symbol: str) -> AsyncGenerator[Ticker, None]:
        api_symbol = await self._resolve_symbol(symbol, market_type)
        model_sym = self.get_model_symbol(api_symbol, market_type)

        async for msg in self._ws_connect([{"channel": "tickers", "instId": api_symbol}]):
            for t in msg.get("data", []):
                last = float(t.get("last") or 0)
                open_24h = float(t.get("open24h") or 0)
                pct = ((last - open_24h) / open_24h * 100) if open_24h else 0.0
                yield Ticker(
                    symbol=model_sym,
                    price=last,
                    open_24h=open_24h,
                    high_24h=float(t.get("high24h") or 0),
                    low_24h=float(t.get("low24h") or 0),
                    volume_24h=float(t.get("vol24h") or 0),
                    quote_volume_24h=float(t.get("volCcy24h") or 0),
                    price_change_percent=pct,
                    timestamp=self.normalize_timestamp(t["ts"]),
                )


    async def stream_book_ticker(self, market_type: MarketType, symbol: str) -> AsyncGenerator[BookTicker, None]:
        api_symbol = await self._resolve_symbol(symbol, market_type)
        model_sym = self.get_model_symbol(api_symbol, market_type)

        async for msg in self._ws_connect([{"channel": "bbo-tbt", "instId": api_symbol}]):
            for b in msg.get("data", []):
                bids = b.get("bids", [])
                asks = b.get("asks", [])
                if not bids or not asks:
                    continue
                yield BookTicker(
                    symbol=model_sym,
                    bid_price=float(bids[0][0]),
                    bid_qty=float(bids[0][1]),
                    ask_price=float(asks[0][0]),
                    ask_qty=float(asks[0][1]),
                    timestamp=self.normalize_timestamp(b["ts"]),
                )


    async def stream_orderbook(self, market_type: MarketType, symbol: str, depth: int = 20, update_speed: str = "100ms") -> AsyncGenerator[OrderBook, None]:
        api_symbol = await self._resolve_symbol(symbol, market_type)
        model_sym = self.get_model_symbol(api_symbol, market_type)
        channel = "bbo-tbt" if depth <= 1 else "books5" if depth <= 5 else "books"

        async for msg in self._ws_connect([{"channel": channel, "instId": api_symbol}]):
            for b in msg.get("data", []):
                yield OrderBook(
                    symbol=model_sym,
                    bids=[[float(p), float(q)] for p, q, *_ in b.get("bids", [])],
                    asks=[[float(p), float(q)] for p, q, *_ in b.get("asks", [])],
                    timestamp=self.normalize_timestamp(b["ts"]),
                )


    async def stream_trades(self, market_type: MarketType, symbol: str) -> AsyncGenerator[Trade, None]:
        api_symbol = await self._resolve_symbol(symbol, market_type)

        async for msg in self._ws_connect([{"channel": "trades", "instId": api_symbol}]):
            for t in msg.get("data", []):
                yield Trade(
                    id=str(t["tradeId"]),
                    price=float(t["px"]),
                    qty=float(t["sz"]),
                    side=t["side"],
                    timestamp=self.normalize_timestamp(t["ts"]),
                )


    async def stream_mark_price(self, market_type: MarketType, symbol: str) -> AsyncGenerator[MarkPrice, None]:
        if market_type == MarketType.SPOT:
            raise NotImplementedError(f"{self.name} does not support stream_mark_price for {market_type.value}")
        api_symbol = await self._resolve_symbol(symbol, market_type)
        model_sym = self.get_model_symbol(api_symbol, market_type)

        async for msg in self._ws_connect([{"channel": "mark-price", "instId": api_symbol}]):
            for m in msg.get("data", []):
                yield MarkPrice(
                    symbol=model_sym,
                    mark_price=float(m["markPx"]),
                    index_price=0.0,
                    funding_rate=0.0,
                    next_funding_time=0,
                    timestamp=self.normalize_timestamp(m["ts"]),
                )


    async def stream_liquidations(self, market_type: MarketType, symbol: str) -> AsyncGenerator[Liquidation, None]:
        if market_type == MarketType.SPOT:
            raise NotImplementedError(f"{self.name} does not support stream_liquidations for {market_type.value}")
        api_symbol = await self._resolve_symbol(symbol, market_type)
        model_sym = self.get_model_symbol(api_symbol, market_type)

        async for msg in self._ws_connect([{"channel": "liquidation-orders", "instType": "SWAP"}]):
            for entry in msg.get("data", []):
                if entry.get("instId") != api_symbol:
                    continue
                for d in entry.get("details", []):
                    side = d.get("side", "").lower()
                    if side not in ("buy", "sell"):
                        continue
                    yield Liquidation(
                        symbol=model_sym,
                        side=side,
                        price=float(d["bkPx"]),
                        qty=float(d["sz"]),
                        timestamp=self.normalize_timestamp(d["ts"]),
                    )
