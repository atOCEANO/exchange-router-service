import httpx
import asyncio
import collections
import random
import time
import websockets
import logging
from typing import List, AsyncGenerator, Dict, Any, Optional, Callable, Deque
from src.exchanges.base import (
    BaseExchange, StreamHub, UpstreamUnavailableError,
    build_qty_value, build_volume_value, build_oi_value,
    build_funding_current, build_funding_historical, build_funding_convention,
)
from src.models import (
    Ticker, BookTicker, MarkPrice, OrderBook, Candle, Trade, AggTrade,
    MarketType, SymbolInfo, OpenInterest, FundingRate, Liquidation, LongShortRatio,
)


logger = logging.getLogger("okx_adapter")


class OkxAdapter(BaseExchange):
    SPOT_QUOTES = ["USDT", "USDC", "USD", "BTC", "ETH", "EUR", "GBP", "DAI"]


    class _RateBucket:

        def __init__(self, max_requests: int, window_s: float):
            self.max         = max_requests
            self.window      = window_s
            self.timestamps: Deque[float] = collections.deque()
            self.lock        = asyncio.Lock()


        async def acquire(self) -> None:
            while True:
                async with self.lock:
                    now = time.time()
                    while self.timestamps and self.timestamps[0] < now - self.window:
                        self.timestamps.popleft()
                    if len(self.timestamps) < self.max:
                        self.timestamps.append(now)
                        return
                    wait = self.timestamps[0] + self.window - now
                await asyncio.sleep(max(wait, 0.01) + random.uniform(0, 0.05))


    def __init__(self):
        super().__init__()
        self.http_client = httpx.AsyncClient(
            timeout=60.0,
            limits=httpx.Limits(max_connections=50, max_keepalive_connections=20),
        )

        self._backoff_until = 0.0
        self._backoff_lock = asyncio.Lock()
        self._request_sem = asyncio.Semaphore(40)

        self._bucket_rubik   = self._RateBucket(max_requests=4, window_s=2.0)
        self._bucket_default = self._RateBucket(max_requests=18, window_s=2.0)

        self.rest_url = "https://www.okx.com"
        self.ws_url = "wss://ws.okx.com:8443/ws/v5/public"

        self._spot_symbol_map: Dict[str, str] = {}
        self._spot_symbol_map_lock = asyncio.Lock()
        self._learned_quotes: List[str] = []

        self._hub: Optional[StreamHub] = None
        self._hub_lock = asyncio.Lock()
        self._topic_args: Dict[str, Dict[str, Any]] = {}

        self._funding_interval_cache: Dict[str, int] = {}
        self._funding_interval_lock = asyncio.Lock()

        self._capabilities = self._build_capabilities()


    async def shutdown(self):
        if self._hub is not None:
            await self._hub.close()
            self._hub = None
        await self.http_client.aclose()


    async def _warm(self) -> None:
        await self._step("linear_info",     self._ensure_info_cache(MarketType.LINEAR))
        await self._step("inverse_info",    self._ensure_info_cache(MarketType.INVERSE))
        await self._step("linear_funding",  self._warm_funding_intervals(MarketType.LINEAR))
        await self._step("inverse_funding", self._warm_funding_intervals(MarketType.INVERSE))


    async def _warm_funding_intervals(self, market_type: MarketType) -> None:
        cache = await self._ensure_info_cache(market_type)
        sem   = asyncio.Semaphore(2)
        tasks = [self._warm_one_funding_interval(info, sem) for info in cache.values()]

        await asyncio.gather(*tasks, return_exceptions=True)


    async def _warm_one_funding_interval(self, info: SymbolInfo, sem: asyncio.Semaphore) -> None:
        async with sem:
            try:
                await self._funding_interval_ms_for(info.native_symbol)
            except Exception as e:
                logger.warning(f"funding interval lookup failed for {info.native_symbol}: {e}")


    @staticmethod
    def _topic_key_for_arg(arg: Dict[str, Any]) -> str:
        ch = arg.get("channel", "")
        val = arg.get("instId") or arg.get("instType") or ""
        return f"{ch}:{val}"


    async def _get_hub(self) -> StreamHub:
        async with self._hub_lock:
            if self._hub is None:
                async def _connect():
                    return await websockets.connect(self.ws_url, ping_interval=None)
                def _sub(topics: List[str]):
                    args = [self._topic_args[t] for t in topics if t in self._topic_args]
                    return [{"op": "subscribe", "args": args}] if args else []
                def _unsub(topics: List[str]):
                    args = [self._topic_args[t] for t in topics if t in self._topic_args]
                    return [{"op": "unsubscribe", "args": args}] if args else []
                def _route(msg):
                    if not isinstance(msg, dict):
                        return None
                    arg = msg.get("arg")
                    if not isinstance(arg, dict) or "data" not in msg:
                        return None
                    return self._topic_key_for_arg(arg)
                self._hub = StreamHub(
                    name="okx_public",
                    connect=_connect,
                    subscribe_payload=_sub,
                    unsubscribe_payload=_unsub,
                    route=_route,
                    keepalive_payload="ping",
                    keepalive_interval=25.0,
                )
            return self._hub


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
                    "mark_price": {
                        "rest": False,
                        "ws":   False,
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
                        "completeness": None,
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
                    "mark_price": {
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
                    "funding_rate": {
                        "rest":         True,
                        "ws":           False,
                        "paginated":    True,
                        "max_limit":    300,
                        "retention_ms": 90 * 24 * 60 * 60 * 1000,
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
                        "max_limit":    100,
                        "retention_ms": 24 * 60 * 60 * 1000,
                        "completeness": "partial",
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
                    "mark_price": {
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
                    "funding_rate": {
                        "rest":         True,
                        "ws":           False,
                        "paginated":    True,
                        "max_limit":    300,
                        "retention_ms": 90 * 24 * 60 * 60 * 1000,
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
                        "max_limit":    100,
                        "retention_ms": 24 * 60 * 60 * 1000,
                        "completeness": "partial",
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


    async def _funding_interval_ms_for(self, inst_id: str) -> Optional[int]:
        async with self._funding_interval_lock:
            if inst_id in self._funding_interval_cache:
                return self._funding_interval_cache[inst_id]
        try:
            data = await self._make_request("GET", "/api/v5/public/funding-rate", {"instId": inst_id})
        except Exception as e:
            logger.warning(f"OKX funding-rate lookup failed for {inst_id}: {e}; assuming 8h default")
            return 8 * 3_600_000
        items = data if isinstance(data, list) else []
        if not items:
            return 8 * 3_600_000
        try:
            next_ms = int(items[0].get("nextFundingTime", 0))
            cur_ms = int(items[0].get("fundingTime", 0))
            interval = next_ms - cur_ms if next_ms > cur_ms > 0 else 8 * 3_600_000
        except (TypeError, ValueError):
            interval = 8 * 3_600_000
        async with self._funding_interval_lock:
            self._funding_interval_cache[inst_id] = interval
        return interval


    async def _make_request(self, method: str, endpoint: str, params: Optional[Dict] = None) -> Any:
        url = f"{self.rest_url}{endpoint}"
        bucket = self._bucket_rubik if "/rubik/stat/" in endpoint else self._bucket_default
        retries = 3

        while retries > 0:
            now = time.time()
            if now < self._backoff_until:
                wait_time = self._backoff_until - now
                if wait_time > 60:
                    raise UpstreamUnavailableError(f"OKX backoff active. Retry in {wait_time:.0f}s.", retry_after=wait_time)
                await asyncio.sleep(wait_time + random.uniform(0.05, 1.0))

            try:
                async with self._request_sem:
                    await bucket.acquire()
                    resp = await self.http_client.request(method, url, params=params)

                if resp.status_code == 429:
                    retry_after_hdr = resp.headers.get("Retry-After")
                    try:
                        wait_s = max(float(retry_after_hdr), 2.0) if retry_after_hdr else 10.0
                    except (TypeError, ValueError):
                        wait_s = 10.0
                    async with self._backoff_lock:
                        self._backoff_until = max(self._backoff_until, time.time() + wait_s)
                    logger.warning(f"OKX rate limit (429) on {endpoint}. Backing off {wait_s:.1f}s.")
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
                if e.response.status_code in (502, 503, 504, 520):
                    logger.warning(f"OKX HTTP {e.response.status_code} on {endpoint}. Retrying...")
                    retries -= 1
                    await asyncio.sleep(1)
                    continue
                logger.error(f"HTTP Error: {e}")
                raise e
            except Exception as e:
                if isinstance(e, ValueError):
                    raise e
                logger.error(f"Request Error: {e}")
                retries -= 1
                await asyncio.sleep(1)

        raise UpstreamUnavailableError(f"Max retries exceeded for {url}")


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
            next_end = batch[0].timestamp
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
        arg = args[0]
        topic = self._topic_key_for_arg(arg)
        self._topic_args[topic] = arg

        hub = await self._get_hub()
        q = await hub.subscribe(topic)
        try:
            while True:
                msg = await q.get()
                yield msg
        finally:
            await hub.unsubscribe(topic, q)


    async def get_ticker(self, market_type: MarketType, symbol: str) -> Ticker:
        api_symbol = await self._resolve_symbol(symbol, market_type)

        data = await self._make_request("GET", "/api/v5/market/ticker", {"instId": api_symbol})
        if not data:
            raise ValueError(f"Symbol {api_symbol} not found")
        t = data[0]

        last     = float(t.get("last") or 0)
        open_24h = float(t.get("open24h") or 0)
        pct      = ((last - open_24h) / open_24h * 100) if open_24h else 0.0

        model_sym     = self.get_model_symbol(t["instId"], market_type)
        info          = await self._info_for(market_type, model_sym)
        quote         = info.quote_asset if info else ""
        contract_size = info.contract_size if info else None

        if market_type == MarketType.LINEAR:
            vol_native = float(t.get("volCcy24h") or 0)
            vol_unit   = "base"
            vol_cs     = None
        elif market_type == MarketType.INVERSE:
            vol_native = float(t.get("vol24h") or 0)
            vol_unit   = "contract"
            vol_cs     = contract_size
        else:
            vol_native = float(t.get("vol24h") or 0)
            vol_unit   = "base"
            vol_cs     = None

        return Ticker(
            symbol               = model_sym,
            market_type          = market_type,
            quote                = quote,
            price                = last,
            open_24h             = open_24h,
            high_24h             = float(t.get("high24h") or 0),
            low_24h              = float(t.get("low24h") or 0),
            volume_24h           = build_volume_value(
                native        = vol_native,
                volume_unit   = vol_unit,
                contract_size = vol_cs,
                close         = last,
            ),
            price_change_percent = pct,
            timestamp            = self.normalize_timestamp(t["ts"]),
        )


    async def get_book_ticker(self, market_type: MarketType, symbol: str) -> BookTicker:
        api_symbol = await self._resolve_symbol(symbol, market_type)

        data = await self._make_request("GET", "/api/v5/market/ticker", {"instId": api_symbol})
        if not data:
            raise ValueError(f"Symbol {api_symbol} not found")
        t = data[0]

        model_sym     = self.get_model_symbol(t["instId"], market_type)
        info          = await self._info_for(market_type, model_sym)
        quote         = info.quote_asset if info else ""
        contract_size = info.contract_size if info else None
        bid_price     = float(t.get("bidPx") or 0)
        ask_price     = float(t.get("askPx") or 0)
        bid_raw       = float(t.get("bidSz") or 0)
        ask_raw       = float(t.get("askSz") or 0)

        if market_type == MarketType.LINEAR and contract_size is None:
            raise ValueError(f"Contract size unavailable for {model_sym} on OKX linear")

        if market_type == MarketType.LINEAR:
            bid_native = bid_raw * contract_size
            ask_native = ask_raw * contract_size
            qty_unit   = "base"
            qty_cs     = None
        elif market_type == MarketType.INVERSE:
            bid_native = bid_raw
            ask_native = ask_raw
            qty_unit   = "contract"
            qty_cs     = contract_size
        else:
            bid_native = bid_raw
            ask_native = ask_raw
            qty_unit   = "base"
            qty_cs     = None

        return BookTicker(
            symbol      = model_sym,
            market_type = market_type,
            quote       = quote,
            bid_price   = bid_price,
            bid_qty     = build_qty_value(
                native        = bid_native,
                qty_unit      = qty_unit,
                contract_size = qty_cs,
                price         = bid_price,
            ),
            ask_price   = ask_price,
            ask_qty     = build_qty_value(
                native        = ask_native,
                qty_unit      = qty_unit,
                contract_size = qty_cs,
                price         = ask_price,
            ),
            timestamp   = self.normalize_timestamp(t["ts"]),
        )


    async def get_orderbook(self, market_type: MarketType, symbol: str, depth: int = 20) -> OrderBook:
        api_symbol = await self._resolve_symbol(symbol, market_type)
        max_depth = self.get_capabilities()["markets"][market_type]["orderbook"]["max_depth"]
        sz = max(1, min(depth, max_depth))

        data = await self._make_request("GET", "/api/v5/market/books", {"instId": api_symbol, "sz": sz})
        if not data:
            raise ValueError(f"Orderbook not found for {api_symbol}")
        b = data[0]

        model_sym     = self.get_model_symbol(api_symbol, market_type)
        info          = await self._info_for(market_type, model_sym)
        quote         = info.quote_asset if info else ""
        contract_size = info.contract_size if info else None

        if market_type == MarketType.LINEAR and contract_size is None:
            raise ValueError(f"Contract size unavailable for {model_sym} on OKX linear")

        if market_type == MarketType.LINEAR:
            mult      = contract_size
            bids_rows = [[float(p), float(q) * mult] for p, q, *_ in b.get("bids", [])]
            asks_rows = [[float(p), float(q) * mult] for p, q, *_ in b.get("asks", [])]
            ob_qty_unit = "base"
        elif market_type == MarketType.INVERSE:
            bids_rows = [[float(p), float(q)] for p, q, *_ in b.get("bids", [])]
            asks_rows = [[float(p), float(q)] for p, q, *_ in b.get("asks", [])]
            ob_qty_unit = "contract"
        else:
            bids_rows = [[float(p), float(q)] for p, q, *_ in b.get("bids", [])]
            asks_rows = [[float(p), float(q)] for p, q, *_ in b.get("asks", [])]
            ob_qty_unit = "base"

        return OrderBook(
            symbol      = model_sym,
            market_type = market_type,
            quote       = quote,
            bids        = bids_rows,
            asks        = asks_rows,
            qty_unit    = ob_qty_unit,
            timestamp   = self.normalize_timestamp(b["ts"]),
        )


    async def get_trades(self, market_type: MarketType, symbol: str, limit: int = 100) -> List[Trade]:
        api_symbol = await self._resolve_symbol(symbol, market_type)
        max_limit  = self.get_capabilities()["markets"][market_type]["trades"]["max_limit"]
        req_limit  = min(limit, max_limit)

        data = await self._make_request("GET", "/api/v5/market/trades", {"instId": api_symbol, "limit": req_limit})

        model_sym     = self.get_model_symbol(api_symbol, market_type)
        info          = await self._info_for(market_type, model_sym)
        quote         = info.quote_asset if info else ""
        contract_size = info.contract_size if info else None

        if market_type == MarketType.LINEAR and contract_size is None:
            raise ValueError(f"Contract size unavailable for {model_sym} on OKX linear")

        trades = []
        for t in data:
            price    = float(t["px"])
            sz_raw   = float(t["sz"])
            if market_type == MarketType.LINEAR:
                native = sz_raw * contract_size
                qty_unit_local = "base"
                qty_cs = None
            elif market_type == MarketType.INVERSE:
                native = sz_raw
                qty_unit_local = "contract"
                qty_cs = contract_size
            else:
                native = sz_raw
                qty_unit_local = "base"
                qty_cs = None
            trades.append(Trade(
                id          = str(t["tradeId"]),
                symbol      = model_sym,
                market_type = market_type,
                quote       = quote,
                price       = price,
                qty         = build_qty_value(
                    native        = native,
                    qty_unit      = qty_unit_local,
                    contract_size = qty_cs,
                    price         = price,
                ),
                side        = t["side"],
                timestamp   = self.normalize_timestamp(t["ts"]),
            ))
        trades.sort(key=lambda t: t.timestamp)

        return trades[-limit:]


    async def get_agg_trades(self, market_type: MarketType, symbol: str, start_time: Optional[int] = None, limit: int = 500) -> List[AggTrade]:
        raise NotImplementedError(f"{self.name} does not support agg_trades for {market_type.value}")


    async def get_candles(self, market_type: MarketType, symbol: str, interval: str, start_time: Optional[int] = None, limit: int = 100) -> List[Candle]:
        api_symbol    = await self._resolve_symbol(symbol, market_type)
        bar           = self._map_candle_interval(interval)
        per_req       = 300
        model_sym     = self.get_model_symbol(api_symbol, market_type)
        info          = await self._info_for(market_type, model_sym)
        quote         = info.quote_asset if info else ""
        contract_size = info.contract_size if info else None

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
                    close = float(r[4])
                    if market_type == MarketType.LINEAR and len(r) >= 7:
                        vol_native = float(r[6])
                        vol_unit_local = "base"
                        vol_cs = None
                    elif market_type == MarketType.INVERSE:
                        vol_native = float(r[5])
                        vol_unit_local = "contract"
                        vol_cs = contract_size
                    else:
                        vol_native = float(r[5])
                        vol_unit_local = "base"
                        vol_cs = None
                    out.append(Candle(
                        symbol      = model_sym,
                        market_type = market_type,
                        quote       = quote,
                        interval    = interval,
                        timestamp   = ts,
                        open        = float(r[1]),
                        high        = float(r[2]),
                        low         = float(r[3]),
                        close       = close,
                        volume      = build_volume_value(
                            native        = vol_native,
                            volume_unit   = vol_unit_local,
                            contract_size = vol_cs,
                            close         = close,
                        ),
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
        mark_data  = await self._make_request("GET", "/api/v5/public/mark-price", {"instType": "SWAP", "instId": api_symbol})
        if not mark_data:
            raise ValueError(f"Mark price not found for {api_symbol}")
        m = mark_data[0]

        model_sym = self.get_model_symbol(m["instId"], market_type)
        info      = await self._info_for(market_type, model_sym)
        quote     = info.quote_asset if info else ""
        cycle_ms  = await self._funding_interval_ms_for(api_symbol)

        funding_rate: Optional[float] = None
        next_funding: Optional[int] = None
        try:
            f_data = await self._make_request("GET", "/api/v5/public/funding-rate", {"instId": api_symbol})
            if f_data:
                f = f_data[0]
                rate_raw = f.get("fundingRate")
                if rate_raw is not None:
                    funding_rate = float(rate_raw)
                nft_raw = f.get("nextFundingTime") or 0
                if nft_raw:
                    nft_norm = self.normalize_timestamp(nft_raw)
                    if nft_norm > 0:
                        next_funding = nft_norm
        except (httpx.HTTPStatusError, ValueError):
            pass

        index_price: Optional[float] = None
        try:
            i_data = await self._make_request("GET", "/api/v5/market/index-tickers", {"instId": api_symbol.replace("-SWAP", "")})
            if i_data:
                idx_raw = i_data[0].get("idxPx")
                if idx_raw is not None:
                    index_price = float(idx_raw)
        except (httpx.HTTPStatusError, ValueError):
            pass

        ts = self.normalize_timestamp(m["ts"])

        funding_block = None
        if funding_rate is not None and cycle_ms is not None and next_funding is not None:
            funding_block = build_funding_current(
                kind           = "discrete",
                per_cycle      = funding_rate,
                cycle_ms       = cycle_ms,
                valid_until_ts = max(next_funding, ts + 1),
            )

        return MarkPrice(
            symbol      = model_sym,
            market_type = market_type,
            quote       = quote,
            mark_price  = float(m["markPx"]),
            index_price = index_price,
            funding     = funding_block,
            timestamp   = ts,
        )


    async def get_funding_rate(self, market_type: MarketType, symbol: str, start_time: Optional[int] = None, limit: int = 100) -> List[FundingRate]:
        if market_type == MarketType.SPOT:
            raise NotImplementedError(f"{self.name} does not support funding_rate for {market_type.value}")
        api_symbol = await self._resolve_symbol(symbol, market_type)
        per_req = 100

        cycle_ms = await self._funding_interval_ms_for(api_symbol)
        cycle_ms = cycle_ms if cycle_ms is not None else 8 * 3_600_000

        model_sym = self.get_model_symbol(api_symbol, market_type)
        info      = await self._info_for(market_type, model_sym)
        quote     = info.quote_asset if info else ""

        def _parse(rows, sym):
            return sorted([FundingRate(
                symbol      = sym,
                market_type = market_type,
                quote       = quote,
                rate        = build_funding_historical(
                    kind      = "discrete",
                    per_cycle = float(r["fundingRate"]),
                    cycle_ms  = cycle_ms,
                ),
                timestamp   = self.normalize_timestamp(r["fundingTime"]),
            ) for r in rows], key=lambda x: x.timestamp)

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
        api_symbol    = await self._resolve_symbol(symbol, market_type)
        model_sym     = self.get_model_symbol(api_symbol, market_type)
        bar           = self._map_candle_interval(period)
        per_req       = 100
        info          = await self._info_for(market_type, model_sym)
        quote         = info.quote_asset if info else ""
        contract_size = info.contract_size if info else None

        is_linear = market_type == MarketType.LINEAR

        def _parse(rows):
            out = []
            for r in rows:
                if not isinstance(r, list) or len(r) < 2:
                    continue
                try:
                    ts = self.normalize_timestamp(r[0])
                    if ts <= 0:
                        continue
                    if is_linear and len(r) >= 3:
                        native_value  = float(r[2])
                        oi_unit_local = "base"
                        cs            = None
                    else:
                        native_value  = float(r[1])
                        oi_unit_local = "contract"
                        cs            = contract_size
                    out.append(OpenInterest(
                        symbol        = model_sym,
                        market_type   = market_type,
                        quote         = quote,
                        interval      = period,
                        open_interest = build_oi_value(
                            native          = native_value,
                            oi_unit         = oi_unit_local,
                            contract_size   = cs,
                            candle_close    = None,
                            candle_close_ts = None,
                        ),
                        timestamp     = ts,
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
        api_symbol    = await self._resolve_symbol(symbol, market_type)
        model_sym     = self.get_model_symbol(api_symbol, market_type)
        uly           = api_symbol.replace("-SWAP", "")
        per_req       = 100
        info          = await self._info_for(market_type, model_sym)
        quote         = info.quote_asset if info else ""
        contract_size = info.contract_size if info else None

        if market_type == MarketType.LINEAR and contract_size is None:
            raise ValueError(f"Contract size unavailable for {model_sym} on OKX linear")

        def _parse(rows):
            out = []
            for entry in rows:
                if entry.get("instId") != api_symbol:
                    continue
                for d in entry.get("details", []):
                    side = d.get("side", "").lower()
                    if side not in ("buy", "sell"):
                        continue
                    price  = float(d["bkPx"])
                    sz_raw = float(d["sz"])
                    if market_type == MarketType.LINEAR:
                        native = sz_raw * contract_size
                        qty_unit_local = "base"
                        qty_cs = None
                    elif market_type == MarketType.INVERSE:
                        native = sz_raw
                        qty_unit_local = "contract"
                        qty_cs = contract_size
                    else:
                        native = sz_raw
                        qty_unit_local = "base"
                        qty_cs = None
                    out.append(Liquidation(
                        symbol      = model_sym,
                        market_type = market_type,
                        quote       = quote,
                        side        = side,
                        price       = price,
                        qty         = build_qty_value(
                            native        = native,
                            qty_unit      = qty_unit_local,
                            contract_size = qty_cs,
                            price         = price,
                        ),
                        timestamp   = self.normalize_timestamp(d["ts"]),
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
                    out.append(LongShortRatio(
                        symbol        = model_sym,
                        market_type   = market_type,
                        interval      = period,
                        ratio         = ratio,
                        long_account  = None,
                        short_account = None,
                        account_scope = "opaque",
                        timestamp     = ts,
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


    async def _fetch_exchange_info(self, market_type: MarketType) -> List[SymbolInfo]:
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
                base = s.get("baseCcy") or None
                quote = s.get("quoteCcy") or None
            else:
                uly_parts = s.get("uly", "").split("-")
                base = uly_parts[0] if len(uly_parts) >= 2 else None
                quote = uly_parts[1] if len(uly_parts) >= 2 else None
            if not base or not quote:
                continue

            qty_unit = "contract" if market_type != MarketType.SPOT else "base"
            contract_size: Optional[float] = None
            if market_type != MarketType.SPOT:
                ct_val_raw = s.get("ctVal")
                if ct_val_raw is not None:
                    try:
                        contract_size = float(ct_val_raw)
                    except (TypeError, ValueError):
                        contract_size = None

            raw_min_qty = s.get("minSz")
            raw_max_qty = s.get("maxLmtSz") or s.get("maxMktSz")
            min_qty_v: Optional[float] = float(raw_min_qty) if raw_min_qty is not None else None
            max_qty_v: Optional[float] = float(raw_max_qty) if raw_max_qty is not None else None
            if min_qty_v is not None and min_qty_v <= 0:
                min_qty_v = None
            if max_qty_v is not None and max_qty_v <= 0:
                max_qty_v = None

            funding_block = None
            if market_type != MarketType.SPOT:
                funding_block = build_funding_convention("discrete")

            results.append(SymbolInfo(
                symbol             = self.get_model_symbol(api_sym, market_type),
                native_symbol      = api_sym,
                base_asset         = base,
                quote_asset        = quote,
                price_precision    = self._precision(s.get("tickSz", "0")),
                quantity_precision = self._precision(s.get("lotSz", "0")),
                min_qty            = min_qty_v,
                max_qty            = max_qty_v,
                min_notional       = None,
                qty_unit           = qty_unit,
                contract_size      = contract_size,
                funding            = funding_block,
            ))
        return results


    async def get_exchange_info(self, market_type: MarketType) -> List[SymbolInfo]:
        cache = await self._ensure_info_cache(market_type)
        return list(cache.values())


    async def get_symbol_info(self, market_type: MarketType, symbol: str) -> SymbolInfo:
        cache = await self._ensure_info_cache(market_type)
        api_symbol = await self._resolve_symbol(symbol, market_type)
        model_sym = self.get_model_symbol(api_symbol, market_type)
        info = cache.get(model_sym)
        if info is None:
            raise ValueError(f"Symbol {symbol} not found on OKX {market_type.value}")
        if market_type != MarketType.SPOT:
            await self._funding_interval_ms_for(api_symbol)
        return info


    async def get_markets(self, market_type: MarketType) -> List[str]:
        info = await self.get_exchange_info(market_type)
        return [s.symbol for s in info]


    async def stream_ticker(self, market_type: MarketType, symbol: str) -> AsyncGenerator[Ticker, None]:
        api_symbol    = await self._resolve_symbol(symbol, market_type)
        model_sym     = self.get_model_symbol(api_symbol, market_type)
        info          = await self._info_for(market_type, model_sym)
        quote         = info.quote_asset if info else ""
        contract_size = info.contract_size if info else None

        async for msg in self._ws_connect([{"channel": "tickers", "instId": api_symbol}]):
            for t in msg.get("data", []):
                last     = float(t.get("last") or 0)
                open_24h = float(t.get("open24h") or 0)
                pct      = ((last - open_24h) / open_24h * 100) if open_24h else 0.0

                if market_type == MarketType.LINEAR:
                    vol_native = float(t.get("volCcy24h") or 0)
                    vol_unit   = "base"
                    vol_cs     = None
                elif market_type == MarketType.INVERSE:
                    vol_native = float(t.get("vol24h") or 0)
                    vol_unit   = "contract"
                    vol_cs     = contract_size
                else:
                    vol_native = float(t.get("vol24h") or 0)
                    vol_unit   = "base"
                    vol_cs     = None

                yield Ticker(
                    symbol               = model_sym,
                    market_type          = market_type,
                    quote                = quote,
                    price                = last,
                    open_24h             = open_24h,
                    high_24h             = float(t.get("high24h") or 0),
                    low_24h              = float(t.get("low24h") or 0),
                    volume_24h           = build_volume_value(
                        native        = vol_native,
                        volume_unit   = vol_unit,
                        contract_size = vol_cs,
                        close         = last,
                    ),
                    price_change_percent = pct,
                    timestamp            = self.normalize_timestamp(t["ts"]),
                )


    async def stream_book_ticker(self, market_type: MarketType, symbol: str) -> AsyncGenerator[BookTicker, None]:
        api_symbol    = await self._resolve_symbol(symbol, market_type)
        model_sym     = self.get_model_symbol(api_symbol, market_type)
        info          = await self._info_for(market_type, model_sym)
        quote         = info.quote_asset if info else ""
        contract_size = info.contract_size if info else None

        if market_type == MarketType.LINEAR and contract_size is None:
            raise ValueError(f"Contract size unavailable for {model_sym} on OKX linear")

        async for msg in self._ws_connect([{"channel": "bbo-tbt", "instId": api_symbol}]):
            for b in msg.get("data", []):
                bids = b.get("bids", [])
                asks = b.get("asks", [])
                if not bids or not asks:
                    continue
                bid_price = float(bids[0][0])
                ask_price = float(asks[0][0])
                bid_raw   = float(bids[0][1])
                ask_raw   = float(asks[0][1])

                if market_type == MarketType.LINEAR:
                    bid_native = bid_raw * contract_size
                    ask_native = ask_raw * contract_size
                    qty_unit   = "base"
                    qty_cs     = None
                elif market_type == MarketType.INVERSE:
                    bid_native = bid_raw
                    ask_native = ask_raw
                    qty_unit   = "contract"
                    qty_cs     = contract_size
                else:
                    bid_native = bid_raw
                    ask_native = ask_raw
                    qty_unit   = "base"
                    qty_cs     = None

                yield BookTicker(
                    symbol      = model_sym,
                    market_type = market_type,
                    quote       = quote,
                    bid_price   = bid_price,
                    bid_qty     = build_qty_value(
                        native        = bid_native,
                        qty_unit      = qty_unit,
                        contract_size = qty_cs,
                        price         = bid_price,
                    ),
                    ask_price   = ask_price,
                    ask_qty     = build_qty_value(
                        native        = ask_native,
                        qty_unit      = qty_unit,
                        contract_size = qty_cs,
                        price         = ask_price,
                    ),
                    timestamp   = self.normalize_timestamp(b["ts"]),
                )


    async def stream_orderbook(self, market_type: MarketType, symbol: str, depth: int = 20, update_speed: str = "100ms") -> AsyncGenerator[OrderBook, None]:
        api_symbol    = await self._resolve_symbol(symbol, market_type)
        model_sym     = self.get_model_symbol(api_symbol, market_type)
        info          = await self._info_for(market_type, model_sym)
        quote         = info.quote_asset if info else ""
        contract_size = info.contract_size if info else None
        channel       = "bbo-tbt" if depth <= 1 else "books5" if depth <= 5 else "books"

        if market_type == MarketType.LINEAR and contract_size is None:
            raise ValueError(f"Contract size unavailable for {model_sym} on OKX linear")

        if market_type == MarketType.LINEAR:
            qty_mult    = contract_size
            ob_qty_unit = "base"
        elif market_type == MarketType.INVERSE:
            qty_mult    = 1.0
            ob_qty_unit = "contract"
        else:
            qty_mult    = 1.0
            ob_qty_unit = "base"

        bids: Dict[float, float] = {}
        asks: Dict[float, float] = {}

        async for msg in self._ws_connect([{"channel": channel, "instId": api_symbol}]):
            action = msg.get("action")
            for b in msg.get("data", []):
                if channel != "books" or action != "update":
                    bids.clear()
                    asks.clear()
                for entry in b.get("bids", []):
                    p, q = float(entry[0]), float(entry[1])
                    if q == 0:
                        bids.pop(p, None)
                    else:
                        bids[p] = q * qty_mult
                for entry in b.get("asks", []):
                    p, q = float(entry[0]), float(entry[1])
                    if q == 0:
                        asks.pop(p, None)
                    else:
                        asks[p] = q * qty_mult

                sorted_bids = sorted(bids.items(), key=lambda kv: -kv[0])[:depth]
                sorted_asks = sorted(asks.items(), key=lambda kv: kv[0])[:depth]

                yield OrderBook(
                    symbol      = model_sym,
                    market_type = market_type,
                    quote       = quote,
                    bids        = [[p, q] for p, q in sorted_bids],
                    asks        = [[p, q] for p, q in sorted_asks],
                    qty_unit    = ob_qty_unit,
                    timestamp   = self.normalize_timestamp(b["ts"]),
                )


    async def stream_trades(self, market_type: MarketType, symbol: str) -> AsyncGenerator[Trade, None]:
        api_symbol    = await self._resolve_symbol(symbol, market_type)
        model_sym     = self.get_model_symbol(api_symbol, market_type)
        info          = await self._info_for(market_type, model_sym)
        quote         = info.quote_asset if info else ""
        contract_size = info.contract_size if info else None

        if market_type == MarketType.LINEAR and contract_size is None:
            raise ValueError(f"Contract size unavailable for {model_sym} on OKX linear")

        async for msg in self._ws_connect([{"channel": "trades", "instId": api_symbol}]):
            for t in msg.get("data", []):
                price  = float(t["px"])
                sz_raw = float(t["sz"])
                if market_type == MarketType.LINEAR:
                    native = sz_raw * contract_size
                    qty_unit_local = "base"
                    qty_cs = None
                elif market_type == MarketType.INVERSE:
                    native = sz_raw
                    qty_unit_local = "contract"
                    qty_cs = contract_size
                else:
                    native = sz_raw
                    qty_unit_local = "base"
                    qty_cs = None
                yield Trade(
                    id          = str(t["tradeId"]),
                    symbol      = model_sym,
                    market_type = market_type,
                    quote       = quote,
                    price       = price,
                    qty         = build_qty_value(
                        native        = native,
                        qty_unit      = qty_unit_local,
                        contract_size = qty_cs,
                        price         = price,
                    ),
                    side        = t["side"],
                    timestamp   = self.normalize_timestamp(t["ts"]),
                )


    async def stream_mark_price(self, market_type: MarketType, symbol: str) -> AsyncGenerator[MarkPrice, None]:
        if market_type == MarketType.SPOT:
            raise NotImplementedError(f"{self.name} does not support stream_mark_price for {market_type.value}")
        api_symbol = await self._resolve_symbol(symbol, market_type)
        model_sym  = self.get_model_symbol(api_symbol, market_type)
        info       = await self._info_for(market_type, model_sym)
        quote      = info.quote_asset if info else ""

        async for msg in self._ws_connect([{"channel": "mark-price", "instId": api_symbol}]):
            for m in msg.get("data", []):
                yield MarkPrice(
                    symbol      = model_sym,
                    market_type = market_type,
                    quote       = quote,
                    mark_price  = float(m["markPx"]),
                    index_price = None,
                    funding     = None,
                    timestamp   = self.normalize_timestamp(m["ts"]),
                )


    async def stream_liquidations(self, market_type: MarketType, symbol: str) -> AsyncGenerator[Liquidation, None]:
        if market_type == MarketType.SPOT:
            raise NotImplementedError(f"{self.name} does not support stream_liquidations for {market_type.value}")
        api_symbol    = await self._resolve_symbol(symbol, market_type)
        model_sym     = self.get_model_symbol(api_symbol, market_type)
        info          = await self._info_for(market_type, model_sym)
        quote         = info.quote_asset if info else ""
        contract_size = info.contract_size if info else None

        if market_type == MarketType.LINEAR and contract_size is None:
            raise ValueError(f"Contract size unavailable for {model_sym} on OKX linear")

        async for msg in self._ws_connect([{"channel": "liquidation-orders", "instType": "SWAP"}]):
            for entry in msg.get("data", []):
                if entry.get("instId") != api_symbol:
                    continue
                for d in entry.get("details", []):
                    side = d.get("side", "").lower()
                    if side not in ("buy", "sell"):
                        continue
                    price  = float(d["bkPx"])
                    sz_raw = float(d["sz"])
                    if market_type == MarketType.LINEAR:
                        native = sz_raw * contract_size
                        qty_unit_local = "base"
                        qty_cs = None
                    elif market_type == MarketType.INVERSE:
                        native = sz_raw
                        qty_unit_local = "contract"
                        qty_cs = contract_size
                    else:
                        native = sz_raw
                        qty_unit_local = "base"
                        qty_cs = None
                    yield Liquidation(
                        symbol      = model_sym,
                        market_type = market_type,
                        quote       = quote,
                        side        = side,
                        price       = price,
                        qty         = build_qty_value(
                            native        = native,
                            qty_unit      = qty_unit_local,
                            contract_size = qty_cs,
                            price         = price,
                        ),
                        timestamp   = self.normalize_timestamp(d["ts"]),
                    )
