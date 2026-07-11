import httpx
import asyncio
import random
import time
import uuid
import websockets
import logging
from typing import List, AsyncGenerator, Dict, Any, Optional, Tuple
from src.exchanges.base import (
    BaseExchange, StreamHub, UpstreamUnavailableError,
    build_qty_value, build_volume_value, build_oi_value,
    build_funding_current, build_funding_historical, build_funding_convention,
)
from src.models import (
    Ticker, BookTicker, MarkPrice, OrderBook, Candle, Trade, AggTrade,
    MarketType, SymbolInfo, OpenInterest, FundingRate,
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

        self._hubs: Dict[MarketType, StreamHub] = {}
        self._hubs_lock = asyncio.Lock()

        self._funding_interval_cache: Dict[MarketType, Dict[str, int]] = {}
        self._contract_multiplier_cache: Dict[MarketType, Dict[str, float]] = {}

        self._capabilities = self._build_capabilities()


    async def shutdown(self):
        for hub in list(self._hubs.values()):
            await hub.close()
        self._hubs.clear()
        await self.http_client.aclose()


    async def _warm(self) -> None:
        await self._step("spot_info",       self._ensure_info_cache(MarketType.SPOT))
        await self._step("linear_info",     self._ensure_info_cache(MarketType.LINEAR))
        await self._step("inverse_info",    self._ensure_info_cache(MarketType.INVERSE))
        await self._step("spot_symbol_map", self._ensure_spot_symbol_map())


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
                    "mark_price": {
                        "rest": False,
                        "ws":   False,
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
            "1m":  "1min",
            "3m":  "3min",
            "5m":  "5min",
            "15m": "15min",
            "30m": "30min",
            "1h":  "1hour",
            "2h":  "2hour",
            "4h":  "4hour",
            "6h":  "6hour",
            "8h":  "8hour",
            "12h": "12hour",
            "1d":  "1day",
            "1w":  "1week",
        }
        if interval not in mapping:
            raise ValueError(f"KuCoin does not support interval {interval}")
        return mapping[interval]


    def _map_futures_granularity(self, interval: str) -> int:
        mapping = {
            "1m":  1,
            "5m":  5,
            "15m": 15,
            "30m": 30,
            "1h":  60,
            "2h":  120,
            "4h":  240,
            "8h":  480,
            "12h": 720,
            "1d":  1440,
            "1w":  10080,
        }
        if interval not in mapping:
            raise ValueError(f"KuCoin does not support interval {interval}")
        return mapping[interval]


    def _map_oi_interval(self, period: str) -> str:
        mapping = {
            "5m":  "5min",
            "15m": "15min",
            "30m": "30min",
            "1h":  "1hour",
            "4h":  "4hour",
            "1d":  "1day",
        }
        if period not in mapping:
            raise ValueError(f"KuCoin does not support interval {period}")
        return mapping[period]


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
                    raise UpstreamUnavailableError(f"KuCoin backoff active. Retry in {wait_time:.0f}s.", retry_after=wait_time)
                await asyncio.sleep(wait_time + random.uniform(0.05, 1.0))

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
                if e.response.status_code in (502, 503, 504, 520):
                    logger.warning(f"KuCoin HTTP {e.response.status_code} on {endpoint}. Retrying...")
                    retries -= 1
                    await asyncio.sleep(1)
                    continue
                if e.response.status_code >= 500:
                    raise UpstreamUnavailableError(f"KuCoin upstream error {e.response.status_code} on {endpoint}")
                logger.error(f"HTTP Error: {e}")
                raise e
            except Exception as e:
                if isinstance(e, ValueError):
                    raise e
                logger.error(f"Request Error: {e}")
                retries -= 1
                await asyncio.sleep(1)

        remaining = self._backoff_until - time.time()
        raise UpstreamUnavailableError(f"Max retries exceeded for {url}", retry_after=remaining if remaining > 0 else None)


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


    async def _get_hub(self, market_type: MarketType) -> StreamHub:
        async with self._hubs_lock:
            hub = self._hubs.get(market_type)
            if hub is None:
                async def _connect():
                    endpoint, token, _ping_ms = await self._get_bullet(market_type)
                    connect_id = str(uuid.uuid4())
                    url = f"{endpoint}?token={token}&connectId={connect_id}"
                    ws = await websockets.connect(url, ping_interval=None)
                    await asyncio.wait_for(ws.recv(), timeout=10)
                    return ws
                def _sub(topics: List[str]):
                    return [{"id": str(int(time.time() * 1000)), "type": "subscribe",   "topic": t, "privateChannel": False, "response": False} for t in topics]
                def _unsub(topics: List[str]):
                    return [{"id": str(int(time.time() * 1000)), "type": "unsubscribe", "topic": t, "privateChannel": False, "response": False} for t in topics]
                def _route(msg):
                    if not isinstance(msg, dict):
                        return None
                    if msg.get("type") != "message":
                        return None
                    return msg.get("topic")
                hub = StreamHub(
                    name=f"kucoin_{market_type.value}",
                    connect=_connect,
                    subscribe_payload=_sub,
                    unsubscribe_payload=_unsub,
                    route=_route,
                    keepalive_payload={"id": "keepalive", "type": "ping"},
                    keepalive_interval=15.0,
                )
                self._hubs[market_type] = hub
            return hub


    async def _ws_connect(self, market_type: MarketType, topic: str) -> AsyncGenerator[Dict, None]:
        hub = await self._get_hub(market_type)
        q = await hub.subscribe(topic)
        try:
            while True:
                msg = await q.get()
                yield msg
        finally:
            await hub.unsubscribe(topic, q)


    async def get_ticker(self, market_type: MarketType, symbol: str) -> Ticker:
        api_symbol = await self._resolve_symbol(symbol, market_type)
        model_sym  = self.get_model_symbol(api_symbol, market_type)
        info       = await self._info_for(market_type, model_sym)
        quote      = info.quote_asset if info else ""

        if market_type == MarketType.SPOT:
            t = await self._make_request("GET", self.spot_rest_url, "/api/v1/market/stats", {"symbol": api_symbol})
            if not t:
                raise ValueError(f"Symbol {api_symbol} not found")

            last       = float(t.get("last") or 0)
            change_pct = float(t.get("changeRate") or 0) * 100

            return Ticker(
                symbol               = model_sym,
                market_type          = market_type,
                quote                = quote,
                price                = last,
                open_24h             = last - float(t.get("changePrice") or 0),
                high_24h             = float(t.get("high") or 0),
                low_24h              = float(t.get("low") or 0),
                volume_24h           = build_volume_value(
                    native        = float(t.get("vol") or 0),
                    volume_unit   = "base",
                    contract_size = None,
                    close         = last,
                ),
                price_change_percent = change_pct,
                timestamp            = int(t.get("time") or int(time.time() * 1000)),
            )

        c = await self._make_request("GET", self.fut_rest_url, f"/api/v1/contracts/{api_symbol}")
        if not c:
            raise ValueError(f"Symbol {api_symbol} not found")

        last       = float(c.get("lastTradePrice") or 0)
        price_chg  = float(c.get("priceChg") or 0)
        is_inverse = market_type == MarketType.INVERSE

        return Ticker(
            symbol               = model_sym,
            market_type          = market_type,
            quote                = quote,
            price                = last,
            open_24h             = last - price_chg,
            high_24h             = float(c.get("highPrice") or 0),
            low_24h              = float(c.get("lowPrice") or 0),
            volume_24h           = build_volume_value(
                native        = float(c.get("volumeOf24h") or 0),
                volume_unit   = "contract" if is_inverse else "base",
                contract_size = info.contract_size if info else None,
                close         = last,
            ),
            price_change_percent = float(c.get("priceChgPct") or 0) * 100,
            timestamp            = int(time.time() * 1000),
        )


    async def get_book_ticker(self, market_type: MarketType, symbol: str) -> BookTicker:
        api_symbol    = await self._resolve_symbol(symbol, market_type)
        model_sym     = self.get_model_symbol(api_symbol, market_type)
        info          = await self._info_for(market_type, model_sym)
        quote         = info.quote_asset if info else ""
        contract_size = info.contract_size if info else None
        multiplier    = await self._multiplier_for(market_type, model_sym)
        is_inverse    = market_type == MarketType.INVERSE
        unit          = "contract" if is_inverse else "base"

        if market_type == MarketType.LINEAR and multiplier is None:
            raise ValueError(f"Contract multiplier unavailable for {model_sym} on KuCoin linear")

        if market_type == MarketType.SPOT:
            data = await self._make_request("GET", self.spot_rest_url, "/api/v1/market/orderbook/level1", {"symbol": api_symbol})
            if not data:
                raise ValueError(f"Book ticker not found for {api_symbol}")
            bid_raw    = data.get("bestBid")
            ask_raw    = data.get("bestAsk")
            bid_sz_raw = data.get("bestBidSize")
            ask_sz_raw = data.get("bestAskSize")
            if bid_raw is None or ask_raw is None or bid_sz_raw is None or ask_sz_raw is None:
                raise ValueError(f"No quotes available for {api_symbol} on KuCoin spot")

            bid_price = float(bid_raw)
            ask_price = float(ask_raw)
            return BookTicker(
                symbol      = model_sym,
                market_type = market_type,
                quote       = quote,
                bid_price   = bid_price,
                bid_qty     = build_qty_value(
                    native        = float(bid_sz_raw),
                    qty_unit      = "base",
                    contract_size = None,
                    price         = bid_price,
                ),
                ask_price   = ask_price,
                ask_qty     = build_qty_value(
                    native        = float(ask_sz_raw),
                    qty_unit      = "base",
                    contract_size = None,
                    price         = ask_price,
                ),
                timestamp   = int(data.get("time") or int(time.time() * 1000)),
            )

        t = await self._make_request("GET", self.fut_rest_url, "/api/v1/ticker", {"symbol": api_symbol})
        if not t:
            raise ValueError(f"Book ticker not found for {api_symbol}")
        bid_raw    = t.get("bestBidPrice")
        ask_raw    = t.get("bestAskPrice")
        bid_sz_raw = t.get("bestBidSize")
        ask_sz_raw = t.get("bestAskSize")
        if bid_raw is None or ask_raw is None or bid_sz_raw is None or ask_sz_raw is None:
            raise ValueError(f"No quotes available for {api_symbol} on KuCoin {market_type.value}")

        bid_price  = float(bid_raw)
        ask_price  = float(ask_raw)
        bid_sz     = float(bid_sz_raw)
        ask_sz     = float(ask_sz_raw)

        if is_inverse:
            bid_native = bid_sz
            ask_native = ask_sz
            qty_unit   = "contract"
            qty_cs     = contract_size
        else:
            bid_native = bid_sz * multiplier
            ask_native = ask_sz * multiplier
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
            timestamp   = self._ns_to_ms(t.get("ts") or int(time.time() * 1000)),
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

        info       = await self._info_for(market_type, model_sym)
        quote      = info.quote_asset if info else ""
        multiplier = await self._multiplier_for(market_type, model_sym)

        if market_type == MarketType.LINEAR and multiplier is None:
            raise ValueError(f"Contract multiplier unavailable for {model_sym} on KuCoin linear")

        if market_type == MarketType.INVERSE:
            qty_mult    = 1.0
            ob_qty_unit = "contract"
        elif market_type == MarketType.LINEAR:
            qty_mult    = multiplier
            ob_qty_unit = "base"
        else:
            qty_mult    = 1.0
            ob_qty_unit = "base"

        return OrderBook(
            symbol      = model_sym,
            market_type = market_type,
            quote       = quote,
            bids        = [[float(p), float(q) * qty_mult] for p, q, *_ in data.get("bids", [])][:depth],
            asks        = [[float(p), float(q) * qty_mult] for p, q, *_ in data.get("asks", [])][:depth],
            qty_unit    = ob_qty_unit,
            timestamp   = ts,
        )


    async def get_trades(self, market_type: MarketType, symbol: str, limit: int = 100) -> List[Trade]:
        limit         = min(limit, self.get_capabilities()["markets"][market_type]["trades"]["max_limit"])
        api_symbol    = await self._resolve_symbol(symbol, market_type)
        model_sym     = self.get_model_symbol(api_symbol, market_type)
        info          = await self._info_for(market_type, model_sym)
        quote         = info.quote_asset if info else ""
        contract_size = info.contract_size if info else None
        multiplier    = await self._multiplier_for(market_type, model_sym)
        is_inverse    = market_type == MarketType.INVERSE
        unit          = "contract" if is_inverse else "base"

        if market_type == MarketType.LINEAR and multiplier is None:
            raise ValueError(f"Contract multiplier unavailable for {model_sym} on KuCoin linear")

        def _build_trade(raw, ts_key):
            side_raw = raw.get("side")
            if side_raw is None:
                return None
            side_norm = side_raw.lower()
            if side_norm not in ("buy", "sell"):
                return None
            raw_id   = raw.get("tradeId") or raw.get("sequence")
            trade_id = str(raw_id) if raw_id else None
            price    = float(raw["price"])
            size_raw = float(raw["size"])

            if market_type == MarketType.INVERSE:
                native = size_raw
                qty_unit_local = "contract"
                qty_cs = contract_size
            elif market_type == MarketType.LINEAR:
                native = size_raw * multiplier
                qty_unit_local = "base"
                qty_cs = None
            else:
                native = size_raw
                qty_unit_local = "base"
                qty_cs = None

            return Trade(
                id          = trade_id,
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
                side        = side_norm,
                timestamp   = self._ns_to_ms(raw.get(ts_key) or int(time.time() * 1000)),
            )

        if market_type == MarketType.SPOT:
            data = await self._make_request("GET", self.spot_rest_url, "/api/v1/market/histories", {"symbol": api_symbol})
            trades = [t for t in (_build_trade(r, "time") for r in (data or [])) if t is not None]
        else:
            data = await self._make_request("GET", self.fut_rest_url, "/api/v1/trade/history", {"symbol": api_symbol})
            trades = [t for t in (_build_trade(r, "ts") for r in (data or [])) if t is not None]

        trades.sort(key=lambda t: t.timestamp)

        return trades[-limit:]


    async def get_agg_trades(self, market_type: MarketType, symbol: str, start_time: Optional[int] = None, limit: int = 500) -> List[AggTrade]:
        raise NotImplementedError(f"{self.name} does not support agg_trades for {market_type.value}")


    async def get_candles(self, market_type: MarketType, symbol: str, interval: str, start_time: Optional[int] = None, limit: int = 100) -> List[Candle]:
        api_symbol = await self._resolve_symbol(symbol, market_type)

        if market_type == MarketType.SPOT:
            return await self._get_candles_spot(api_symbol, interval, start_time, limit)
        return await self._get_candles_futures(market_type, api_symbol, interval, start_time, limit)


    async def _get_candles_spot(self, api_symbol: str, interval: str, start_time: Optional[int], limit: int) -> List[Candle]:
        ktype       = self._map_spot_interval(interval)
        interval_ms = self._interval_to_ms(interval)
        per_req     = 1500
        model_sym   = self.get_model_symbol(api_symbol, MarketType.SPOT)
        info        = await self._info_for(MarketType.SPOT, model_sym)
        quote       = info.quote_asset if info else ""

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
                    close = float(r[2])
                    out.append(Candle(
                        symbol      = model_sym,
                        market_type = MarketType.SPOT,
                        quote       = quote,
                        interval    = interval,
                        timestamp   = ts,
                        open        = float(r[1]),
                        close       = close,
                        high        = float(r[3]),
                        low         = float(r[4]),
                        volume      = build_volume_value(
                            native        = float(r[5]),
                            volume_unit   = "base",
                            contract_size = None,
                            close         = close,
                        ),
                    ))
                except (ValueError, TypeError):
                    continue
            return sorted(out, key=lambda x: x.timestamp)

        async def fetch(end_ts, l):
            anchor_ms = end_ts if end_ts is not None else (start_time if start_time is not None else int(time.time() * 1000))
            end_s = (anchor_ms // 1000) + 1
            window_s = max(interval_ms * l // 1000, 60)
            params = {
                "symbol":  api_symbol,
                "type":    ktype,
                "startAt": max(end_s - window_s, 0),
                "endAt":   end_s,
            }
            return _parse(await self._make_request("GET", self.spot_rest_url, "/api/v1/market/candles", params))

        if start_time is None and limit <= per_req:
            return (await fetch(None, limit))[-limit:]
        return await self._paginate_backwards(fetch, limit, per_req)


    async def _get_candles_futures(self, market_type: MarketType, api_symbol: str, interval: str, start_time: Optional[int], limit: int) -> List[Candle]:
        granularity   = self._map_futures_granularity(interval)
        per_req       = 200
        model_sym     = self.get_model_symbol(api_symbol, market_type)
        info          = await self._info_for(market_type, model_sym)
        quote         = info.quote_asset if info else ""
        contract_size = info.contract_size if info else None
        multiplier    = await self._multiplier_for(market_type, model_sym)
        is_inverse    = market_type == MarketType.INVERSE

        if not is_inverse and multiplier is None:
            raise ValueError(f"Contract multiplier unavailable for {model_sym} on KuCoin linear")

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
                    close    = float(r[4])
                    vol_raw  = float(r[5])
                    if is_inverse:
                        vol_native = vol_raw
                        vol_unit_local = "contract"
                        vol_cs = contract_size
                    else:
                        vol_native = vol_raw * multiplier
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
        model_sym  = self.get_model_symbol(api_symbol, market_type)
        info       = await self._info_for(market_type, model_sym)
        quote      = info.quote_asset if info else ""
        cycle_ms   = await self._funding_interval_ms_for(market_type, model_sym)

        m = await self._make_request("GET", self.fut_rest_url, f"/api/v1/mark-price/{api_symbol}/current")
        if not m:
            raise ValueError(f"Mark price not found for {api_symbol}")

        funding_rate: Optional[float] = None
        next_funding: Optional[int] = None
        contract = await self._make_request("GET", self.fut_rest_url, f"/api/v1/contracts/{api_symbol}")
        if contract:
            rate_raw = contract.get("fundingFeeRate")
            if rate_raw is not None:
                funding_rate = float(rate_raw)
            ms_until = int(contract.get("nextFundingRateTime") or 0)
            if ms_until > 0:
                next_funding = int(time.time() * 1000) + ms_until

        idx_raw = m.get("indexPrice")
        idx_price: Optional[float] = float(idx_raw) if idx_raw is not None else None
        ts = int(m.get("timePoint") or int(time.time() * 1000))

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
            mark_price  = float(m.get("value") or 0),
            index_price = idx_price,
            funding     = funding_block,
            timestamp   = ts,
        )


    async def get_funding_rate(self, market_type: MarketType, symbol: str, start_time: Optional[int] = None, limit: int = 100) -> List[FundingRate]:
        if market_type == MarketType.SPOT:
            raise NotImplementedError(f"{self.name} does not support funding_rate for {market_type.value}")
        api_symbol        = await self._resolve_symbol(symbol, market_type)
        model_sym         = self.get_model_symbol(api_symbol, market_type)
        info              = await self._info_for(market_type, model_sym)
        quote             = info.quote_asset if info else ""
        cycle_ms          = await self._funding_interval_ms_for(market_type, model_sym)
        cycle_ms          = cycle_ms if cycle_ms is not None else 8 * 3_600_000
        funding_window_ms = 8 * 60 * 60 * 1000
        per_req           = 100

        def _parse(rows):
            out = []
            for r in rows or []:
                try:
                    ts = int(r.get("timepoint") or r.get("timePoint") or 0)
                    if ts <= 0:
                        continue
                    out.append(FundingRate(
                        symbol      = model_sym,
                        market_type = market_type,
                        quote       = quote,
                        rate        = build_funding_historical(
                            kind      = "discrete",
                            per_cycle = float(r.get("fundingRate") or 0),
                            cycle_ms  = cycle_ms,
                        ),
                        timestamp   = ts,
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
        api_symbol    = await self._resolve_symbol(symbol, market_type)
        model_sym     = self.get_model_symbol(api_symbol, market_type)
        interval      = self._map_oi_interval(period)
        period_ms     = self._interval_to_ms(period)
        per_req       = 200
        info          = await self._info_for(market_type, model_sym)
        quote         = info.quote_asset if info else ""
        contract_size = info.contract_size if info else None
        multiplier    = await self._multiplier_for(market_type, model_sym)
        is_inverse    = market_type == MarketType.INVERSE

        if not is_inverse and multiplier is None:
            raise ValueError(f"Contract multiplier unavailable for {model_sym} on KuCoin linear")

        def _parse(rows):
            out = []
            for r in rows or []:
                try:
                    ts = int(r.get("ts") or 0)
                    if ts <= 0:
                        continue
                    raw = float(r.get("openInterest") or 0)
                    if is_inverse:
                        native_value  = raw
                        oi_unit_local = "contract"
                        cs            = contract_size
                    else:
                        native_value  = raw * multiplier
                        oi_unit_local = "base"
                        cs            = None
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


    async def _fetch_exchange_info(self, market_type: MarketType) -> List[SymbolInfo]:
        if market_type == MarketType.SPOT:
            data = await self._make_request("GET", self.spot_rest_url, "/api/v1/symbols")
            results = []
            for s in data or []:
                if not s.get("enableTrading"):
                    continue
                api_sym = s["symbol"]
                base_raw = s.get("baseCurrency")
                quote_raw = s.get("quoteCurrency")
                if not base_raw or not quote_raw:
                    continue
                raw_min_qty = s.get("baseMinSize")
                raw_max_qty = s.get("baseMaxSize")
                raw_min_notional = s.get("minFunds") or s.get("quoteMinSize")
                spot_min_qty: Optional[float] = float(raw_min_qty) if raw_min_qty is not None else None
                spot_max_qty: Optional[float] = float(raw_max_qty) if raw_max_qty is not None else None
                spot_min_notional: Optional[float] = float(raw_min_notional) if raw_min_notional is not None else None
                if spot_min_qty is not None and spot_min_qty <= 0:
                    spot_min_qty = None
                if spot_max_qty is not None and spot_max_qty <= 0:
                    spot_max_qty = None
                if spot_min_notional is not None and spot_min_notional <= 0:
                    spot_min_notional = None
                results.append(SymbolInfo(
                    symbol             = self.get_model_symbol(api_sym, market_type),
                    native_symbol      = api_sym,
                    base_asset         = base_raw,
                    quote_asset        = quote_raw,
                    price_precision    = self._precision(s.get("priceIncrement", "0")),
                    quantity_precision = self._precision(s.get("baseIncrement", "0")),
                    min_qty            = spot_min_qty,
                    max_qty            = spot_max_qty,
                    min_notional       = spot_min_notional,
                    qty_unit           = "base",
                    contract_size      = None,
                    funding            = None,
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

            multiplier: Optional[float] = None
            mult_raw = s.get("multiplier")
            if mult_raw is not None:
                try:
                    multiplier = abs(float(mult_raw))
                except (TypeError, ValueError):
                    multiplier = None

            qty_unit = "contract" if market_type == MarketType.INVERSE else "base"
            contract_size: Optional[float] = None
            if market_type == MarketType.INVERSE:
                contract_size = multiplier if multiplier is not None else 1.0

            funding_interval_ms: Optional[int] = None
            granularity_raw = s.get("fundingRateGranularity")
            if granularity_raw is not None:
                try:
                    funding_interval_ms = int(granularity_raw)
                except (TypeError, ValueError):
                    funding_interval_ms = None

            fut_base = s.get("baseCurrency")
            fut_quote = s.get("quoteCurrency")
            if not fut_base or not fut_quote:
                continue
            raw_lot = s.get("lotSize")
            raw_max = s.get("maxOrderQty")
            fut_min_qty: Optional[float] = float(raw_lot) if raw_lot is not None else None
            fut_max_qty: Optional[float] = float(raw_max) if raw_max is not None else None
            if fut_min_qty is not None and fut_min_qty <= 0:
                fut_min_qty = None
            if fut_max_qty is not None and fut_max_qty <= 0:
                fut_max_qty = None
            if market_type == MarketType.LINEAR:
                if multiplier is None:
                    fut_min_qty = None
                    fut_max_qty = None
                else:
                    if fut_min_qty is not None:
                        fut_min_qty = round(fut_min_qty * multiplier, 12)
                    if fut_max_qty is not None:
                        fut_max_qty = round(fut_max_qty * multiplier, 12)

            if market_type == MarketType.LINEAR and multiplier is not None and raw_lot is not None:
                qty_precision = self._precision(f"{float(raw_lot) * multiplier:.10f}")
            else:
                qty_precision = self._precision(str(s.get("lotSize", "0")))

            model_sym = self.get_model_symbol(api_sym, market_type)
            self._funding_interval_cache.setdefault(market_type, {})
            if funding_interval_ms is not None:
                self._funding_interval_cache[market_type][model_sym] = funding_interval_ms
            self._contract_multiplier_cache.setdefault(market_type, {})
            if multiplier is not None:
                self._contract_multiplier_cache[market_type][model_sym] = multiplier

            results.append(SymbolInfo(
                symbol             = model_sym,
                native_symbol      = api_sym,
                base_asset         = fut_base,
                quote_asset        = fut_quote,
                price_precision    = self._precision(str(s.get("tickSize", "0"))),
                quantity_precision = qty_precision,
                min_qty            = fut_min_qty,
                max_qty            = fut_max_qty,
                min_notional       = None,
                qty_unit           = qty_unit,
                contract_size      = contract_size,
                funding            = build_funding_convention("discrete"),
            ))
        return results


    async def _funding_interval_ms_for(self, market_type: MarketType, model_symbol: str) -> Optional[int]:
        await self._ensure_info_cache(market_type)
        return self._funding_interval_cache.get(market_type, {}).get(model_symbol)


    async def _multiplier_for(self, market_type: MarketType, model_symbol: str) -> Optional[float]:
        await self._ensure_info_cache(market_type)
        return self._contract_multiplier_cache.get(market_type, {}).get(model_symbol)


    async def get_exchange_info(self, market_type: MarketType) -> List[SymbolInfo]:
        cache = await self._ensure_info_cache(market_type)
        return list(cache.values())


    async def get_symbol_info(self, market_type: MarketType, symbol: str) -> SymbolInfo:
        cache = await self._ensure_info_cache(market_type)
        api_symbol = await self._resolve_symbol(symbol, market_type)
        model_sym = self.get_model_symbol(api_symbol, market_type)
        info = cache.get(model_sym)
        if info is None:
            raise ValueError(f"Symbol {symbol} not found on KuCoin {market_type.value}")
        return info


    async def stream_ticker(self, market_type: MarketType, symbol: str) -> AsyncGenerator[Ticker, None]:
        api_symbol    = await self._resolve_symbol(symbol, market_type)
        model_sym     = self.get_model_symbol(api_symbol, market_type)
        info          = await self._info_for(market_type, model_sym)
        quote         = info.quote_asset if info else ""
        contract_size = info.contract_size if info else None
        is_inverse    = market_type == MarketType.INVERSE
        unit          = "contract" if is_inverse else "base"

        if market_type == MarketType.SPOT:
            topic = f"/market/snapshot:{api_symbol}"
            async for msg in self._ws_connect(market_type, topic):
                try:
                    d = (msg.get("data") or {}).get("data") or {}
                    if not d:
                        continue
                    last = float(d.get("lastTradedPrice") or d.get("close") or 0)
                    yield Ticker(
                        symbol               = model_sym,
                        market_type          = market_type,
                        quote                = quote,
                        price                = last,
                        open_24h             = float(d.get("open") or 0),
                        high_24h             = float(d.get("high") or 0),
                        low_24h              = float(d.get("low") or 0),
                        volume_24h           = build_volume_value(
                            native        = float(d.get("vol") or 0),
                            volume_unit   = "base",
                            contract_size = None,
                            close         = last,
                        ),
                        price_change_percent = float(d.get("changeRate") or 0) * 100,
                        timestamp            = int(d.get("datetime") or int(time.time() * 1000)),
                    )
                except (ValueError, TypeError, KeyError, IndexError, AttributeError) as e:
                    logger.warning(f"KuCoin ticker {api_symbol} malformed frame skipped: {e!r}")
            return

        topic = f"/contractMarket/snapshot:{api_symbol}"
        async for msg in self._ws_connect(market_type, topic):
            try:
                d = msg.get("data") or {}
                last      = float(d.get("lastPrice") or 0)
                price_chg = float(d.get("priceChg") or 0)
                vol_raw   = float(d.get("volume") or 0)
                if is_inverse:
                    vol_native = vol_raw
                    vol_unit   = "contract"
                    vol_cs     = contract_size
                else:
                    vol_native = vol_raw
                    vol_unit   = "base"
                    vol_cs     = None
                yield Ticker(
                    symbol               = model_sym,
                    market_type          = market_type,
                    quote                = quote,
                    price                = last,
                    open_24h             = last - price_chg,
                    high_24h             = float(d.get("highPrice") or 0),
                    low_24h              = float(d.get("lowPrice") or 0),
                    volume_24h           = build_volume_value(
                        native        = vol_native,
                        volume_unit   = vol_unit,
                        contract_size = vol_cs,
                        close         = last,
                    ),
                    price_change_percent = float(d.get("priceChgPct") or 0) * 100,
                    timestamp            = self._ns_to_ms(d.get("ts") or d.get("datetime") or int(time.time() * 1000)),
                )
            except (ValueError, TypeError, KeyError, IndexError, AttributeError) as e:
                logger.warning(f"KuCoin ticker {api_symbol} malformed frame skipped: {e!r}")


    async def stream_book_ticker(self, market_type: MarketType, symbol: str) -> AsyncGenerator[BookTicker, None]:
        api_symbol    = await self._resolve_symbol(symbol, market_type)
        model_sym     = self.get_model_symbol(api_symbol, market_type)
        info          = await self._info_for(market_type, model_sym)
        quote         = info.quote_asset if info else ""
        contract_size = info.contract_size if info else None
        multiplier    = await self._multiplier_for(market_type, model_sym)
        is_inverse    = market_type == MarketType.INVERSE
        unit          = "contract" if is_inverse else "base"

        if market_type == MarketType.LINEAR and multiplier is None:
            raise ValueError(f"Contract multiplier unavailable for {model_sym} on KuCoin linear")

        if market_type == MarketType.SPOT:
            topic = f"/spotMarket/level1:{api_symbol}"
            async for msg in self._ws_connect(market_type, topic):
                try:
                    d = msg.get("data") or {}
                    bids = d.get("bids") or []
                    asks = d.get("asks") or []
                    if not bids or not asks:
                        continue
                    bid_price = float(bids[0])
                    ask_price = float(asks[0])
                    yield BookTicker(
                        symbol      = model_sym,
                        market_type = market_type,
                        quote       = quote,
                        bid_price   = bid_price,
                        bid_qty     = build_qty_value(
                            native        = float(bids[1]) if len(bids) > 1 else 0.0,
                            qty_unit      = "base",
                            contract_size = None,
                            price         = bid_price,
                        ),
                        ask_price   = ask_price,
                        ask_qty     = build_qty_value(
                            native        = float(asks[1]) if len(asks) > 1 else 0.0,
                            qty_unit      = "base",
                            contract_size = None,
                            price         = ask_price,
                        ),
                        timestamp   = self._ns_to_ms(d.get("timestamp") or d.get("time") or int(time.time() * 1000)),
                    )
                except (ValueError, TypeError, KeyError, IndexError, AttributeError) as e:
                    logger.warning(f"KuCoin book_ticker {api_symbol} malformed frame skipped: {e!r}")
            return

        topic = f"/contractMarket/tickerV2:{api_symbol}"
        async for msg in self._ws_connect(market_type, topic):
            try:
                d = msg.get("data") or {}
                bid_price = float(d.get("bestBidPrice") or 0)
                ask_price = float(d.get("bestAskPrice") or 0)
                bid_sz    = float(d.get("bestBidSize") or 0)
                ask_sz    = float(d.get("bestAskSize") or 0)
                if is_inverse:
                    bid_native = bid_sz
                    ask_native = ask_sz
                    qty_unit_local = "contract"
                    qty_cs = contract_size
                else:
                    bid_native = bid_sz * multiplier
                    ask_native = ask_sz * multiplier
                    qty_unit_local = "base"
                    qty_cs = None
                yield BookTicker(
                    symbol      = model_sym,
                    market_type = market_type,
                    quote       = quote,
                    bid_price   = bid_price,
                    bid_qty     = build_qty_value(
                        native        = bid_native,
                        qty_unit      = qty_unit_local,
                        contract_size = qty_cs,
                        price         = bid_price,
                    ),
                    ask_price   = ask_price,
                    ask_qty     = build_qty_value(
                        native        = ask_native,
                        qty_unit      = qty_unit_local,
                        contract_size = qty_cs,
                        price         = ask_price,
                    ),
                    timestamp   = self._ns_to_ms(d.get("ts") or int(time.time() * 1000)),
                )
            except (ValueError, TypeError, KeyError, IndexError, AttributeError) as e:
                logger.warning(f"KuCoin book_ticker {api_symbol} malformed frame skipped: {e!r}")


    async def stream_orderbook(self, market_type: MarketType, symbol: str, depth: int = 20, update_speed: str = "100ms") -> AsyncGenerator[OrderBook, None]:
        api_symbol = await self._resolve_symbol(symbol, market_type)
        model_sym  = self.get_model_symbol(api_symbol, market_type)
        info       = await self._info_for(market_type, model_sym)
        quote      = info.quote_asset if info else ""
        multiplier = await self._multiplier_for(market_type, model_sym)
        size       = 5 if depth <= 5 else 50
        prefix     = "/spotMarket" if market_type == MarketType.SPOT else "/contractMarket"
        topic      = f"{prefix}/level2Depth{size}:{api_symbol}"

        if market_type == MarketType.LINEAR and multiplier is None:
            raise ValueError(f"Contract multiplier unavailable for {model_sym} on KuCoin linear")

        if market_type == MarketType.INVERSE:
            qty_mult = 1.0
            qty_unit = "contract"
        elif market_type == MarketType.LINEAR:
            qty_mult = multiplier
            qty_unit = "base"
        else:
            qty_mult = 1.0
            qty_unit = "base"

        async for msg in self._ws_connect(market_type, topic):
            try:
                d  = msg.get("data") or {}
                ts = d.get("timestamp") or d.get("ts") or d.get("time") or 0
                yield OrderBook(
                    symbol      = model_sym,
                    market_type = market_type,
                    quote       = quote,
                    bids        = [[float(p), float(q) * qty_mult] for p, q, *_ in d.get("bids", [])][:depth],
                    asks        = [[float(p), float(q) * qty_mult] for p, q, *_ in d.get("asks", [])][:depth],
                    qty_unit    = qty_unit,
                    timestamp   = self._ns_to_ms(ts),
                )
            except (ValueError, TypeError, KeyError, IndexError, AttributeError) as e:
                logger.warning(f"KuCoin orderbook {api_symbol} malformed frame skipped: {e!r}")


    async def stream_trades(self, market_type: MarketType, symbol: str) -> AsyncGenerator[Trade, None]:
        api_symbol    = await self._resolve_symbol(symbol, market_type)
        model_sym     = self.get_model_symbol(api_symbol, market_type)
        info          = await self._info_for(market_type, model_sym)
        quote         = info.quote_asset if info else ""
        contract_size = info.contract_size if info else None
        multiplier    = await self._multiplier_for(market_type, model_sym)
        is_inverse    = market_type == MarketType.INVERSE
        unit          = "contract" if is_inverse else "base"

        if market_type == MarketType.LINEAR and multiplier is None:
            raise ValueError(f"Contract multiplier unavailable for {model_sym} on KuCoin linear")

        if market_type == MarketType.SPOT:
            topic = f"/market/match:{api_symbol}"
            async for msg in self._ws_connect(market_type, topic):
                try:
                    d = msg.get("data") or {}
                    side_raw = d.get("side")
                    if side_raw is None:
                        continue
                    side_norm = side_raw.lower()
                    if side_norm not in ("buy", "sell"):
                        continue
                    raw_id = d.get("tradeId") or d.get("sequence")
                    price  = float(d.get("price") or 0)
                    yield Trade(
                        id          = str(raw_id) if raw_id else None,
                        symbol      = model_sym,
                        market_type = market_type,
                        quote       = quote,
                        price       = price,
                        qty         = build_qty_value(
                            native        = float(d.get("size") or 0),
                            qty_unit      = unit,
                            contract_size = contract_size,
                            price         = price,
                        ),
                        side        = side_norm,
                        timestamp   = self._ns_to_ms(d.get("time") or int(time.time() * 1000)),
                    )
                except (ValueError, TypeError, KeyError, IndexError, AttributeError) as e:
                    logger.warning(f"KuCoin trades {api_symbol} malformed frame skipped: {e!r}")
            return

        topic = f"/contractMarket/execution:{api_symbol}"
        async for msg in self._ws_connect(market_type, topic):
            try:
                d = msg.get("data") or {}
                side_raw = d.get("side")
                if side_raw is None:
                    continue
                side_norm = side_raw.lower()
                if side_norm not in ("buy", "sell"):
                    continue
                raw_id   = d.get("tradeId") or d.get("sequence")
                price    = float(d.get("price") or 0)
                size_raw = float(d.get("size") or 0)
                if is_inverse:
                    native = size_raw
                    qty_unit_local = "contract"
                    qty_cs = contract_size
                else:
                    native = size_raw * multiplier
                    qty_unit_local = "base"
                    qty_cs = None
                yield Trade(
                    id          = str(raw_id) if raw_id else None,
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
                    side        = side_norm,
                    timestamp   = self._ns_to_ms(d.get("ts") or int(time.time() * 1000)),
                )
            except (ValueError, TypeError, KeyError, IndexError, AttributeError) as e:
                logger.warning(f"KuCoin trades {api_symbol} malformed frame skipped: {e!r}")


    async def stream_mark_price(self, market_type: MarketType, symbol: str) -> AsyncGenerator[MarkPrice, None]:
        if market_type == MarketType.SPOT:
            raise NotImplementedError(f"{self.name} does not support stream_mark_price for {market_type.value}")
        api_symbol = await self._resolve_symbol(symbol, market_type)
        model_sym  = self.get_model_symbol(api_symbol, market_type)
        info       = await self._info_for(market_type, model_sym)
        quote      = info.quote_asset if info else ""
        cycle_ms   = await self._funding_interval_ms_for(market_type, model_sym)
        topic      = f"/contract/instrument:{api_symbol}"

        async for msg in self._ws_connect(market_type, topic):
            try:
                d = msg.get("data") or {}
                subject = msg.get("subject", "")
                mark = float(d.get("markPrice") or 0)
                if not mark and subject != "mark.index.price":
                    continue
                next_ft_raw = d.get("nextFundingTime")
                next_ft: Optional[int] = None
                if next_ft_raw:
                    try:
                        v = int(next_ft_raw)
                        if v > 0:
                            next_ft = v
                    except (TypeError, ValueError):
                        pass
                idx_raw  = d.get("indexPrice")
                rate_raw = d.get("fundingRate")
                ts       = self._ns_to_ms(d.get("timestamp") or d.get("ts") or int(time.time() * 1000))

                funding_block = None
                if rate_raw is not None and cycle_ms is not None and next_ft is not None:
                    funding_block = build_funding_current(
                        kind           = "discrete",
                        per_cycle      = float(rate_raw),
                        cycle_ms       = cycle_ms,
                        valid_until_ts = max(next_ft, ts + 1),
                    )

                yield MarkPrice(
                    symbol      = model_sym,
                    market_type = market_type,
                    quote       = quote,
                    mark_price  = mark,
                    index_price = float(idx_raw) if idx_raw is not None else None,
                    funding     = funding_block,
                    timestamp   = ts,
                )
            except (ValueError, TypeError, KeyError, IndexError, AttributeError) as e:
                logger.warning(f"KuCoin mark_price {api_symbol} malformed frame skipped: {e!r}")
