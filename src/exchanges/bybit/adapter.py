import httpx
import asyncio
import random
import time
import websockets
import logging
from typing import List, AsyncGenerator, Dict, Any, Optional, Callable
from src.exchanges.base import (
    BaseExchange, StreamHub, UpstreamUnavailableError,
    build_qty_value, build_volume_value, build_oi_value,
    build_funding_current, build_funding_historical, build_funding_convention,
)
from src.models import (
    Ticker, BookTicker, MarkPrice, OrderBook, Candle, Trade, AggTrade,
    MarketType, SymbolInfo, OpenInterest, FundingRate, Liquidation, LongShortRatio,
)


logger = logging.getLogger("bybit_adapter")


class BybitAdapter(BaseExchange):
    def __init__(self):
        super().__init__()
        self.http_client = httpx.AsyncClient(timeout=30.0)

        self._backoff_until = 0.0
        self._backoff_lock = asyncio.Lock()

        self.rest_url = "https://api.bybit.com"
        self.ws_urls = {
            MarketType.SPOT:    "wss://stream.bybit.com/v5/public/spot",
            MarketType.LINEAR:  "wss://stream.bybit.com/v5/public/linear",
            MarketType.INVERSE: "wss://stream.bybit.com/v5/public/inverse",
        }

        self._hubs: Dict[MarketType, StreamHub] = {}
        self._hubs_lock = asyncio.Lock()

        self._funding_interval_cache: Dict[MarketType, Dict[str, int]] = {}

        self._capabilities = self._build_capabilities()


    async def shutdown(self):
        for hub in list(self._hubs.values()):
            await hub.close()
        self._hubs.clear()
        await self.http_client.aclose()


    async def _warm(self) -> None:
        await self._step("spot_info",    self._ensure_info_cache(MarketType.SPOT))
        await self._step("linear_info",  self._ensure_info_cache(MarketType.LINEAR))
        await self._step("inverse_info", self._ensure_info_cache(MarketType.INVERSE))


    @property
    def name(self) -> str:
        return "bybit"


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
                        "depths":    [50, 200],
                        "max_depth": 200,
                    },
                    "trades": {
                        "rest":      True,
                        "ws":        True,
                        "max_limit": 60,
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
                        "ws":   False,
                    },
                    "orderbook": {
                        "rest":      True,
                        "ws":        True,
                        "depths":    [50, 200, 500],
                        "max_depth": 500,
                    },
                    "trades": {
                        "rest":      True,
                        "ws":        True,
                        "max_limit": 1000,
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
                        "max_limit":    None,
                        "retention_ms": None,
                    },
                    "open_interest": {
                        "rest":         True,
                        "ws":           False,
                        "paginated":    True,
                        "max_limit":    None,
                        "retention_ms": None,
                        "intervals":    ["5m", "15m", "30m", "1h", "4h", "1d"],
                    },
                    "liquidations": {
                        "rest":         False,
                        "ws":           True,
                        "paginated":    False,
                        "max_limit":    None,
                        "retention_ms": None,
                        "completeness": "partial",
                    },
                    "long_short_ratio": {
                        "rest":         True,
                        "ws":           False,
                        "paginated":    True,
                        "max_limit":    None,
                        "retention_ms": None,
                        "intervals":    ["5m", "15m", "30m", "1h", "4h", "1d"],
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
                        "ws":   False,
                    },
                    "orderbook": {
                        "rest":      True,
                        "ws":        True,
                        "depths":    [50, 200, 500],
                        "max_depth": 500,
                    },
                    "trades": {
                        "rest":      True,
                        "ws":        True,
                        "max_limit": 1000,
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
                        "max_limit":    None,
                        "retention_ms": None,
                    },
                    "open_interest": {
                        "rest":         True,
                        "ws":           False,
                        "paginated":    True,
                        "max_limit":    None,
                        "retention_ms": None,
                        "intervals":    ["5m", "15m", "30m", "1h", "4h", "1d"],
                    },
                    "liquidations": {
                        "rest":         False,
                        "ws":           True,
                        "paginated":    False,
                        "max_limit":    None,
                        "retention_ms": None,
                        "completeness": "partial",
                    },
                    "long_short_ratio": {
                        "rest":         True,
                        "ws":           False,
                        "paginated":    True,
                        "max_limit":    None,
                        "retention_ms": None,
                        "intervals":    ["5m", "15m", "30m", "1h", "4h", "1d"],
                    },
                },
            },
        }


    def normalize_timestamp(self, ts: Any) -> int:
        try:
            val = int(float(ts))
            if val < 100_000_000_000:
                return val * 1000
            return val
        except (ValueError, TypeError):
            raise ValueError(f"Invalid Bybit timestamp: {ts}")


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
        if market_type == MarketType.SPOT:
            return "spot"
        if market_type == MarketType.LINEAR:
            return "linear"
        if market_type == MarketType.INVERSE:
            return "inverse"
        raise ValueError(f"Unknown market type: {market_type}")


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


    def _map_candle_interval(self, interval: str) -> str:
        if interval == "1d": return "D"
        if interval == "1w": return "W"
        if interval == "1M": return "M"
        if interval.endswith("m"): return interval[:-1]
        if interval.endswith("h"): return str(int(interval[:-1]) * 60)
        return interval


    def _map_metric_interval(self, interval: str) -> str:
        if interval.endswith("m"):
            return f"{interval[:-1]}min"
        return interval


    async def _make_request(self, method: str, endpoint: str, params: Optional[Dict] = None) -> Any:
        url = f"{self.rest_url}{endpoint}"
        retries = 3
        while retries > 0:
            now = time.time()
            if now < self._backoff_until:
                wait_time = self._backoff_until - now
                if wait_time > 30:
                    raise UpstreamUnavailableError(f"Bybit IP ban in effect. Retry in {wait_time:.0f}s.", retry_after=wait_time)
                await asyncio.sleep(wait_time + random.uniform(0.05, 1.0))

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
                    result = data["result"]
                    if isinstance(result, dict) and "time" in data:
                        result.setdefault("_envelope_ts", data["time"])
                    return result

                if resp.status_code == 403:
                    ban_until = time.time() + 600
                    async with self._backoff_lock:
                        self._backoff_until = max(self._backoff_until, ban_until)
                    logger.error(f"Bybit IP ban (403). All requests blocked for 10 min.")
                    raise UpstreamUnavailableError("Bybit IP ban triggered (403). Retry in 10 minutes.", retry_after=600)

                if resp.status_code == 429:
                    async with self._backoff_lock:
                        self._backoff_until = max(self._backoff_until, time.time() + 5)
                    logger.error(f"Bybit rate limit (429). Backing off 5s.")
                    retries -= 1
                    continue

                resp.raise_for_status()

            except httpx.HTTPStatusError as e:
                if e.response.status_code in (502, 503, 504, 520):
                    logger.warning(f"Bybit HTTP {e.response.status_code} on {endpoint}. Retrying...")
                    retries -= 1
                    await asyncio.sleep(1)
                    continue
                if e.response.status_code >= 500:
                    raise UpstreamUnavailableError(f"Bybit upstream error {e.response.status_code} on {endpoint}")
                logger.error(f"HTTP Error: {e}")
                raise e
            except Exception as e:
                if isinstance(e, (ValueError, UpstreamUnavailableError)):
                    raise e
                logger.error(f"Request Error: {e}")
                retries -= 1
                await asyncio.sleep(1)

        remaining = self._backoff_until - time.time()
        raise UpstreamUnavailableError(f"Max retries exceeded for {url}", retry_after=remaining if remaining > 0 else None)


    async def _paginate_backwards(self, fetch_func_by_end: Callable, total_limit: int, limit_per_req: int) -> List[Any]:
        chunks = []
        seen = set()
        collected_count = 0
        current_end = None
        max_requests = 100
        request_count = 0

        while collected_count < total_limit and request_count < max_requests:
            req_size = limit_per_req
            try:
                batch = await fetch_func_by_end(current_end, req_size)
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
            collected_count += len(new_items)
            request_count += 1

            if not hasattr(batch[0], "timestamp"):
                break
            next_end = batch[0].timestamp
            if next_end <= 0 or (current_end is not None and next_end >= current_end):
                break
            current_end = next_end

        if request_count >= max_requests and collected_count < total_limit:
            logger.warning(f"Bybit pagination hit {max_requests}-page safety cap with {collected_count}/{total_limit} records; result truncated")

        chunks.reverse()
        final_results = [item for chunk in chunks for item in chunk]
        final_results.sort(key=lambda x: x.timestamp)
        return final_results[-total_limit:]


    async def _get_hub(self, market_type: MarketType) -> StreamHub:
        async with self._hubs_lock:
            hub = self._hubs.get(market_type)
            if hub is None:
                url = self.ws_urls[market_type]
                async def _connect():
                    return await websockets.connect(url, ping_interval=20, ping_timeout=20)
                hub = StreamHub(
                    name=f"bybit_{market_type.value}",
                    connect=_connect,
                    subscribe_payload=lambda ts: [{"op": "subscribe",   "args": ts, "req_id": f"sub-{int(time.time() * 1000)}"}],
                    unsubscribe_payload=lambda ts: [{"op": "unsubscribe", "args": ts, "req_id": f"unsub-{int(time.time() * 1000)}"}],
                    route=lambda msg: msg.get("topic") if isinstance(msg, dict) else None,
                    keepalive_payload={"op": "ping"},
                    keepalive_interval=20.0,
                )
                self._hubs[market_type] = hub
            return hub


    async def _ws_connect(self, market_type: MarketType, topics: list) -> AsyncGenerator[Dict, None]:
        topic = topics[0]
        hub = await self._get_hub(market_type)
        q = await hub.subscribe(topic)
        try:
            while True:
                msg = await q.get()
                yield msg
        finally:
            await hub.unsubscribe(topic, q)


    async def get_ticker(self, market_type: MarketType, symbol: str) -> Ticker:
        cat = self._get_category(market_type)
        api_symbol = self.get_api_symbol(symbol, market_type)

        data = await self._make_request("GET", "/v5/market/tickers", {"category": cat, "symbol": api_symbol})
        if not data["list"]:
            raise ValueError(f"Symbol {api_symbol} not found")
        t = data["list"][0]

        model_sym  = self.get_model_symbol(t["symbol"], market_type)
        info       = await self._info_for(market_type, model_sym)
        quote      = info.quote_asset if info else ""
        is_inverse = market_type == MarketType.INVERSE
        close      = float(t["lastPrice"])

        return Ticker(
            symbol               = model_sym,
            market_type          = market_type,
            quote                = quote,
            price                = close,
            open_24h             = float(t["prevPrice24h"]),
            high_24h             = float(t["highPrice24h"]),
            low_24h              = float(t["lowPrice24h"]),
            volume_24h           = build_volume_value(
                native        = float(t["volume24h"]),
                volume_unit   = "contract" if is_inverse else "base",
                contract_size = info.contract_size if info else None,
                close         = close,
            ),
            price_change_percent = float(t["price24hPcnt"]) * 100,
            timestamp            = self.normalize_timestamp(data.get("_envelope_ts") or int(time.time() * 1000)),
        )


    async def get_book_ticker(self, market_type: MarketType, symbol: str) -> BookTicker:
        cat = self._get_category(market_type)
        api_symbol = self.get_api_symbol(symbol, market_type)

        data = await self._make_request("GET", "/v5/market/tickers", {"category": cat, "symbol": api_symbol})
        if not data["list"]:
            raise ValueError(f"Symbol {api_symbol} not found")
        t = data["list"][0]

        model_sym     = self.get_model_symbol(t["symbol"], market_type)
        info          = await self._info_for(market_type, model_sym)
        quote         = info.quote_asset if info else ""
        contract_size = info.contract_size if info else None
        is_inverse    = market_type == MarketType.INVERSE
        unit          = "contract" if is_inverse else "base"
        bid_price     = float(t.get("bid1Price", 0))
        ask_price     = float(t.get("ask1Price", 0))

        return BookTicker(
            symbol      = model_sym,
            market_type = market_type,
            quote       = quote,
            bid_price   = bid_price,
            bid_qty     = build_qty_value(
                native        = float(t.get("bid1Size", 0)),
                qty_unit      = unit,
                contract_size = contract_size,
                price         = bid_price,
            ),
            ask_price   = ask_price,
            ask_qty     = build_qty_value(
                native        = float(t.get("ask1Size", 0)),
                qty_unit      = unit,
                contract_size = contract_size,
                price         = ask_price,
            ),
            timestamp   = self.normalize_timestamp(data.get("_envelope_ts") or int(time.time() * 1000)),
        )


    async def get_orderbook(self, market_type: MarketType, symbol: str, depth: int = 20) -> OrderBook:
        cat = self._get_category(market_type)
        api_symbol = self.get_api_symbol(symbol, market_type)

        ob_caps = self.get_capabilities()["markets"][market_type]["orderbook"]
        discrete_depth_bucket = next((d for d in ob_caps["depths"] if d >= depth), ob_caps["max_depth"])

        data = await self._make_request("GET", "/v5/market/orderbook", {"category": cat, "symbol": api_symbol, "limit": discrete_depth_bucket})

        model_sym = self.get_model_symbol(data["s"], market_type)
        info      = await self._info_for(market_type, model_sym)
        quote     = info.quote_asset if info else ""

        return OrderBook(
            symbol      = model_sym,
            market_type = market_type,
            quote       = quote,
            bids        = [[float(p), float(q)] for p, q in data["b"][:depth]],
            asks        = [[float(p), float(q)] for p, q in data["a"][:depth]],
            qty_unit    = "contract" if market_type == MarketType.INVERSE else "base",
            timestamp   = self.normalize_timestamp(data["ts"]),
        )


    async def get_trades(self, market_type: MarketType, symbol: str, limit: int = 100) -> List[Trade]:
        cat = self._get_category(market_type)
        api_symbol = self.get_api_symbol(symbol, market_type)

        max_limit = self.get_capabilities()["markets"][market_type]["trades"]["max_limit"]
        req_limit = min(limit, max_limit)

        data = await self._make_request("GET", "/v5/market/recent-trade", {"category": cat, "symbol": api_symbol, "limit": req_limit})

        model_sym     = self.get_model_symbol(api_symbol, market_type)
        info          = await self._info_for(market_type, model_sym)
        quote         = info.quote_asset if info else ""
        contract_size = info.contract_size if info else None
        is_inverse    = market_type == MarketType.INVERSE
        unit          = "contract" if is_inverse else "base"

        trades = []
        for t in data["list"]:
            price = float(t["price"])
            trades.append(Trade(
                id          = t["execId"],
                symbol      = model_sym,
                market_type = market_type,
                quote       = quote,
                price       = price,
                qty         = build_qty_value(
                    native        = float(t["size"]),
                    qty_unit      = unit,
                    contract_size = contract_size,
                    price         = price,
                ),
                side        = t["side"].lower(),
                timestamp   = self.normalize_timestamp(t["time"]),
            ))
        trades.sort(key=lambda t: t.timestamp)

        return trades[-limit:]


    async def get_agg_trades(self, market_type: MarketType, symbol: str, start_time: Optional[int] = None, limit: int = 500) -> List[AggTrade]:
        raise NotImplementedError(f"{self.name} does not support agg_trades for {market_type.value}")


    async def get_candles(self, market_type: MarketType, symbol: str, interval: str, start_time: Optional[int] = None, limit: int = 100) -> List[Candle]:
        cat        = self._get_category(market_type)
        api_symbol = self.get_api_symbol(symbol, market_type)
        model_sym  = self.get_model_symbol(api_symbol, market_type)
        is_inverse = market_type == MarketType.INVERSE
        unit       = "contract" if is_inverse else "base"

        info          = await self._info_for(market_type, model_sym)
        quote         = info.quote_asset if info else ""
        contract_size = info.contract_size if info else None

        bybit_interval = self._map_candle_interval(interval)

        max_per_req = 1000

        async def fetch_by_end(end_ts, l):
            anchor = end_ts if end_ts is not None else start_time
            p = {"category": cat, "symbol": api_symbol, "interval": bybit_interval, "limit": min(l, max_per_req)}
            if anchor:
                p["end"] = anchor
            data = await self._make_request("GET", "/v5/market/kline", p)
            if not data["list"]:
                return []

            parsed = []
            for k in data["list"]:
                close = float(k[4])
                parsed.append(Candle(
                    symbol      = model_sym,
                    market_type = market_type,
                    quote       = quote,
                    interval    = interval,
                    timestamp   = self.normalize_timestamp(k[0]),
                    open        = float(k[1]),
                    high        = float(k[2]),
                    low         = float(k[3]),
                    close       = close,
                    volume      = build_volume_value(
                        native        = float(k[5]),
                        volume_unit   = unit,
                        contract_size = contract_size,
                        close         = close,
                    ),
                ))

            return sorted(parsed, key=lambda x: x.timestamp)

        if start_time is not None or limit > max_per_req:
            return await self._paginate_backwards(fetch_by_end, limit, max_per_req)
        return await fetch_by_end(None, limit)


    async def get_mark_price(self, market_type: MarketType, symbol: str) -> MarkPrice:
        if market_type == MarketType.SPOT:
            raise NotImplementedError(f"{self.name} does not support mark_price for {market_type.value}")
        cat = self._get_category(market_type)
        api_symbol = self.get_api_symbol(symbol, market_type)

        data = await self._make_request("GET", "/v5/market/tickers", {"category": cat, "symbol": api_symbol})
        if not data["list"]:
            raise ValueError(f"Symbol {api_symbol} not found")
        t = data["list"][0]

        model_sym = self.get_model_symbol(t["symbol"], market_type)
        info      = await self._info_for(market_type, model_sym)
        quote     = info.quote_asset if info else ""
        cycle_ms  = await self._funding_interval_ms_for(market_type, model_sym)

        ts          = self.normalize_timestamp(data.get("_envelope_ts") or int(time.time() * 1000))
        next_ft_raw = t.get("nextFundingTime")
        next_ft: Optional[int] = None
        if next_ft_raw:
            try:
                v = int(next_ft_raw)
                if v > 0:
                    next_ft = v
            except (TypeError, ValueError):
                pass

        funding_block = None
        rate_raw      = t.get("fundingRate")
        if rate_raw is not None and cycle_ms is not None and next_ft is not None:
            funding_block = build_funding_current(
                kind           = "discrete",
                per_cycle      = float(rate_raw),
                cycle_ms       = cycle_ms,
                valid_until_ts = max(next_ft, ts + 1),
            )

        return MarkPrice(
            symbol      = model_sym,
            market_type = market_type,
            quote       = quote,
            mark_price  = float(t["markPrice"]),
            index_price = float(t["indexPrice"]),
            funding     = funding_block,
            timestamp   = ts,
        )


    async def get_funding_rate(self, market_type: MarketType, symbol: str, start_time: Optional[int] = None, limit: int = 100) -> List[FundingRate]:
        if market_type == MarketType.SPOT:
            raise NotImplementedError(f"{self.name} does not support funding_rate for {market_type.value}")
        cat        = self._get_category(market_type)
        api_symbol = self.get_api_symbol(symbol, market_type)
        model_sym  = self.get_model_symbol(api_symbol, market_type)
        info       = await self._info_for(market_type, model_sym)
        quote      = info.quote_asset if info else ""
        cycle_ms   = await self._funding_interval_ms_for(market_type, model_sym)
        cycle_ms   = cycle_ms if cycle_ms is not None else 8 * 3_600_000

        max_per_req = 200

        async def fetch_backwards(end_ts, l):
            anchor = end_ts if end_ts is not None else start_time
            p = {"category": cat, "symbol": api_symbol, "limit": min(l, max_per_req)}
            if anchor:
                p["endTime"] = anchor
            data = await self._make_request("GET", "/v5/market/funding/history", p)

            parsed = [FundingRate(
                symbol      = model_sym,
                market_type = market_type,
                quote       = quote,
                rate        = build_funding_historical(
                    kind      = "discrete",
                    per_cycle = float(i["fundingRate"]),
                    cycle_ms  = cycle_ms,
                ),
                timestamp   = self.normalize_timestamp(i["fundingRateTimestamp"]),
            ) for i in data.get("list", [])]
            return sorted(parsed, key=lambda x: x.timestamp)

        return await self._paginate_backwards(fetch_backwards, limit, max_per_req)


    async def get_open_interest(self, market_type: MarketType, symbol: str, period: str = "1h", start_time: Optional[int] = None, limit: int = 30) -> List[OpenInterest]:
        if market_type == MarketType.SPOT:
            raise NotImplementedError(f"{self.name} does not support open_interest for {market_type.value}")
        cat = self._get_category(market_type)
        api_symbol = self.get_api_symbol(symbol, market_type)
        model_sym = self.get_model_symbol(api_symbol, market_type)

        interval      = self._map_metric_interval(period)
        is_inverse    = market_type == MarketType.INVERSE
        oi_unit       = "contract" if is_inverse else "base"
        info          = await self._info_for(market_type, model_sym)
        quote         = info.quote_asset if info else ""
        contract_size = info.contract_size if info else None
        max_per_req   = 200

        async def fetch_backwards(end_ts, l):
            anchor = end_ts if end_ts is not None else start_time
            p = {"category": cat, "symbol": api_symbol, "intervalTime": interval, "limit": min(l, max_per_req)}
            if anchor:
                p["endTime"] = anchor

            data = await self._make_request("GET", "/v5/market/open-interest", p)

            res_list = [OpenInterest(
                symbol        = model_sym,
                market_type   = market_type,
                quote         = quote,
                interval      = period,
                open_interest = build_oi_value(
                    native          = float(i["openInterest"]),
                    oi_unit         = oi_unit,
                    contract_size   = contract_size,
                    candle_close    = None,
                    candle_close_ts = None,
                ),
                timestamp     = self.normalize_timestamp(i["timestamp"]),
            ) for i in data["list"]]

            return sorted(res_list, key=lambda x: x.timestamp)

        return await self._paginate_backwards(fetch_backwards, limit, max_per_req)


    async def get_long_short_ratio(self, market_type: MarketType, symbol: str, period: str = "5m", start_time: Optional[int] = None, limit: int = 30) -> List[LongShortRatio]:
        if market_type == MarketType.SPOT:
            raise NotImplementedError(f"{self.name} does not support long_short_ratio for {market_type.value}")
        cat        = self._get_category(market_type)
        api_symbol = self.get_api_symbol(symbol, market_type)
        interval   = self._map_metric_interval(period)
        model_sym  = self.get_model_symbol(api_symbol, market_type)
        per_req    = 500

        async def fetch_batch_backward(end_ts, l):
            anchor = end_ts if end_ts is not None else start_time
            p = {"category": cat, "symbol": api_symbol, "period": interval, "limit": min(l, per_req)}
            if anchor:
                p["endTime"] = anchor
            data = await self._make_request("GET", "/v5/market/account-ratio", p)
            out = []
            for i in data.get("list", []):
                sell = float(i["sellRatio"])
                if sell <= 0:
                    continue
                out.append(LongShortRatio(
                    symbol        = model_sym,
                    market_type   = market_type,
                    interval      = period,
                    ratio         = float(i["buyRatio"]) / sell,
                    long_account  = float(i["buyRatio"]),
                    short_account = sell,
                    account_scope = "all_accounts",
                    timestamp     = self.normalize_timestamp(i["timestamp"]),
                ))
            return out

        return await self._paginate_backwards(fetch_batch_backward, limit, per_req)


    @staticmethod
    def _precision(value: str) -> int:
        if not value or "." not in value:
            return 0
        return len(value.split(".")[1].rstrip("0"))


    async def _fetch_exchange_info(self, market_type: MarketType) -> List[SymbolInfo]:
        cat = self._get_category(market_type)
        params: dict = {"category": cat, "limit": 1000}
        if market_type == MarketType.LINEAR:
            params["contractType"] = "LinearPerpetual"
        elif market_type == MarketType.INVERSE:
            params["contractType"] = "InversePerpetual"

        instruments = []
        cursor      = None
        pages       = 0
        while pages < 50:
            if cursor:
                params["cursor"] = cursor
            data = await self._make_request("GET", "/v5/market/instruments-info", params)
            instruments.extend(data["list"])
            pages += 1
            cursor = data.get("nextPageCursor")
            if not cursor:
                break

        if cursor:
            logger.warning(f"Bybit instruments-info stopped at the {pages}-page cap with a cursor still set")

        results = []
        for s in instruments:
            if s["status"] != "Trading":
                continue
            if market_type == MarketType.LINEAR and s.get("contractType") != "LinearPerpetual":
                continue
            if market_type == MarketType.INVERSE and s.get("contractType") != "InversePerpetual":
                continue
            min_qty: Optional[float] = None
            max_qty: Optional[float] = None
            min_notional: Optional[float] = None
            lot = s.get("lotSizeFilter") or {}
            raw_min_qty = lot.get("minOrderQty")
            raw_max_qty = lot.get("maxOrderQty")
            raw_min_notional = lot.get("minNotionalValue")
            if raw_min_qty is not None:
                v = float(raw_min_qty)
                min_qty = v if v > 0 else None
            if raw_max_qty is not None:
                v = float(raw_max_qty)
                max_qty = v if v > 0 else None
            if raw_min_notional is not None:
                v = float(raw_min_notional)
                min_notional = v if v > 0 else None

            qty_unit = "contract" if market_type == MarketType.INVERSE else "base"
            contract_size: Optional[float] = 1.0 if market_type == MarketType.INVERSE else None

            funding_interval_ms: Optional[int] = None
            funding_block = None
            if market_type != MarketType.SPOT:
                fi_raw = s.get("fundingInterval")
                if fi_raw is not None:
                    try:
                        funding_interval_ms = int(fi_raw) * 60_000
                    except (TypeError, ValueError):
                        funding_interval_ms = None
                funding_block = build_funding_convention("discrete")

            self._funding_interval_cache.setdefault(market_type, {})
            if funding_interval_ms is not None:
                self._funding_interval_cache[market_type][s["symbol"]] = funding_interval_ms

            results.append(SymbolInfo(
                symbol             = s["symbol"],
                native_symbol      = s["symbol"],
                base_asset         = s["baseCoin"],
                quote_asset        = s["quoteCoin"],
                price_precision    = self._precision(s.get("priceFilter", {}).get("tickSize", "0.01")),
                quantity_precision = self._precision(s.get("lotSizeFilter", {}).get("qtyStep", "0.001")),
                min_qty            = min_qty,
                max_qty            = max_qty,
                min_notional       = min_notional,
                qty_unit           = qty_unit,
                contract_size      = contract_size,
                funding            = funding_block,
            ))
        return results


    async def _funding_interval_ms_for(self, market_type: MarketType, model_symbol: str) -> Optional[int]:
        await self._ensure_info_cache(market_type)
        return self._funding_interval_cache.get(market_type, {}).get(model_symbol)


    async def get_exchange_info(self, market_type: MarketType) -> List[SymbolInfo]:
        cache = await self._ensure_info_cache(market_type)
        return list(cache.values())


    async def get_symbol_info(self, market_type: MarketType, symbol: str) -> SymbolInfo:
        cache = await self._ensure_info_cache(market_type)
        model_sym = self.get_model_symbol(self.get_api_symbol(symbol, market_type), market_type)
        info = cache.get(model_sym)
        if info is None:
            raise ValueError(f"Symbol {symbol} not found on Bybit {market_type.value}")
        return info


    async def stream_ticker(self, market_type: MarketType, symbol: str) -> AsyncGenerator[Ticker, None]:
        api_symbol = self.get_api_symbol(symbol, market_type)
        topic      = f"tickers.{api_symbol}"
        is_inverse = market_type == MarketType.INVERSE
        unit       = "contract" if is_inverse else "base"

        model_sym_for_lookup = self.get_model_symbol(api_symbol, market_type)
        info                 = await self._info_for(market_type, model_sym_for_lookup)
        quote                = info.quote_asset if info else ""
        contract_size        = info.contract_size if info else None

        state: Dict[str, Any] = {}
        bootstrapped          = False

        async for msg in self._ws_connect(market_type, [topic]):
            d = msg.get("data") or {}
            if msg.get("type") == "snapshot":
                state        = dict(d)
                bootstrapped = True
            else:
                state.update(d)
            if not bootstrapped:
                continue
            close_price = float(state.get("lastPrice") or 0)
            yield Ticker(
                symbol               = self.get_model_symbol(state.get("symbol", api_symbol), market_type),
                market_type          = market_type,
                quote                = quote,
                price                = close_price,
                open_24h             = float(state.get("prevPrice24h") or 0),
                high_24h             = float(state.get("highPrice24h") or 0),
                low_24h              = float(state.get("lowPrice24h") or 0),
                volume_24h           = build_volume_value(
                    native        = float(state.get("volume24h") or 0),
                    volume_unit   = unit,
                    contract_size = contract_size,
                    close         = close_price,
                ),
                price_change_percent = float(state.get("price24hPcnt") or 0) * 100,
                timestamp            = self.normalize_timestamp(msg.get("ts", time.time())),
            )


    async def stream_book_ticker(self, market_type: MarketType, symbol: str) -> AsyncGenerator[BookTicker, None]:
        api_symbol = self.get_api_symbol(symbol, market_type)
        topic      = f"orderbook.1.{api_symbol}"
        is_inverse = market_type == MarketType.INVERSE
        unit       = "contract" if is_inverse else "base"

        model_sym_for_lookup = self.get_model_symbol(api_symbol, market_type)
        info                 = await self._info_for(market_type, model_sym_for_lookup)
        quote                = info.quote_asset if info else ""
        contract_size        = info.contract_size if info else None

        bids: Dict[float, float] = {}
        asks: Dict[float, float] = {}

        async for msg in self._ws_connect(market_type, [topic]):
            d = msg.get("data") or {}
            if msg.get("type") == "snapshot":
                bids.clear()
                asks.clear()
            for p_str, q_str in d.get("b", []):
                p, q = float(p_str), float(q_str)
                if q == 0:
                    bids.pop(p, None)
                else:
                    bids[p] = q
            for p_str, q_str in d.get("a", []):
                p, q = float(p_str), float(q_str)
                if q == 0:
                    asks.pop(p, None)
                else:
                    asks[p] = q

            if not bids or not asks:
                continue
            bid_price = max(bids)
            ask_price = min(asks)
            yield BookTicker(
                symbol      = self.get_model_symbol(d.get("s", api_symbol), market_type),
                market_type = market_type,
                quote       = quote,
                bid_price   = bid_price,
                bid_qty     = build_qty_value(
                    native        = bids[bid_price],
                    qty_unit      = unit,
                    contract_size = contract_size,
                    price         = bid_price,
                ),
                ask_price   = ask_price,
                ask_qty     = build_qty_value(
                    native        = asks[ask_price],
                    qty_unit      = unit,
                    contract_size = contract_size,
                    price         = ask_price,
                ),
                timestamp   = self.normalize_timestamp(msg.get("ts")),
            )


    async def stream_orderbook(self, market_type: MarketType, symbol: str, depth: int = 20, update_speed: str = "100ms") -> AsyncGenerator[OrderBook, None]:
        api_symbol = self.get_api_symbol(symbol, market_type)
        model_sym  = self.get_model_symbol(api_symbol, market_type)
        info       = await self._info_for(market_type, model_sym)
        quote      = info.quote_asset if info else ""

        if depth <= 1:
            lvl = 1
        elif depth <= 50:
            lvl = 50
        elif depth <= 200:
            lvl = 200
        else:
            lvl = 500
        topic = f"orderbook.{lvl}.{api_symbol}"

        bids: Dict[float, float] = {}
        asks: Dict[float, float] = {}
        last_u: Optional[int] = None

        async for msg in self._ws_connect(market_type, [topic]):
            d = msg.get("data") or {}
            u = d.get("u")
            if msg.get("type") == "snapshot":
                bids.clear()
                asks.clear()
            elif last_u is not None and u is not None and u != last_u + 1:
                logger.warning(f"Bybit orderbook {api_symbol} sequence gap: expected u={last_u + 1}, got u={u}")
            last_u = u
            for p_str, q_str in d.get("b", []):
                p, q = float(p_str), float(q_str)
                if q == 0:
                    bids.pop(p, None)
                else:
                    bids[p] = q
            for p_str, q_str in d.get("a", []):
                p, q = float(p_str), float(q_str)
                if q == 0:
                    asks.pop(p, None)
                else:
                    asks[p] = q

            sorted_bids = sorted(bids.items(), key=lambda kv: -kv[0])[:depth]
            sorted_asks = sorted(asks.items(), key=lambda kv: kv[0])[:depth]

            yield OrderBook(
                symbol      = self.get_model_symbol(d.get("s", api_symbol), market_type),
                market_type = market_type,
                quote       = quote,
                bids        = [[p, q] for p, q in sorted_bids],
                asks        = [[p, q] for p, q in sorted_asks],
                qty_unit    = "contract" if market_type == MarketType.INVERSE else "base",
                timestamp   = self.normalize_timestamp(msg.get("ts")),
            )


    async def stream_trades(self, market_type: MarketType, symbol: str) -> AsyncGenerator[Trade, None]:
        api_symbol = self.get_api_symbol(symbol, market_type)
        topic      = f"publicTrade.{api_symbol}"
        model_sym  = self.get_model_symbol(api_symbol, market_type)
        is_inverse = market_type == MarketType.INVERSE
        unit       = "contract" if is_inverse else "base"

        info          = await self._info_for(market_type, model_sym)
        quote         = info.quote_asset if info else ""
        contract_size = info.contract_size if info else None

        async for msg in self._ws_connect(market_type, [topic]):
            for t in msg.get("data", []):
                price = float(t["p"])
                yield Trade(
                    id          = t["i"],
                    symbol      = model_sym,
                    market_type = market_type,
                    quote       = quote,
                    price       = price,
                    qty         = build_qty_value(
                        native        = float(t["v"]),
                        qty_unit      = unit,
                        contract_size = contract_size,
                        price         = price,
                    ),
                    side        = t["S"].lower(),
                    timestamp   = self.normalize_timestamp(t["T"]),
                )


    async def stream_liquidations(self, market_type: MarketType, symbol: str) -> AsyncGenerator[Liquidation, None]:
        if market_type == MarketType.SPOT:
            raise NotImplementedError(f"{self.name} does not support stream_liquidations for {market_type.value}")
        api_symbol = self.get_api_symbol(symbol, market_type)
        topic      = f"allLiquidation.{api_symbol}"
        model_sym  = self.get_model_symbol(api_symbol, market_type)
        is_inverse = market_type == MarketType.INVERSE
        unit       = "contract" if is_inverse else "base"

        info          = await self._info_for(market_type, model_sym)
        quote         = info.quote_asset if info else ""
        contract_size = info.contract_size if info else None

        async for msg in self._ws_connect(market_type, [topic]):
            for d in msg.get("data", []):
                side_raw = d.get("S")
                if side_raw is None:
                    continue
                side_norm = side_raw.lower()
                if side_norm not in ("buy", "sell"):
                    continue
                price = float(d.get("p", 0))
                yield Liquidation(
                    symbol      = self.get_model_symbol(d.get("s", api_symbol), market_type),
                    market_type = market_type,
                    quote       = quote,
                    side        = side_norm,
                    price       = price,
                    qty         = build_qty_value(
                        native        = float(d.get("v", 0)),
                        qty_unit      = unit,
                        contract_size = contract_size,
                        price         = price,
                    ),
                    timestamp   = self.normalize_timestamp(d.get("T", msg.get("ts"))),
                )
