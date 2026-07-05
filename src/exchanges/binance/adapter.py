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


logger = logging.getLogger("binance_adapter")


class BinanceAdapter(BaseExchange):
    _USDM_BUCKETS: Dict[str, str] = {
        "ticker":      "market",
        "miniTicker":  "market",
        "markPrice":   "market",
        "aggTrade":    "market",
        "forceOrder":  "market",
        "trade":       "public",
        "bookTicker":  "public",
        "depth":       "public",
    }

    _WEIGHT_LIMITS: Dict[str, int] = {
        "api.binance.com":  6000,
        "fapi.binance.com": 2400,
        "dapi.binance.com": 2400,
    }


    def __init__(self):
        super().__init__()
        self.http_client = httpx.AsyncClient(timeout=30.0)

        self._backoff_until: Dict[str, float] = {}
        self._backoff_lock = asyncio.Lock()

        self.rest_urls = {
            MarketType.SPOT:    "https://api.binance.com",
            MarketType.LINEAR:  "https://fapi.binance.com",
            MarketType.INVERSE: "https://dapi.binance.com",
        }
        self.ws_urls = {
            MarketType.SPOT:    "wss://stream.binance.com:9443/stream",
            MarketType.INVERSE: "wss://dstream.binance.com/stream",
        }

        self._hubs: Dict[str, StreamHub] = {}
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
        return "binance"


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
                        "depths":    [5, 10, 20, 50, 100, 500, 1000],
                        "max_depth": 1000,
                    },
                    "trades": {
                        "rest":      True,
                        "ws":        True,
                        "max_limit": 1000,
                    },
                    "agg_trades": {
                        "rest":         True,
                        "ws":           True,
                        "paginated":    True,
                        "max_limit":    None,
                        "retention_ms": None,
                    },
                    "candles": {
                        "rest":         True,
                        "ws":           False,
                        "paginated":    True,
                        "max_limit":    None,
                        "retention_ms": None,
                        "intervals":    ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"],
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
                        "depths":    [5, 10, 20, 50, 100, 500, 1000],
                        "max_depth": 1000,
                    },
                    "trades": {
                        "rest":      True,
                        "ws":        True,
                        "max_limit": 1000,
                    },
                    "agg_trades": {
                        "rest":         True,
                        "ws":           True,
                        "paginated":    True,
                        "max_limit":    None,
                        "retention_ms": None,
                    },
                    "candles": {
                        "rest":         True,
                        "ws":           False,
                        "paginated":    True,
                        "max_limit":    None,
                        "retention_ms": None,
                        "intervals":    ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"],
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
                        "retention_ms": 30 * 24 * 60 * 60 * 1000,
                        "intervals":    ["5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"],
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
                        "depths":    [5, 10, 20, 50, 100, 500, 1000],
                        "max_depth": 1000,
                    },
                    "trades": {
                        "rest":      True,
                        "ws":        True,
                        "max_limit": 1000,
                    },
                    "agg_trades": {
                        "rest":         True,
                        "ws":           True,
                        "paginated":    True,
                        "max_limit":    None,
                        "retention_ms": None,
                    },
                    "candles": {
                        "rest":         True,
                        "ws":           False,
                        "paginated":    True,
                        "max_limit":    None,
                        "retention_ms": None,
                        "intervals":    ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"],
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
                        "retention_ms": 30 * 24 * 60 * 60 * 1000,
                        "intervals":    ["5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"],
                    },
                },
            },
        }


    def _get_rest_url(self, market_type: MarketType) -> str:
        if market_type not in self.rest_urls:
            raise ValueError(f"Market type {market_type} not supported by Binance adapter.")
        return self.rest_urls[market_type]


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
        if not interval:
            return 0
        unit = interval[-1]
        try:
            val = int(interval[:-1])
        except (ValueError, TypeError):
            return 0

        if unit == "s": return val * 1000
        if unit == "m": return val * 60 * 1000
        if unit == "h": return val * 60 * 60 * 1000
        if unit == "d": return val * 24 * 60 * 60 * 1000
        if unit == "w": return val * 7 * 24 * 60 * 60 * 1000
        if unit == "M": return val * 30 * 24 * 60 * 60 * 1000

        return 0


    async def _make_request(self, method: str, url: str, params: Optional[Dict] = None) -> Any:
        host = url.split("/", 3)[2]
        retries = 3
        while retries > 0:
            now = time.time()
            host_backoff = self._backoff_until.get(host, 0.0)
            if now < host_backoff:
                wait_time = host_backoff - now + random.uniform(0.05, 1.0)
                await asyncio.sleep(wait_time)

            try:
                resp = await self.http_client.request(method, url, params=params)

                used_weight  = int(resp.headers.get("x-mbx-used-weight-1m", 0))
                weight_limit = self._WEIGHT_LIMITS.get(host, 1200)

                if used_weight > weight_limit * 0.95:
                    async with self._backoff_lock:
                        if time.time() > self._backoff_until.get(host, 0.0):
                            self._backoff_until[host] = time.time() + 2
                    logger.warning(f"High API Weight on {host}: {used_weight}/{weight_limit}. Pausing its requests for 2s...")

                if resp.status_code == 200:
                    return resp.json()

                if resp.status_code in [429, 418]:
                    retry_after = int(resp.headers.get("Retry-After", 5))

                    async with self._backoff_lock:
                        self._backoff_until[host] = max(self._backoff_until.get(host, 0.0), time.time() + retry_after)

                    logger.error(f"Rate Limit Hit ({resp.status_code}) on {host}. Backoff set for {retry_after}s.")
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
                if e.response.status_code in (502, 503, 504, 520):
                    logger.warning(f"HTTP {e.response.status_code} on {host}. Retrying...")
                    retries -= 1
                    await asyncio.sleep(1)
                    continue
                if e.response.status_code >= 500:
                    raise UpstreamUnavailableError(f"Binance upstream error {e.response.status_code} on {url}")
                raise e

            except Exception as e:
                if isinstance(e, ValueError):
                    raise e
                logger.error(f"Request Error: {e}")
                retries -= 1
                await asyncio.sleep(1)

        remaining = self._backoff_until.get(host, 0.0) - time.time()
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
            logger.warning(f"Binance pagination hit {max_requests}-page safety cap with {collected_count}/{total_limit} records; result truncated")

        chunks.reverse()
        final_results = [item for chunk in chunks for item in chunk]
        final_results.sort(key=lambda x: x.timestamp)
        return final_results[-total_limit:]


    def _usdm_bucket_for_topic(self, topic: str) -> str:
        if "@" not in topic:
            return "public"
        channel = topic.split("@", 1)[1].split("@", 1)[0]
        if channel.startswith("depth"):
            channel = "depth"
        return self._USDM_BUCKETS.get(channel, "public")


    def _hub_key(self, market_type: MarketType, topic: str) -> str:
        if market_type == MarketType.LINEAR:
            return f"{market_type.value}:{self._usdm_bucket_for_topic(topic)}"
        return market_type.value


    def _hub_url(self, market_type: MarketType, topic: str) -> str:
        if market_type == MarketType.LINEAR:
            bucket = self._usdm_bucket_for_topic(topic)
            return f"wss://fstream.binance.com/{bucket}/stream"
        return self.ws_urls[market_type]


    async def _get_hub(self, market_type: MarketType, topic: str) -> StreamHub:
        key = self._hub_key(market_type, topic)
        async with self._hubs_lock:
            hub = self._hubs.get(key)
            if hub is None:
                url = self._hub_url(market_type, topic)
                async def _connect():
                    return await websockets.connect(url, ping_interval=20, ping_timeout=20)
                hub = StreamHub(
                    name=f"binance_{key}",
                    connect=_connect,
                    subscribe_payload=lambda ts: [{"method": "SUBSCRIBE",   "params": ts, "id": int(time.time() * 1000)}],
                    unsubscribe_payload=lambda ts: [{"method": "UNSUBSCRIBE", "params": ts, "id": int(time.time() * 1000)}],
                    route=lambda msg: msg.get("stream") if isinstance(msg, dict) else None,
                )
                self._hubs[key] = hub
            return hub


    async def _ws_connect(self, market_type: MarketType, params: list) -> AsyncGenerator[Dict, None]:
        topic = params[0]
        hub   = await self._get_hub(market_type, topic)
        q     = await hub.subscribe(topic)

        try:
            while True:
                msg  = await q.get()
                data = msg.get("data") if isinstance(msg, dict) else None
                if data is None:
                    continue
                yield data
        finally:
            await hub.unsubscribe(topic, q)


    async def get_ticker(self, market_type: MarketType, symbol: str) -> Ticker:
        base_url = self._get_rest_url(market_type)
        api_symbol = self.get_api_symbol(symbol, market_type)

        endpoint = "/api/v3/ticker/24hr" if market_type == MarketType.SPOT else "/fapi/v1/ticker/24hr"
        if market_type == MarketType.INVERSE:
            endpoint = "/dapi/v1/ticker/24hr"

        data = await self._make_request("GET", f"{base_url}{endpoint}", params={"symbol": api_symbol})
        if isinstance(data, list):
            data = data[0]

        model_sym = self.get_model_symbol(data["symbol"], market_type)
        info      = await self._info_for(market_type, model_sym)
        quote     = info.quote_asset if info else ""

        is_inverse    = market_type == MarketType.INVERSE
        volume_native = float(data["volume"])
        close_price   = float(data["lastPrice"])

        return Ticker(
            symbol               = model_sym,
            market_type          = market_type,
            quote                = quote,
            price                = close_price,
            open_24h             = float(data["openPrice"]),
            high_24h             = float(data["highPrice"]),
            low_24h              = float(data["lowPrice"]),
            volume_24h           = build_volume_value(
                native        = volume_native,
                volume_unit   = "contract" if is_inverse else "base",
                contract_size = info.contract_size if info else None,
                close         = close_price,
            ),
            price_change_percent = float(data["priceChangePercent"]),
            timestamp            = self.normalize_timestamp(data["closeTime"]),
        )


    async def get_book_ticker(self, market_type: MarketType, symbol: str) -> BookTicker:
        base_url = self._get_rest_url(market_type)
        api_symbol = self.get_api_symbol(symbol, market_type)

        endpoint = "/api/v3/ticker/bookTicker" if market_type == MarketType.SPOT else "/fapi/v1/ticker/bookTicker"
        if market_type == MarketType.INVERSE:
            endpoint = "/dapi/v1/ticker/bookTicker"

        data = await self._make_request("GET", f"{base_url}{endpoint}", params={"symbol": api_symbol})
        if isinstance(data, list):
            data = data[0]

        model_sym = self.get_model_symbol(data["symbol"], market_type)
        info      = await self._info_for(market_type, model_sym)
        quote     = info.quote_asset if info else ""

        is_inverse    = market_type == MarketType.INVERSE
        unit          = "contract" if is_inverse else "base"
        contract_size = info.contract_size if info else None
        bid_price     = float(data["bidPrice"])
        ask_price     = float(data["askPrice"])

        return BookTicker(
            symbol      = model_sym,
            market_type = market_type,
            quote       = quote,
            bid_price   = bid_price,
            bid_qty     = build_qty_value(
                native        = float(data["bidQty"]),
                qty_unit      = unit,
                contract_size = contract_size,
                price         = bid_price,
            ),
            ask_price   = ask_price,
            ask_qty     = build_qty_value(
                native        = float(data["askQty"]),
                qty_unit      = unit,
                contract_size = contract_size,
                price         = ask_price,
            ),
            timestamp   = self.normalize_timestamp(data.get("time") or int(time.time() * 1000)),
        )


    async def get_orderbook(self, market_type: MarketType, symbol: str, depth: int = 20) -> OrderBook:
        base_url = self._get_rest_url(market_type)
        api_symbol = self.get_api_symbol(symbol, market_type)

        path = "/api/v3/depth" if market_type == MarketType.SPOT else "/fapi/v1/depth"
        if market_type == MarketType.INVERSE:
            path = "/dapi/v1/depth"

        ob_caps = self.get_capabilities()["markets"][market_type]["orderbook"]
        discrete_depth_bucket = next((d for d in ob_caps["depths"] if d >= depth), ob_caps["max_depth"])

        data = await self._make_request("GET", f"{base_url}{path}", params={"symbol": api_symbol, "limit": discrete_depth_bucket})
        ts = data.get("E") or data.get("T") or int(time.time() * 1000)

        model_sym = self.get_model_symbol(api_symbol, market_type)
        info      = await self._info_for(market_type, model_sym)
        quote     = info.quote_asset if info else ""

        return OrderBook(
            symbol      = model_sym,
            market_type = market_type,
            quote       = quote,
            bids        = [[float(p), float(q)] for p, q in data["bids"][:depth]],
            asks        = [[float(p), float(q)] for p, q in data["asks"][:depth]],
            qty_unit    = "contract" if market_type == MarketType.INVERSE else "base",
            timestamp   = self.normalize_timestamp(ts),
        )


    async def get_trades(self, market_type: MarketType, symbol: str, limit: int = 100) -> List[Trade]:
        limit = min(limit, self.get_capabilities()["markets"][market_type]["trades"]["max_limit"])
        base_url = self._get_rest_url(market_type)
        api_symbol = self.get_api_symbol(symbol, market_type)

        path = "/api/v3/trades" if market_type == MarketType.SPOT else "/fapi/v1/trades"
        if market_type == MarketType.INVERSE:
            path = "/dapi/v1/trades"

        data = await self._make_request("GET", f"{base_url}{path}", params={"symbol": api_symbol, "limit": limit})

        model_sym     = self.get_model_symbol(api_symbol, market_type)
        info          = await self._info_for(market_type, model_sym)
        quote         = info.quote_asset if info else ""
        contract_size = info.contract_size if info else None
        is_inverse    = market_type == MarketType.INVERSE
        unit          = "contract" if is_inverse else "base"

        trades = []
        for t in data:
            price = float(t["price"])
            trades.append(Trade(
                id          = str(t["id"]),
                symbol      = model_sym,
                market_type = market_type,
                quote       = quote,
                price       = price,
                qty         = build_qty_value(
                    native        = float(t["qty"]),
                    qty_unit      = unit,
                    contract_size = contract_size,
                    price         = price,
                ),
                side        = "sell" if t["isBuyerMaker"] else "buy",
                timestamp   = self.normalize_timestamp(t["time"]),
            ))

        trades.sort(key=lambda t: t.timestamp)
        return trades[-limit:]


    async def get_agg_trades(self, market_type: MarketType, symbol: str, start_time: Optional[int] = None, limit: int = 500) -> List[AggTrade]:
        base_url = self._get_rest_url(market_type)
        api_symbol = self.get_api_symbol(symbol, market_type)

        path = "/api/v3/aggTrades" if market_type == MarketType.SPOT else "/fapi/v1/aggTrades"
        if market_type == MarketType.INVERSE:
            path = "/dapi/v1/aggTrades"

        model_sym     = self.get_model_symbol(api_symbol, market_type)
        info          = await self._info_for(market_type, model_sym)
        quote         = info.quote_asset if info else ""
        contract_size = info.contract_size if info else None
        is_inverse    = market_type == MarketType.INVERSE
        unit          = "contract" if is_inverse else "base"

        async def fetch_batch_by_end(end_ts, l):
            anchor = end_ts if end_ts is not None else start_time
            p = {"symbol": api_symbol, "limit": l}
            if anchor:
                p["endTime"] = anchor
            data = await self._make_request("GET", f"{base_url}{path}", params=p)
            out = []
            for t in data:
                price = float(t["p"])
                out.append(AggTrade(
                    agg_id         = str(t["a"]),
                    symbol         = model_sym,
                    market_type    = market_type,
                    quote          = quote,
                    price          = price,
                    qty            = build_qty_value(
                        native        = float(t["q"]),
                        qty_unit      = unit,
                        contract_size = contract_size,
                        price         = price,
                    ),
                    first_trade_id = str(t["f"]),
                    last_trade_id  = str(t["l"]),
                    side           = "sell" if t["m"] else "buy",
                    timestamp      = self.normalize_timestamp(t["T"]),
                ))
            return out

        if start_time or limit > 1000:
            return await self._paginate_backwards(fetch_batch_by_end, limit, 1000)
        return await fetch_batch_by_end(None, limit)


    async def get_candles(self, market_type: MarketType, symbol: str, interval: str, start_time: Optional[int] = None, limit: int = 100) -> List[Candle]:
        base_url = self._get_rest_url(market_type)
        api_symbol = self.get_api_symbol(symbol, market_type)

        path = "/api/v3/klines" if market_type == MarketType.SPOT else "/fapi/v1/klines"
        if market_type == MarketType.INVERSE:
            path = "/dapi/v1/klines"

        model_sym     = self.get_model_symbol(api_symbol, market_type)
        info          = await self._info_for(market_type, model_sym)
        quote         = info.quote_asset if info else ""
        contract_size = info.contract_size if info else None
        is_inverse    = market_type == MarketType.INVERSE
        unit          = "contract" if is_inverse else "base"

        def _parse_candles(data):
            out = []
            for k in data:
                close = float(k[4])
                out.append(Candle(
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
            return out

        async def fetch_batch_by_end(end_ts, l):
            anchor = end_ts if end_ts is not None else start_time
            p = {"symbol": api_symbol, "interval": interval, "limit": l}
            if anchor:
                p["endTime"] = anchor
            return _parse_candles(await self._make_request("GET", f"{base_url}{path}", params=p))

        max_per_req = 1500 if market_type != MarketType.SPOT else 1000

        if start_time is not None or limit > max_per_req:
            return await self._paginate_backwards(fetch_batch_by_end, limit, max_per_req)
        return await fetch_batch_by_end(None, limit)


    async def get_mark_price(self, market_type: MarketType, symbol: str) -> MarkPrice:
        if market_type == MarketType.SPOT:
            raise NotImplementedError(f"{self.name} does not support mark_price for {market_type.value}")

        base_url = self._get_rest_url(market_type)
        api_symbol = self.get_api_symbol(symbol, market_type)

        endpoint = "/fapi/v1/premiumIndex" if market_type == MarketType.LINEAR else "/dapi/v1/premiumIndex"

        data = await self._make_request("GET", f"{base_url}{endpoint}", params={"symbol": api_symbol})
        if isinstance(data, list):
            data = data[0]

        model_sym = self.get_model_symbol(data["symbol"], market_type)
        info      = await self._info_for(market_type, model_sym)
        quote     = info.quote_asset if info else ""
        cycle_ms  = await self._funding_interval_ms_for(market_type, model_sym)

        ts          = self.normalize_timestamp(data["time"])
        next_ft_raw = data.get("nextFundingTime")
        next_ft     = self.normalize_timestamp(next_ft_raw) if next_ft_raw else None

        funding_block = None
        rate_raw      = data.get("lastFundingRate")
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
            mark_price  = float(data["markPrice"]),
            index_price = float(data["indexPrice"]),
            funding     = funding_block,
            timestamp   = ts,
        )


    async def get_funding_rate(self, market_type: MarketType, symbol: str, start_time: Optional[int] = None, limit: int = 100) -> List[FundingRate]:
        if market_type == MarketType.SPOT:
            raise NotImplementedError(f"{self.name} does not support funding_rate for {market_type.value}")

        base_url = self._get_rest_url(market_type)
        endpoint = "/fapi/v1/fundingRate" if market_type == MarketType.LINEAR else "/dapi/v1/fundingRate"
        api_symbol = self.get_api_symbol(symbol, market_type)

        per_req = 1000
        default_cadence_ms = 8 * 3_600_000

        model_sym       = self.get_model_symbol(api_symbol, market_type)
        info            = await self._info_for(market_type, model_sym)
        quote           = info.quote_asset if info else ""
        row_interval_ms = await self._funding_interval_ms_for(market_type, model_sym)
        cycle_ms        = row_interval_ms if row_interval_ms is not None else default_cadence_ms

        async def call(params: dict) -> List[FundingRate]:
            data = await self._make_request("GET", f"{base_url}{endpoint}", params=params)
            return [FundingRate(
                symbol      = self.get_model_symbol(i["symbol"], market_type),
                market_type = market_type,
                quote       = quote,
                rate        = build_funding_historical(
                    kind      = "discrete",
                    per_cycle = float(i["fundingRate"]),
                    cycle_ms  = cycle_ms,
                ),
                timestamp   = self.normalize_timestamp(i["fundingTime"]),
            ) for i in data]

        if start_time is not None:
            params = {
                "symbol":    api_symbol,
                "limit":     min(limit, per_req),
                "startTime": max(0, start_time - min(limit, per_req) * cycle_ms),
                "endTime":   start_time,
            }
            return (await call(params))[-limit:]

        recent = await call({"symbol": api_symbol, "limit": min(limit, 200)})
        if limit <= len(recent):
            return recent[-limit:]

        collected = list(recent)
        seen = {r.timestamp for r in collected}

        if len(recent) >= 2:
            intervals = sorted(recent[i + 1].timestamp - recent[i].timestamp for i in range(len(recent) - 1))
            cadence_ms = max(intervals[len(intervals) // 2], 60_000)
        else:
            cadence_ms = default_cadence_ms

        safety_pages = 10
        while len(collected) < limit and safety_pages > 0:
            oldest_ts = min(r.timestamp for r in collected)
            params = {
                "symbol":    api_symbol,
                "limit":     per_req,
                "startTime": max(0, oldest_ts - per_req * cadence_ms),
                "endTime":   oldest_ts - 1,
            }
            batch = await call(params)
            new = [r for r in batch if r.timestamp not in seen]
            if not new:
                break
            for r in new:
                seen.add(r.timestamp)
            collected.extend(new)
            safety_pages -= 1

        collected.sort(key=lambda x: x.timestamp)
        return collected[-limit:]


    async def get_open_interest(self, market_type: MarketType, symbol: str, period: str = "1h", start_time: Optional[int] = None, limit: int = 30) -> List[OpenInterest]:
        if market_type == MarketType.SPOT:
            raise NotImplementedError(f"{self.name} does not support open_interest for {market_type.value}")

        base_url = self._get_rest_url(market_type)
        endpoint = "/futures/data/openInterestHist"

        raw_symbol = self.normalize_symbol(symbol).replace("_PERP", "")
        model_symbol = self.get_model_symbol(raw_symbol, market_type)

        sym_key = "symbol" if market_type == MarketType.LINEAR else "pair"

        info          = await self._info_for(market_type, model_symbol)
        quote         = info.quote_asset if info else ""
        contract_size = info.contract_size if info else None
        is_inverse    = market_type == MarketType.INVERSE
        oi_unit       = "contract" if is_inverse else "base"

        async def fetch_batch_backward(end_ts, l):
            anchor = end_ts if end_ts is not None else start_time
            p = {sym_key: raw_symbol, "period": period, "limit": l}
            if anchor:
                p["endTime"] = anchor
            if market_type == MarketType.INVERSE:
                p["contractType"] = "PERPETUAL"
            data = await self._make_request("GET", f"{base_url}{endpoint}", params=p)
            return [OpenInterest(
                symbol        = model_symbol,
                market_type   = market_type,
                quote         = quote,
                interval      = period,
                open_interest = build_oi_value(
                    native        = float(i["sumOpenInterest"]),
                    oi_unit       = oi_unit,
                    contract_size = contract_size,
                    candle_close  = None,
                    candle_close_ts = None,
                ),
                timestamp     = self.normalize_timestamp(i["timestamp"]),
            ) for i in data]

        return await self._paginate_backwards(fetch_batch_backward, limit, 500)


    async def get_liquidations(self, market_type: MarketType, symbol: str, start_time: Optional[int] = None, limit: int = 100) -> List[Liquidation]:
        if market_type == MarketType.SPOT:
            raise NotImplementedError(f"{self.name} does not support liquidations for {market_type.value}")
        raise NotImplementedError(f"{self.name} does not support liquidations REST for {market_type.value}; use stream_liquidations instead")


    async def get_long_short_ratio(self, market_type: MarketType, symbol: str, period: str = "5m", start_time: Optional[int] = None, limit: int = 30) -> List[LongShortRatio]:
        if market_type == MarketType.SPOT:
            raise NotImplementedError(f"{self.name} does not support long_short_ratio for {market_type.value}")

        base_url = self._get_rest_url(market_type)
        endpoint = "/futures/data/globalLongShortAccountRatio"

        raw_symbol = self.normalize_symbol(symbol).replace("_PERP", "")
        model_symbol = self.get_model_symbol(raw_symbol, market_type)

        sym_key = "symbol" if market_type == MarketType.LINEAR else "pair"

        async def fetch_batch_backward(end_ts, l):
            anchor = end_ts if end_ts is not None else start_time
            p = {"period": period, "limit": l}
            if anchor:
                p["endTime"] = anchor
            p[sym_key] = raw_symbol
            data = await self._make_request("GET", f"{base_url}{endpoint}", params=p)
            return [LongShortRatio(
                symbol        = model_symbol,
                market_type   = market_type,
                interval      = period,
                ratio         = float(i["longShortRatio"]),
                long_account  = float(i["longAccount"]),
                short_account = float(i["shortAccount"]),
                account_scope = "all_accounts",
                timestamp     = self.normalize_timestamp(i["timestamp"]),
            ) for i in data]

        return await self._paginate_backwards(fetch_batch_backward, limit, 500)


    async def _fetch_funding_overrides(self, market_type: MarketType) -> Dict[str, int]:
        if market_type == MarketType.SPOT:
            return {}
        base_url = self._get_rest_url(market_type)
        endpoint = "/fapi/v1/fundingInfo" if market_type == MarketType.LINEAR else "/dapi/v1/fundingInfo"
        try:
            data = await self._make_request("GET", f"{base_url}{endpoint}")
        except Exception as e:
            logger.warning(f"Binance fundingInfo fetch failed for {market_type}: {e}; falling back to default 8h cycle for every symbol")
            return {}
        out: Dict[str, int] = {}
        for item in data or []:
            try:
                hours = float(item["fundingIntervalHours"])
                out[item["symbol"]] = int(hours * 3_600_000)
            except (KeyError, TypeError, ValueError):
                continue
        return out


    async def _fetch_exchange_info(self, market_type: MarketType) -> List[SymbolInfo]:
        base_url = self._get_rest_url(market_type)
        endpoint = "/api/v3/exchangeInfo" if market_type == MarketType.SPOT else "/fapi/v1/exchangeInfo"
        if market_type == MarketType.INVERSE:
            endpoint = "/dapi/v1/exchangeInfo"

        data = await self._make_request("GET", f"{base_url}{endpoint}")
        funding_overrides = await self._fetch_funding_overrides(market_type)
        results = []

        self._funding_interval_cache.setdefault(market_type, {})

        for s in data["symbols"]:
            s_status = s.get("status") or s.get("contractStatus")
            if s_status != "TRADING":
                continue
            if market_type != MarketType.SPOT and s.get("contractType") not in ("PERPETUAL", "TRADIFI_PERPETUAL"):
                continue

            min_qty: Optional[float] = None
            max_qty: Optional[float] = None
            min_notional: Optional[float] = None
            tick_size = ""
            step_size = ""

            for f in s.get("filters", []):
                if f["filterType"] == "LOT_SIZE":
                    min_qty, max_qty = float(f["minQty"]), float(f["maxQty"])
                    step_size = f.get("stepSize", "") or step_size
                if f["filterType"] == "PRICE_FILTER":
                    tick_size = f.get("tickSize", "") or tick_size
                if f["filterType"] in ["MIN_NOTIONAL", "NOTIONAL"]:
                    raw_notional = f.get("minNotional") or f.get("notional")
                    if raw_notional is not None:
                        min_notional = float(raw_notional)

            def _decimals(s_val):
                if not s_val or "." not in s_val:
                    return 0
                return len(s_val.split(".")[1].rstrip("0"))

            if market_type == MarketType.SPOT:
                price_prec    = _decimals(tick_size) if tick_size else s.get("quotePrecision", 8)
                quantity_prec = _decimals(step_size) if step_size else s.get("baseAssetPrecision", 8)
            else:
                price_prec    = s.get("pricePrecision", 8)
                quantity_prec = s.get("quantityPrecision", 8)

            qty_unit = "contract" if market_type == MarketType.INVERSE else "base"
            contract_size: Optional[float] = None
            if market_type == MarketType.INVERSE:
                cs_raw = s.get("contractSize")
                if cs_raw is not None:
                    try:
                        contract_size = float(cs_raw)
                    except (TypeError, ValueError):
                        contract_size = None

            funding_interval_ms: Optional[int] = None
            funding_block = None
            if market_type != MarketType.SPOT:
                funding_interval_ms = funding_overrides.get(s["symbol"], 8 * 3_600_000)
                funding_block = build_funding_convention("discrete")

            model_sym = self.get_model_symbol(s["symbol"], market_type)
            if funding_interval_ms is not None:
                self._funding_interval_cache[market_type][model_sym] = funding_interval_ms

            results.append(SymbolInfo(
                symbol=model_sym,
                native_symbol=s["symbol"],
                base_asset=s["baseAsset"],
                quote_asset=s["quoteAsset"],
                price_precision=price_prec,
                quantity_precision=quantity_prec,
                min_qty=min_qty,
                max_qty=max_qty,
                min_notional=min_notional,
                qty_unit=qty_unit,
                contract_size=contract_size,
                funding=funding_block,
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
            raise ValueError(f"Symbol {symbol} not found on Binance {market_type.value}")
        return info


    async def stream_ticker(self, market_type: MarketType, symbol: str) -> AsyncGenerator[Ticker, None]:
        s_norm     = self.get_stream_symbol(symbol, market_type)
        is_inverse = market_type == MarketType.INVERSE
        unit       = "contract" if is_inverse else "base"

        model_sym_for_lookup = self.get_model_symbol(self.get_api_symbol(symbol, market_type), market_type)
        info                 = await self._info_for(market_type, model_sym_for_lookup)
        quote                = info.quote_asset if info else ""
        contract_size        = info.contract_size if info else None

        async for data in self._ws_connect(market_type, [f"{s_norm}@ticker"]):
            close_price = float(data["c"])
            yield Ticker(
                symbol               = self.get_model_symbol(data["s"], market_type),
                market_type          = market_type,
                quote                = quote,
                price                = close_price,
                open_24h             = float(data["o"]),
                high_24h             = float(data["h"]),
                low_24h              = float(data["l"]),
                volume_24h           = build_volume_value(
                    native        = float(data["v"]),
                    volume_unit   = unit,
                    contract_size = contract_size,
                    close         = close_price,
                ),
                price_change_percent = float(data["P"]),
                timestamp            = self.normalize_timestamp(data["E"]),
            )


    async def stream_book_ticker(self, market_type: MarketType, symbol: str) -> AsyncGenerator[BookTicker, None]:
        s_norm     = self.get_stream_symbol(symbol, market_type)
        is_inverse = market_type == MarketType.INVERSE
        unit       = "contract" if is_inverse else "base"

        model_sym_for_lookup = self.get_model_symbol(self.get_api_symbol(symbol, market_type), market_type)
        info                 = await self._info_for(market_type, model_sym_for_lookup)
        quote                = info.quote_asset if info else ""
        contract_size        = info.contract_size if info else None

        async for data in self._ws_connect(market_type, [f"{s_norm}@bookTicker"]):
            bid_price = float(data["b"])
            ask_price = float(data["a"])
            yield BookTicker(
                symbol      = self.get_model_symbol(data["s"], market_type),
                market_type = market_type,
                quote       = quote,
                bid_price   = bid_price,
                bid_qty     = build_qty_value(
                    native        = float(data["B"]),
                    qty_unit      = unit,
                    contract_size = contract_size,
                    price         = bid_price,
                ),
                ask_price   = ask_price,
                ask_qty     = build_qty_value(
                    native        = float(data["A"]),
                    qty_unit      = unit,
                    contract_size = contract_size,
                    price         = ask_price,
                ),
                timestamp   = self.normalize_timestamp(data.get("T") or data.get("E") or int(time.time() * 1000)),
            )


    async def stream_orderbook(self, market_type: MarketType, symbol: str, depth: int = 20, update_speed: str = "100ms") -> AsyncGenerator[OrderBook, None]:
        s_norm       = self.get_stream_symbol(symbol, market_type)
        model_sym    = self.get_model_symbol(self.get_api_symbol(symbol, market_type), market_type)
        target_depth = depth if depth in [5, 10, 20] else 20
        if market_type == MarketType.SPOT:
            topic = f"{s_norm}@depth{target_depth}" if update_speed == "1000ms" else f"{s_norm}@depth{target_depth}@100ms"
        else:
            topic = f"{s_norm}@depth{target_depth}@{update_speed}" if update_speed in ["250ms", "500ms"] else f"{s_norm}@depth{target_depth}@100ms"

        info  = await self._info_for(market_type, model_sym)
        quote = info.quote_asset if info else ""

        qty_unit = "contract" if market_type == MarketType.INVERSE else "base"
        async for data in self._ws_connect(market_type, [topic]):
            bids = data.get("bids") or data.get("b") or []
            asks = data.get("asks") or data.get("a") or []
            ts   = data.get("E") or data.get("T") or int(time.time() * 1000)
            yield OrderBook(
                symbol      = model_sym,
                market_type = market_type,
                quote       = quote,
                bids        = [[float(p), float(q)] for p, q in bids],
                asks        = [[float(p), float(q)] for p, q in asks],
                qty_unit    = qty_unit,
                timestamp   = self.normalize_timestamp(ts),
            )


    async def stream_trades(self, market_type: MarketType, symbol: str) -> AsyncGenerator[Trade, None]:
        s_norm     = self.get_stream_symbol(symbol, market_type)
        model_sym  = self.get_model_symbol(self.get_api_symbol(symbol, market_type), market_type)
        is_inverse = market_type == MarketType.INVERSE
        unit       = "contract" if is_inverse else "base"

        info          = await self._info_for(market_type, model_sym)
        quote         = info.quote_asset if info else ""
        contract_size = info.contract_size if info else None

        async for data in self._ws_connect(market_type, [f"{s_norm}@trade"]):
            price = float(data.get("p", 0) or 0)
            qty   = float(data.get("q", 0) or 0)
            if price <= 0 or qty <= 0:
                continue
            yield Trade(
                id          = str(data["t"]),
                symbol      = model_sym,
                market_type = market_type,
                quote       = quote,
                price       = price,
                qty         = build_qty_value(
                    native        = qty,
                    qty_unit      = unit,
                    contract_size = contract_size,
                    price         = price,
                ),
                side        = "sell" if data["m"] else "buy",
                timestamp   = self.normalize_timestamp(data["T"]),
            )


    async def stream_agg_trades(self, market_type: MarketType, symbol: str) -> AsyncGenerator[AggTrade, None]:
        s_norm     = self.get_stream_symbol(symbol, market_type)
        model_sym  = self.get_model_symbol(self.get_api_symbol(symbol, market_type), market_type)
        is_inverse = market_type == MarketType.INVERSE
        unit       = "contract" if is_inverse else "base"

        info          = await self._info_for(market_type, model_sym)
        quote         = info.quote_asset if info else ""
        contract_size = info.contract_size if info else None

        async for data in self._ws_connect(market_type, [f"{s_norm}@aggTrade"]):
            price = float(data["p"])
            yield AggTrade(
                agg_id         = str(data["a"]),
                symbol         = model_sym,
                market_type    = market_type,
                quote          = quote,
                price          = price,
                qty            = build_qty_value(
                    native        = float(data["q"]),
                    qty_unit      = unit,
                    contract_size = contract_size,
                    price         = price,
                ),
                first_trade_id = str(data["f"]),
                last_trade_id  = str(data["l"]),
                side           = "sell" if data["m"] else "buy",
                timestamp      = self.normalize_timestamp(data["T"]),
            )


    async def stream_mark_price(self, market_type: MarketType, symbol: str) -> AsyncGenerator[MarkPrice, None]:
        if market_type == MarketType.SPOT:
            raise NotImplementedError(f"{self.name} does not support stream_mark_price for {market_type.value}")
        s_norm    = self.get_stream_symbol(symbol, market_type)
        model_sym = self.get_model_symbol(self.get_api_symbol(symbol, market_type), market_type)

        info     = await self._info_for(market_type, model_sym)
        quote    = info.quote_asset if info else ""
        cycle_ms = await self._funding_interval_ms_for(market_type, model_sym)

        async for data in self._ws_connect(market_type, [f"{s_norm}@markPrice"]):
            ts          = self.normalize_timestamp(data["E"])
            next_ft_raw = data.get("T")
            next_ft     = self.normalize_timestamp(next_ft_raw) if next_ft_raw else None

            funding_block = None
            rate_raw      = data.get("r")
            if rate_raw is not None and cycle_ms is not None and next_ft is not None:
                funding_block = build_funding_current(
                    kind           = "discrete",
                    per_cycle      = float(rate_raw),
                    cycle_ms       = cycle_ms,
                    valid_until_ts = max(next_ft, ts + 1),
                )

            yield MarkPrice(
                symbol      = self.get_model_symbol(data["s"], market_type),
                market_type = market_type,
                quote       = quote,
                mark_price  = float(data["p"]),
                index_price = float(data["i"]),
                funding     = funding_block,
                timestamp   = ts,
            )


    async def stream_liquidations(self, market_type: MarketType, symbol: str) -> AsyncGenerator[Liquidation, None]:
        if market_type == MarketType.SPOT:
            raise NotImplementedError(f"{self.name} does not support stream_liquidations for {market_type.value}")
        s_norm     = self.get_stream_symbol(symbol, market_type)
        model_sym  = self.get_model_symbol(self.get_api_symbol(symbol, market_type), market_type)
        is_inverse = market_type == MarketType.INVERSE
        unit       = "contract" if is_inverse else "base"

        info          = await self._info_for(market_type, model_sym)
        quote         = info.quote_asset if info else ""
        contract_size = info.contract_size if info else None

        async for data in self._ws_connect(market_type, [f"{s_norm}@forceOrder"]):
            o = data.get("o", {})
            if not o:
                continue
            price = float(o["p"])
            yield Liquidation(
                symbol      = self.get_model_symbol(o["s"], market_type),
                market_type = market_type,
                quote       = quote,
                side        = o["S"].lower(),
                price       = price,
                qty         = build_qty_value(
                    native        = float(o["q"]),
                    qty_unit      = unit,
                    contract_size = contract_size,
                    price         = price,
                ),
                timestamp   = self.normalize_timestamp(data["E"]),
            )
