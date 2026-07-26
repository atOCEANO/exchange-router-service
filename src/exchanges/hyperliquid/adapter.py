import httpx
import asyncio
import random
import time
import websockets
import logging
from typing import List, AsyncGenerator, Dict, Any, Optional
from src.exchanges.base import (
    BaseExchange, StreamHub, UpstreamUnavailableError,
    build_qty_value, build_volume_value, build_oi_value,
    build_funding_current, build_funding_historical, build_funding_convention,
)
from src.models import (
    Ticker, BookTicker, MarkPrice, OrderBook, Candle, Trade, AggTrade,
    MarketType, SymbolInfo, OpenInterest, FundingRate,
)


logger = logging.getLogger("hyperliquid_adapter")


class HyperliquidAdapter(BaseExchange):
    _FUNDING_CYCLE_MS    = 3_600_000
    _MIN_ORDER_VALUE_USD = 10.0
    _TICKER_RESEED_S     = 120
    _MAX_SUBS_PER_HUB    = 900
    _MAX_HUBS            = 10


    def __init__(self):
        super().__init__()
        self.http_client = httpx.AsyncClient(timeout=30.0)

        self.info_url = "https://api.hyperliquid.xyz/info"
        self.ws_url   = "wss://api.hyperliquid.xyz/ws"

        self._backoff_until = 0.0
        self._backoff_lock  = asyncio.Lock()

        self._hubs: List[Dict[str, Any]] = []
        self._hubs_lock = asyncio.Lock()
        self._topic_args: Dict[str, Dict] = {}
        self._topic_hub: Dict[str, StreamHub] = {}
        self._topic_refs: Dict[str, int] = {}

        self._funding_interval_cache: Dict[MarketType, Dict[str, int]] = {}

        self._capabilities = self._build_capabilities()


    async def shutdown(self):
        for slot in list(self._hubs):
            await slot["hub"].close()
        self._hubs.clear()
        await self.http_client.aclose()


    async def _warm(self) -> None:
        await self._step("spot_info",   self._ensure_info_cache(MarketType.SPOT))
        await self._step("linear_info", self._ensure_info_cache(MarketType.LINEAR))


    @property
    def name(self) -> str:
        return "hyperliquid"


    @property
    def supported_market_types(self) -> List[MarketType]:
        return [MarketType.SPOT, MarketType.LINEAR]


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
                        "depths":    [20],
                        "max_depth": 20,
                    },
                    "trades": {
                        "rest":      True,
                        "ws":        True,
                        "max_limit": 10,
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
                        "intervals":    ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "8h", "12h", "1d", "3d", "1w", "1M"],
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
                        "depths":    [20],
                        "max_depth": 20,
                    },
                    "trades": {
                        "rest":      True,
                        "ws":        True,
                        "max_limit": 10,
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
                        "intervals":    ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "8h", "12h", "1d", "3d", "1w", "1M"],
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
                        "paginated":    False,
                        "max_limit":    1,
                        "retention_ms": None,
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


    def get_api_symbol(self, symbol: str, market_type: MarketType) -> str:
        s = self.normalize_symbol(symbol)
        if market_type == MarketType.LINEAR and s.endswith("USDC"):
            return s[:-4]
        return s


    def get_model_symbol(self, api_symbol: str, market_type: MarketType) -> str:
        s = api_symbol.upper()
        if market_type == MarketType.LINEAR and not s.endswith("USDC"):
            return f"{s}USDC"
        return s


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


    async def _make_request(self, info_type: str, body: Dict) -> Any:
        retries = 3
        while retries > 0:
            now = time.time()
            if now < self._backoff_until:
                wait_time = self._backoff_until - now
                if wait_time > 30:
                    raise UpstreamUnavailableError(f"Hyperliquid backoff in effect. Retry in {wait_time:.0f}s.", retry_after=wait_time)
                await asyncio.sleep(wait_time + random.uniform(0.05, 1.0))

            try:
                resp = await self.http_client.post(self.info_url, json=body)

                if resp.status_code == 200:
                    return resp.json()

                if resp.status_code == 429:
                    backoff = 5 * (4 - retries)
                    async with self._backoff_lock:
                        self._backoff_until = max(self._backoff_until, time.time() + backoff)
                    logger.error(f"Hyperliquid rate limit (429) on {info_type}. Backing off {backoff}s.")
                    retries -= 1
                    continue

                if resp.status_code == 422:
                    raise ValueError(f"Hyperliquid rejected {info_type} request (422)")

                resp.raise_for_status()

            except httpx.HTTPStatusError as e:
                if e.response.status_code in (502, 503, 504, 520):
                    logger.warning(f"Hyperliquid HTTP {e.response.status_code} on {info_type}. Retrying...")
                    retries -= 1
                    await asyncio.sleep(1)
                    continue
                if e.response.status_code >= 500:
                    raise UpstreamUnavailableError(f"Hyperliquid upstream error {e.response.status_code} on {info_type}")
                logger.error(f"HTTP Error: {e}")
                raise e
            except Exception as e:
                if isinstance(e, (ValueError, UpstreamUnavailableError)):
                    raise e
                logger.error(f"Request Error: {e}")
                retries -= 1
                await asyncio.sleep(1)

        remaining = self._backoff_until - time.time()
        raise UpstreamUnavailableError(f"Max retries exceeded for {info_type}", retry_after=remaining if remaining > 0 else None)


    async def _lookup(self, market_type: MarketType, symbol: str) -> Any:
        model_sym = self.get_model_symbol(self.get_api_symbol(symbol, market_type), market_type)
        info      = await self._info_for(market_type, model_sym)
        if info is None:
            raise ValueError(f"Symbol {symbol} not found on Hyperliquid {market_type.value}")
        return model_sym, info.native_symbol, info.quote_asset


    async def _asset_ctx(self, market_type: MarketType, coin: str) -> Dict[str, Any]:
        if market_type == MarketType.SPOT:
            data = await self._make_request("spotMetaAndAssetCtxs", {"type": "spotMetaAndAssetCtxs"})
            for ctx in data[1]:
                if ctx.get("coin") == coin:
                    return ctx
            raise ValueError(f"Coin {coin} not found on Hyperliquid spot")

        data     = await self._make_request("metaAndAssetCtxs", {"type": "metaAndAssetCtxs"})
        universe = data[0]["universe"]
        ctxs     = data[1]
        for u, ctx in zip(universe, ctxs):
            if u["name"] == coin:
                return ctx
        raise ValueError(f"Coin {coin} not found on Hyperliquid linear")


    async def _high_low_24h(self, coin: str) -> Any:
        now  = int(time.time() * 1000)
        body = {"type": "candleSnapshot", "req": {"coin": coin, "interval": "1h", "startTime": now - 86_400_000, "endTime": now}}
        rows = await self._make_request("candleSnapshot", body)
        if not rows:
            return None, None
        high = max(float(r["h"]) for r in rows)
        low  = min(float(r["l"]) for r in rows)
        return high, low


    async def _funding_interval_ms_for(self, market_type: MarketType, model_symbol: str) -> Optional[int]:
        await self._ensure_info_cache(market_type)
        return self._funding_interval_cache.get(market_type, {}).get(model_symbol)


    def _sub_payload(self, topics: List[str]) -> List[Dict]:
        out = []
        for t in topics:
            arg = self._topic_args.get(t)
            if arg is not None:
                out.append({"method": "subscribe", "subscription": arg})
        return out


    def _unsub_payload(self, topics: List[str]) -> List[Dict]:
        out = []
        for t in topics:
            arg = self._topic_args.get(t)
            if arg is not None:
                out.append({"method": "unsubscribe", "subscription": arg})
        return out


    def _route(self, msg: Any) -> Optional[str]:
        if not isinstance(msg, dict):
            return None
        channel = msg.get("channel")
        if channel in ("subscriptionResponse", "pong", None):
            return None
        if channel == "activeSpotAssetCtx":
            channel = "activeAssetCtx"
        data = msg.get("data")
        if channel == "trades":
            if isinstance(data, list) and data:
                coin = data[0].get("coin")
                return f"trades:{coin}" if coin else None
            return None
        if channel == "candle":
            if isinstance(data, dict):
                return f"candle:{data.get('s')}:{data.get('i')}"
            return None
        if isinstance(data, dict):
            coin = data.get("coin")
            return f"{channel}:{coin}" if coin else None
        return None


    def _make_hub(self, index: int) -> StreamHub:
        async def _connect():
            return await websockets.connect(self.ws_url, ping_interval=20, ping_timeout=20)
        return StreamHub(
            name=f"hyperliquid_{index}",
            connect=_connect,
            subscribe_payload=self._sub_payload,
            unsubscribe_payload=self._unsub_payload,
            route=self._route,
            keepalive_payload={"method": "ping"},
            keepalive_interval=50.0,
        )


    async def _acquire_topic(self, topic: str, sub_arg: Dict) -> StreamHub:
        async with self._hubs_lock:
            hub = self._topic_hub.get(topic)
            if hub is None:
                slot = next((s for s in self._hubs if len(s["topics"]) < self._MAX_SUBS_PER_HUB), None)
                if slot is None:
                    if len(self._hubs) >= self._MAX_HUBS:
                        raise UpstreamUnavailableError("Hyperliquid WS subscription capacity reached")
                    slot = {"hub": self._make_hub(len(self._hubs)), "topics": set()}
                    self._hubs.append(slot)
                slot["topics"].add(topic)
                self._topic_hub[topic] = slot["hub"]
                hub = slot["hub"]
            self._topic_args[topic] = sub_arg
            self._topic_refs[topic] = self._topic_refs.get(topic, 0) + 1
            return hub


    async def _release_topic(self, topic: str, q: Any, hub: StreamHub) -> None:
        await hub.unsubscribe(topic, q)
        async with self._hubs_lock:
            self._topic_refs[topic] = self._topic_refs.get(topic, 1) - 1
            if self._topic_refs[topic] <= 0:
                self._topic_refs.pop(topic, None)
                self._topic_args.pop(topic, None)
                assigned = self._topic_hub.pop(topic, None)
                for slot in self._hubs:
                    if slot["hub"] is assigned:
                        slot["topics"].discard(topic)
                        break


    async def _ws_connect(self, topic: str, sub_arg: Dict) -> AsyncGenerator[Dict, None]:
        hub = await self._acquire_topic(topic, sub_arg)
        q   = await hub.subscribe(topic)
        try:
            while True:
                msg = await q.get()
                yield msg
        finally:
            await self._release_topic(topic, q, hub)


    async def get_ticker(self, market_type: MarketType, symbol: str) -> Ticker:
        model_sym, coin, quote = await self._lookup(market_type, symbol)

        ctx       = await self._asset_ctx(market_type, coin)
        high, low = await self._high_low_24h(coin)
        px        = ctx.get("midPx") or ctx.get("markPx")
        if px is None:
            raise ValueError(f"Hyperliquid returned no price for {coin}")
        close = float(px)
        prev  = float(ctx.get("prevDayPx") or 0)

        return Ticker(
            symbol               = model_sym,
            market_type          = market_type,
            quote                = quote,
            price                = close,
            open_24h             = prev,
            high_24h             = high if high is not None else close,
            low_24h              = low if low is not None else close,
            volume_24h           = build_volume_value(
                native        = float(ctx.get("dayBaseVlm") or 0),
                volume_unit   = "base",
                contract_size = None,
                close         = close,
            ),
            price_change_percent = (close - prev) / prev * 100 if prev > 0 else 0.0,
            timestamp            = int(time.time() * 1000),
        )


    async def get_book_ticker(self, market_type: MarketType, symbol: str) -> BookTicker:
        model_sym, coin, quote = await self._lookup(market_type, symbol)

        data   = await self._make_request("l2Book", {"type": "l2Book", "coin": coin})
        levels = data.get("levels") or []
        if len(levels) < 2 or not levels[0] or not levels[1]:
            raise ValueError(f"Empty order book for {symbol} on Hyperliquid {market_type.value}")
        bids = levels[0]
        asks = levels[1]

        bid       = bids[0]
        ask       = asks[0]
        bid_price = float(bid["px"])
        ask_price = float(ask["px"])

        return BookTicker(
            symbol      = model_sym,
            market_type = market_type,
            quote       = quote,
            bid_price   = bid_price,
            bid_qty     = build_qty_value(
                native        = float(bid["sz"]),
                qty_unit      = "base",
                contract_size = None,
                price         = bid_price,
            ),
            ask_price   = ask_price,
            ask_qty     = build_qty_value(
                native        = float(ask["sz"]),
                qty_unit      = "base",
                contract_size = None,
                price         = ask_price,
            ),
            timestamp   = self.normalize_timestamp(data["time"]),
        )


    async def get_mark_price(self, market_type: MarketType, symbol: str) -> MarkPrice:
        if market_type != MarketType.LINEAR:
            raise NotImplementedError(f"{self.name} does not support mark_price for {market_type.value}")
        model_sym, coin, quote = await self._lookup(market_type, symbol)

        ctx      = await self._asset_ctx(market_type, coin)
        cycle_ms = await self._funding_interval_ms_for(market_type, model_sym)
        cycle_ms = cycle_ms if cycle_ms is not None else self._FUNDING_CYCLE_MS

        mark = ctx.get("markPx")
        if mark is None:
            raise ValueError(f"Hyperliquid returned no mark price for {coin}")
        oracle = ctx.get("oraclePx")
        rate   = ctx.get("funding")

        ts        = int(time.time() * 1000)
        next_hour = ((ts // cycle_ms) + 1) * cycle_ms

        funding = None
        if rate is not None:
            funding = build_funding_current(
                kind           = "discrete",
                per_cycle      = float(rate),
                cycle_ms       = cycle_ms,
                valid_until_ts = max(next_hour, ts + 1),
            )

        return MarkPrice(
            symbol      = model_sym,
            market_type = market_type,
            quote       = quote,
            mark_price  = float(mark),
            index_price = float(oracle) if oracle is not None else None,
            funding     = funding,
            timestamp   = ts,
        )


    async def get_orderbook(self, market_type: MarketType, symbol: str, depth: int = 20) -> OrderBook:
        model_sym, coin, quote = await self._lookup(market_type, symbol)

        data   = await self._make_request("l2Book", {"type": "l2Book", "coin": coin})
        levels = data.get("levels") or [[], []]
        if len(levels) < 2:
            raise ValueError(f"Malformed order book for {symbol} on Hyperliquid {market_type.value}")

        return OrderBook(
            symbol      = model_sym,
            market_type = market_type,
            quote       = quote,
            bids        = [[float(l["px"]), float(l["sz"])] for l in levels[0][:depth]],
            asks        = [[float(l["px"]), float(l["sz"])] for l in levels[1][:depth]],
            qty_unit    = "base",
            timestamp   = self.normalize_timestamp(data["time"]),
        )


    async def get_trades(self, market_type: MarketType, symbol: str, limit: int = 100) -> List[Trade]:
        model_sym, coin, quote = await self._lookup(market_type, symbol)

        max_limit = self.get_capabilities()["markets"][market_type]["trades"]["max_limit"]
        req_limit = min(limit, max_limit)

        data = await self._make_request("recentTrades", {"type": "recentTrades", "coin": coin})

        trades = []
        for t in data:
            price = float(t["px"])
            trades.append(Trade(
                id          = str(t["tid"]),
                symbol      = model_sym,
                market_type = market_type,
                quote       = quote,
                price       = price,
                qty         = build_qty_value(
                    native        = float(t["sz"]),
                    qty_unit      = "base",
                    contract_size = None,
                    price         = price,
                ),
                side        = "buy" if t.get("side") == "B" else "sell",
                timestamp   = self.normalize_timestamp(t["time"]),
            ))
        trades.sort(key=lambda t: t.timestamp)

        return trades[-req_limit:]


    async def get_agg_trades(self, market_type: MarketType, symbol: str, start_time: Optional[int] = None, limit: int = 500) -> List[AggTrade]:
        raise NotImplementedError(f"{self.name} does not support agg_trades for {market_type.value}")


    async def get_candles(self, market_type: MarketType, symbol: str, interval: str, start_time: Optional[int] = None, limit: int = 100) -> List[Candle]:
        model_sym, coin, quote = await self._lookup(market_type, symbol)

        interval_ms = self._interval_to_ms(interval)
        if interval_ms <= 0:
            raise ValueError(f"Unsupported candle interval {interval} for Hyperliquid")
        max_per_req = 5000

        async def fetch_by_end(end_ts, l):
            now    = int(time.time() * 1000)
            anchor = end_ts if end_ts is not None else start_time
            end    = min(anchor, now) if anchor is not None else now
            count  = min(l, max_per_req)
            start  = max(end - count * interval_ms, 0)
            body   = {"type": "candleSnapshot",
                      "req": {"coin": coin, "interval": interval, "startTime": start, "endTime": end}}
            rows = await self._make_request("candleSnapshot", body)
            if not rows:
                return []

            parsed = []
            for k in rows:
                close = float(k["c"])
                parsed.append(Candle(
                    symbol      = model_sym,
                    market_type = market_type,
                    quote       = quote,
                    interval    = interval,
                    timestamp   = self.normalize_timestamp(k["t"]),
                    open        = float(k["o"]),
                    high        = float(k["h"]),
                    low         = float(k["l"]),
                    close       = close,
                    volume      = build_volume_value(
                        native        = float(k["v"]),
                        volume_unit   = "base",
                        contract_size = None,
                        close         = close,
                    ),
                ))

            return sorted(parsed, key=lambda x: x.timestamp)

        if start_time is not None or limit > max_per_req:
            return await self._paginate_backwards(fetch_by_end, limit, max_per_req)
        candles = await fetch_by_end(None, limit)
        return candles[-limit:]


    async def get_funding_rate(self, market_type: MarketType, symbol: str, start_time: Optional[int] = None, limit: int = 100) -> List[FundingRate]:
        if market_type != MarketType.LINEAR:
            raise NotImplementedError(f"{self.name} does not support funding_rate for {market_type.value}")
        model_sym, coin, quote = await self._lookup(market_type, symbol)

        cycle_ms = await self._funding_interval_ms_for(market_type, model_sym)
        cycle_ms = cycle_ms if cycle_ms is not None else self._FUNDING_CYCLE_MS

        max_per_req = 500

        async def fetch_backwards(end_ts, l):
            anchor = end_ts if end_ts is not None else start_time
            end    = anchor if anchor is not None else int(time.time() * 1000)
            count  = min(l, max_per_req)
            start  = end - count * cycle_ms
            body   = {"type": "fundingHistory", "coin": coin, "startTime": start, "endTime": end}
            rows = await self._make_request("fundingHistory", body)

            parsed = [FundingRate(
                symbol      = model_sym,
                market_type = market_type,
                quote       = quote,
                rate        = build_funding_historical(
                    kind      = "discrete",
                    per_cycle = float(r["fundingRate"]),
                    cycle_ms  = cycle_ms,
                ),
                timestamp   = self.normalize_timestamp(r["time"]),
            ) for r in rows]
            return sorted(parsed, key=lambda x: x.timestamp)

        return await self._paginate_backwards(fetch_backwards, limit, max_per_req)


    async def get_open_interest(self, market_type: MarketType, symbol: str, period: str = "1h", start_time: Optional[int] = None, limit: int = 30) -> List[OpenInterest]:
        if market_type != MarketType.LINEAR:
            raise NotImplementedError(f"{self.name} does not support open_interest for {market_type.value}")
        model_sym, coin, quote = await self._lookup(market_type, symbol)

        ctx = await self._asset_ctx(market_type, coin)
        oi  = ctx.get("openInterest")
        if oi is None:
            raise ValueError(f"Hyperliquid returned no open interest for {coin}")

        now_ms    = int(time.time() * 1000)
        period_ms = self._interval_to_ms(period)
        row_ts    = (now_ms // period_ms) * period_ms if period_ms > 0 else now_ms

        return [OpenInterest(
            symbol        = model_sym,
            market_type   = market_type,
            quote         = quote,
            interval      = period,
            open_interest = build_oi_value(
                native          = float(oi),
                oi_unit         = "base",
                contract_size   = None,
                candle_close    = None,
                candle_close_ts = None,
            ),
            timestamp     = row_ts,
        )]


    async def _fetch_exchange_info(self, market_type: MarketType) -> List[SymbolInfo]:
        if market_type == MarketType.SPOT:
            return await self._fetch_spot_info()
        if market_type != MarketType.LINEAR:
            return []

        data     = await self._make_request("metaAndAssetCtxs", {"type": "metaAndAssetCtxs"})
        universe = data[0]["universe"]

        self._funding_interval_cache.setdefault(market_type, {})

        results = []
        for u in universe:
            if u.get("isDelisted"):
                continue
            coin      = u["name"]
            sz_dec    = int(u["szDecimals"])
            model_sym = self.get_model_symbol(coin, market_type)

            self._funding_interval_cache[market_type][model_sym] = self._FUNDING_CYCLE_MS

            results.append(SymbolInfo(
                symbol             = model_sym,
                native_symbol      = coin,
                base_asset         = coin,
                quote_asset        = "USDC",
                price_precision    = max(6 - sz_dec, 0),
                quantity_precision = sz_dec,
                min_qty            = None,
                max_qty            = None,
                min_notional       = self._MIN_ORDER_VALUE_USD,
                qty_unit           = "base",
                contract_size      = None,
                funding            = build_funding_convention("discrete"),
            ))
        return results


    async def _fetch_spot_info(self) -> List[SymbolInfo]:
        data  = await self._make_request("spotMeta", {"type": "spotMeta"})
        names = {t["index"]: t["name"] for t in data["tokens"]}
        szdec = {t["index"]: int(t["szDecimals"]) for t in data["tokens"]}

        results = []
        for pair in data["universe"]:
            base_idx  = pair["tokens"][0]
            quote_idx = pair["tokens"][1]
            base      = names.get(base_idx)
            quote     = names.get(quote_idx)
            if base is None or quote is None:
                continue
            base_sz = szdec.get(base_idx, 0)

            results.append(SymbolInfo(
                symbol             = f"{base}{quote}".upper(),
                native_symbol      = pair["name"],
                base_asset         = base,
                quote_asset        = quote,
                price_precision    = max(8 - base_sz, 0),
                quantity_precision = base_sz,
                min_qty            = None,
                max_qty            = None,
                min_notional       = self._MIN_ORDER_VALUE_USD,
                qty_unit           = "base",
                contract_size      = None,
                funding            = None,
            ))
        return results


    async def get_exchange_info(self, market_type: MarketType) -> List[SymbolInfo]:
        cache = await self._ensure_info_cache(market_type)
        return list(cache.values())


    async def get_symbol_info(self, market_type: MarketType, symbol: str) -> SymbolInfo:
        cache     = await self._ensure_info_cache(market_type)
        model_sym = self.get_model_symbol(self.get_api_symbol(symbol, market_type), market_type)
        info      = cache.get(model_sym)
        if info is None:
            raise ValueError(f"Symbol {symbol} not found on Hyperliquid {market_type.value}")
        return info


    async def stream_ticker(self, market_type: MarketType, symbol: str) -> AsyncGenerator[Ticker, None]:
        model_sym, coin, quote = await self._lookup(market_type, symbol)

        high, low = await self._high_low_24h(coin)
        reseed_at = time.time() + self._TICKER_RESEED_S
        topic     = f"activeAssetCtx:{coin}"
        sub_arg   = {"type": "activeAssetCtx", "coin": coin}

        async for msg in self._ws_connect(topic, sub_arg):
            try:
                ctx   = (msg.get("data") or {}).get("ctx") or {}
                close = float(ctx.get("midPx") or ctx.get("markPx") or 0)
                if close <= 0:
                    continue
                if high is None or close > high:
                    high = close
                if low is None or close < low:
                    low = close
                if time.time() >= reseed_at:
                    reseed_at = time.time() + self._TICKER_RESEED_S
                    h2, l2    = await self._high_low_24h(coin)
                    if h2 is not None:
                        high, low = h2, l2
                prev = float(ctx.get("prevDayPx") or 0)
                yield Ticker(
                    symbol               = model_sym,
                    market_type          = market_type,
                    quote                = quote,
                    price                = close,
                    open_24h             = prev,
                    high_24h             = high,
                    low_24h              = low,
                    volume_24h           = build_volume_value(
                        native        = float(ctx.get("dayBaseVlm") or 0),
                        volume_unit   = "base",
                        contract_size = None,
                        close         = close,
                    ),
                    price_change_percent = (close - prev) / prev * 100 if prev > 0 else 0.0,
                    timestamp            = int(time.time() * 1000),
                )
            except (ValueError, TypeError, KeyError, IndexError, AttributeError) as e:
                logger.warning(f"Hyperliquid ticker {coin} malformed frame skipped: {e!r}")


    async def stream_book_ticker(self, market_type: MarketType, symbol: str) -> AsyncGenerator[BookTicker, None]:
        model_sym, coin, quote = await self._lookup(market_type, symbol)

        topic   = f"bbo:{coin}"
        sub_arg = {"type": "bbo", "coin": coin}

        async for msg in self._ws_connect(topic, sub_arg):
            try:
                d   = msg.get("data") or {}
                bbo = d.get("bbo") or []
                if len(bbo) < 2 or bbo[0] is None or bbo[1] is None:
                    continue
                bid       = bbo[0]
                ask       = bbo[1]
                bid_price = float(bid["px"])
                ask_price = float(ask["px"])
                yield BookTicker(
                    symbol      = model_sym,
                    market_type = market_type,
                    quote       = quote,
                    bid_price   = bid_price,
                    bid_qty     = build_qty_value(
                        native        = float(bid["sz"]),
                        qty_unit      = "base",
                        contract_size = None,
                        price         = bid_price,
                    ),
                    ask_price   = ask_price,
                    ask_qty     = build_qty_value(
                        native        = float(ask["sz"]),
                        qty_unit      = "base",
                        contract_size = None,
                        price         = ask_price,
                    ),
                    timestamp   = self.normalize_timestamp(d.get("time") or int(time.time() * 1000)),
                )
            except (ValueError, TypeError, KeyError, IndexError, AttributeError) as e:
                logger.warning(f"Hyperliquid book_ticker {coin} malformed frame skipped: {e!r}")


    async def stream_mark_price(self, market_type: MarketType, symbol: str) -> AsyncGenerator[MarkPrice, None]:
        if market_type != MarketType.LINEAR:
            raise NotImplementedError(f"{self.name} does not support mark_price for {market_type.value}")
        model_sym, coin, quote = await self._lookup(market_type, symbol)

        topic   = f"activeAssetCtx:{coin}"
        sub_arg = {"type": "activeAssetCtx", "coin": coin}

        async for msg in self._ws_connect(topic, sub_arg):
            try:
                ctx       = (msg.get("data") or {}).get("ctx") or {}
                ts        = int(time.time() * 1000)
                next_hour = ((ts // self._FUNDING_CYCLE_MS) + 1) * self._FUNDING_CYCLE_MS
                yield MarkPrice(
                    symbol      = model_sym,
                    market_type = market_type,
                    quote       = quote,
                    mark_price  = float(ctx["markPx"]),
                    index_price = float(ctx["oraclePx"]),
                    funding     = build_funding_current(
                        kind           = "discrete",
                        per_cycle      = float(ctx["funding"]),
                        cycle_ms       = self._FUNDING_CYCLE_MS,
                        valid_until_ts = max(next_hour, ts + 1),
                    ),
                    timestamp   = ts,
                )
            except (ValueError, TypeError, KeyError, IndexError, AttributeError) as e:
                logger.warning(f"Hyperliquid mark_price {coin} malformed frame skipped: {e!r}")


    async def stream_trades(self, market_type: MarketType, symbol: str) -> AsyncGenerator[Trade, None]:
        model_sym, coin, quote = await self._lookup(market_type, symbol)

        topic   = f"trades:{coin}"
        sub_arg = {"type": "trades", "coin": coin}

        async for msg in self._ws_connect(topic, sub_arg):
            try:
                for t in msg.get("data", []):
                    price = float(t["px"])
                    yield Trade(
                        id          = str(t["tid"]),
                        symbol      = model_sym,
                        market_type = market_type,
                        quote       = quote,
                        price       = price,
                        qty         = build_qty_value(
                            native        = float(t["sz"]),
                            qty_unit      = "base",
                            contract_size = None,
                            price         = price,
                        ),
                        side        = "buy" if t.get("side") == "B" else "sell",
                        timestamp   = self.normalize_timestamp(t["time"]),
                    )
            except (ValueError, TypeError, KeyError, IndexError, AttributeError) as e:
                logger.warning(f"Hyperliquid trades {coin} malformed frame skipped: {e!r}")


    async def stream_orderbook(self, market_type: MarketType, symbol: str, depth: int = 20, update_speed: str = "100ms") -> AsyncGenerator[OrderBook, None]:
        model_sym, coin, quote = await self._lookup(market_type, symbol)

        topic   = f"l2Book:{coin}"
        sub_arg = {"type": "l2Book", "coin": coin}

        async for msg in self._ws_connect(topic, sub_arg):
            try:
                d      = msg.get("data") or {}
                levels = d.get("levels") or [[], []]
                yield OrderBook(
                    symbol      = model_sym,
                    market_type = market_type,
                    quote       = quote,
                    bids        = [[float(l["px"]), float(l["sz"])] for l in levels[0][:depth]],
                    asks        = [[float(l["px"]), float(l["sz"])] for l in levels[1][:depth]],
                    qty_unit    = "base",
                    timestamp   = self.normalize_timestamp(d.get("time") or int(time.time() * 1000)),
                )
            except (ValueError, TypeError, KeyError, IndexError, AttributeError) as e:
                logger.warning(f"Hyperliquid orderbook {coin} parse error: {e!r}; ending stream so clients resync")
                return
