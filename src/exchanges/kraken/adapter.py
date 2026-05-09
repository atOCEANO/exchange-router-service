import httpx
import orjson
import asyncio
import random
import time
import websockets
import logging
from typing import List, AsyncGenerator, Dict, Any, Optional, Tuple
from src.exchanges.base import BaseExchange, StreamHub
from src.models import (
    Ticker, BookTicker, MarkPrice, OrderBook, Candle, Trade, AggTrade,
    MarketType, SymbolInfo, OpenInterest, FundingRate, Liquidation, LongShortRatio
)


logger = logging.getLogger("kraken_adapter")


class KrakenAdapter(BaseExchange):
    def __init__(self):
        super().__init__()
        self.http_client = httpx.AsyncClient(timeout=30.0)

        self._backoff_until            = 0.0
        self._slowdown_until           = 0.0
        self._base_interval_ms         = 200.0
        self._pair_interval_ms         = 1100.0
        self._extra_interval_ms        = 0.0
        self._last_request_at          = 0.0
        self._last_pair_call_at: Dict[Tuple[str, str], float] = {}
        self._rate_lock                = asyncio.Lock()

        self.spot_rest_url = "https://api.kraken.com/0/public"
        self.spot_ws_url = "wss://ws.kraken.com/v2"
        self.futures_rest_url = "https://futures.kraken.com/derivatives/api/v3"
        self.futures_chart_url = "https://futures.kraken.com/api/charts/v1"
        self.futures_ws_url = "wss://futures.kraken.com/ws/v1"

        self._spot_ws_map: Dict[str, str] = {}
        self._spot_ws_map_lock = asyncio.Lock()

        self._hubs: Dict[str, StreamHub] = {}
        self._hubs_lock = asyncio.Lock()
        self._topic_payloads: Dict[str, Dict[str, Any]] = {}

        self._capabilities = self._build_capabilities()


    async def shutdown(self):
        for hub in list(self._hubs.values()):
            await hub.close()
        self._hubs.clear()
        await self.http_client.aclose()


    @property
    def name(self) -> str:
        return "kraken"


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
                        "intervals":    ["1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w"],
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
                        "max_depth": 500,
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
                        "intervals":    ["1m", "5m", "15m", "30m", "1h", "4h", "12h", "1d", "1w"],
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
                        "retention_ms": None,
                        "intervals":    ["1m", "5m", "15m", "30m", "1h", "4h", "12h", "1d"],
                    },
                    "liquidations": {
                        "rest":         False,
                        "ws":           False,
                        "paginated":    False,
                        "max_limit":    None,
                        "retention_ms": None,
                    },
                    "long_short_ratio": {
                        "rest":         True,
                        "ws":           False,
                        "paginated":    True,
                        "max_limit":    None,
                        "retention_ms": None,
                        "intervals":    ["1m", "5m", "15m", "30m", "1h", "4h", "12h", "1d"],
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
                        "max_depth": 500,
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
                        "intervals":    ["1m", "5m", "15m", "30m", "1h", "4h", "12h", "1d", "1w"],
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
                        "retention_ms": None,
                        "intervals":    ["1m", "5m", "15m", "30m", "1h", "4h", "12h", "1d"],
                    },
                    "liquidations": {
                        "rest":         False,
                        "ws":           False,
                        "paginated":    False,
                        "max_limit":    None,
                        "retention_ms": None,
                    },
                    "long_short_ratio": {
                        "rest":         True,
                        "ws":           False,
                        "paginated":    True,
                        "max_limit":    None,
                        "retention_ms": None,
                        "intervals":    ["1m", "5m", "15m", "30m", "1h", "4h", "12h", "1d"],
                    },
                },
            },
        }


    def normalize_symbol(self, symbol: str) -> str:
        s = symbol.upper().replace("/", "").replace("-", "")
        if s.startswith("PF_"): s = s[3:]
        if s.startswith("PI_"): s = s[3:]
        if s.startswith("FF_"): s = s[3:]
        if s.startswith("FI_"): s = s[3:]
        if s.endswith("_PERP"): s = s[:-5]
        return s


    def get_api_symbol(self, symbol: str, market_type: MarketType) -> str:
        s = self.normalize_symbol(symbol)
        if market_type == MarketType.INVERSE:
            return f"PI_{s}"
        if market_type == MarketType.LINEAR:
            return f"PF_{s}"
        return s


    def get_model_symbol(self, api_symbol: str, market_type: MarketType) -> str:
        s = api_symbol.upper().replace("/", "").replace("-", "")
        if s.startswith("PF_"): s = s[3:]
        elif s.startswith("PI_"): s = s[3:]
        elif s.startswith("FF_"): s = s[3:]
        elif s.startswith("FI_"): s = s[3:]
        if s.endswith("_PERP"): s = s[:-5]
        return s


    @staticmethod
    def _to_v2_wsname(wsname: str) -> str:
        return wsname.replace("XBT", "BTC").replace("XDG", "DOGE")


    def _heuristic_ws_symbol(self, api_symbol: str) -> str:
        if "/" in api_symbol:
            return self._to_v2_wsname(api_symbol)
        s = self._to_v2_wsname(api_symbol.upper())
        for q in ["USDT", "USDC", "USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF", "BTC", "ETH", "DOT"]:
            if s.endswith(q) and len(s) > len(q):
                return f"{s[:-len(q)]}/{q}"
        return s


    async def _ensure_spot_ws_map(self) -> None:
        if self._spot_ws_map:
            return
        async with self._spot_ws_map_lock:
            if self._spot_ws_map:
                return
            data = await self._make_request("GET", f"{self.spot_rest_url}/AssetPairs")
            built: Dict[str, str] = {}
            for k, v in data.items():
                if "." in k:
                    continue
                wsname = v.get("wsname")
                if not wsname:
                    continue
                ws_v2 = self._to_v2_wsname(wsname)
                altname = (v.get("altname") or k).upper()
                built[altname] = ws_v2
                built[self._to_v2_wsname(altname)] = ws_v2
                built[wsname.replace("/", "").upper()] = ws_v2
                built[ws_v2.replace("/", "").upper()] = ws_v2
            self._spot_ws_map = built


    async def _spot_ws_symbol(self, api_symbol: str) -> str:
        if "/" in api_symbol:
            return api_symbol
        try:
            await self._ensure_spot_ws_map()
        except Exception as e:
            logger.warning(f"Could not load Kraken AssetPairs for WS map: {e}. Falling back to heuristic.")
            return self._heuristic_ws_symbol(api_symbol)
        mapped = self._spot_ws_map.get(api_symbol.upper())
        if mapped:
            return mapped
        logger.warning(f"No wsname mapping for {api_symbol}; using heuristic.")
        return self._heuristic_ws_symbol(api_symbol)


    def _map_spot_interval(self, interval: str) -> int:
        mapping = {"1m": 1, "5m": 5, "15m": 15, "30m": 30, "1h": 60, "4h": 240, "1d": 1440, "1w": 10080}
        return mapping.get(interval, 60)


    def _map_analytics_interval(self, interval: str) -> int:
        mapping = {"1m": 60, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600, "4h": 14400, "12h": 43200, "1d": 86400}
        return mapping.get(interval, 3600)


    @staticmethod
    def _decimal_places(value: float) -> int:
        if not value or value <= 0:
            return 8
        s = f"{value:.10f}".rstrip("0")
        return len(s.split(".")[1]) if "." in s else 0


    def _extract_analytics(self, data: Any, field_name: str) -> List[Dict]:
        if not isinstance(data, dict):
            logger.warning(f"Kraken analytics: expected dict, got {type(data).__name__}. Raw: {str(data)[:500]}")
            return []

        candles = data.get("candles")
        if isinstance(candles, list) and candles:
            results = []
            for c in candles:
                if isinstance(c, dict) and field_name in c and "time" in c:
                    results.append({"time": c["time"] * 1000, "value": c[field_name]})
            if results:
                return results

        res = data.get("result")
        if isinstance(res, dict):
            timestamps = res.get("timestamp")
            payload = res.get("data")
            if isinstance(timestamps, list) and isinstance(payload, list):
                results = []
                for t, row in zip(timestamps, payload):
                    if isinstance(row, (list, tuple)) and row:
                        val = row[-1]
                    else:
                        val = row
                    if val is not None:
                        results.append({"time": t, "value": float(val)})
                return results
            if isinstance(timestamps, list) and isinstance(payload, dict):
                values = payload.get(field_name)
                if isinstance(values, list):
                    return [{"time": t, "value": v} for t, v in zip(timestamps, values) if v is not None]

        logger.warning(
            f"Kraken analytics: unrecognised format for '{field_name}'. "
            f"Top-level keys: {list(data.keys())}. Raw: {str(data)[:500]}"
        )
        return []


    def _pair_endpoint(self, url: str) -> Optional[str]:
        if url.startswith(self.spot_rest_url) and (url.endswith("/OHLC") or url.endswith("/Trades")):
            return url.rsplit("/", 1)[1]
        return None


    @staticmethod
    def _parse_throttle_until(err_str: str) -> Optional[float]:
        if "EService:Throttled:" not in err_str:
            return None
        try:
            tail = err_str.split("EService:Throttled:", 1)[1]
            tok = tail.strip().rstrip("]\"'").split()[0].rstrip("]\"',")
            return float(tok)
        except (ValueError, IndexError):
            return None


    async def _await_throttle(self, url: str, params: Optional[Dict]) -> None:
        async with self._rate_lock:
            now = time.time()
            delays = []

            if now < self._backoff_until:
                delays.append(self._backoff_until - now)

            global_interval_ms = self._base_interval_ms
            if now < self._slowdown_until:
                global_interval_ms += self._extra_interval_ms
            if self._last_request_at > 0:
                since = now - self._last_request_at
                interval_s = global_interval_ms / 1000.0
                if since < interval_s:
                    delays.append(interval_s - since)

            endpoint = self._pair_endpoint(url)
            if endpoint and isinstance(params, dict):
                pair = params.get("pair")
                if pair:
                    last = self._last_pair_call_at.get((endpoint, str(pair)), 0.0)
                    if last > 0:
                        since = now - last
                        pair_interval_s = self._pair_interval_ms / 1000.0
                        if since < pair_interval_s:
                            delays.append(pair_interval_s - since)

            wait = max(delays) if delays else 0.0

        if wait > 0:
            await asyncio.sleep(wait + random.uniform(0.05, 1.0))


    async def _record_request(self, url: str, params: Optional[Dict]) -> None:
        async with self._rate_lock:
            now = time.time()
            self._last_request_at = now
            endpoint = self._pair_endpoint(url)
            if endpoint and isinstance(params, dict):
                pair = params.get("pair")
                if pair:
                    self._last_pair_call_at[(endpoint, str(pair))] = now


    async def _set_backoff(self, attempt: int, retry_until: Optional[float], strong: bool) -> float:
        async with self._rate_lock:
            now = time.time()
            if retry_until and retry_until > now:
                wait = retry_until - now
            else:
                wait = min(60.0, 1.0 * (2 ** attempt)) * (0.5 + random.random())
            self._backoff_until = max(self._backoff_until, now + wait)
            if strong:
                self._slowdown_until = max(self._slowdown_until, now + 60.0)
                self._extra_interval_ms = max(self._extra_interval_ms, 1000.0)
        return wait


    async def _make_request(self, method: str, url: str, params: Optional[Dict] = None) -> Any:
        last_status: Optional[int] = None
        last_body_error: Optional[str] = None
        max_retries = 8 if url.startswith(self.spot_rest_url) else 5

        for attempt in range(max_retries):
            await self._await_throttle(url, params)

            try:
                resp = await self.http_client.request(method, url, params=params)
                await self._record_request(url, params)
                last_status = resp.status_code

                if resp.status_code in (429, 418, 502, 503, 504, 520):
                    retry_until = None
                    ra = resp.headers.get("Retry-After")
                    if ra:
                        try:
                            retry_until = time.time() + float(ra)
                        except ValueError:
                            retry_until = None
                    strong = resp.status_code in (429, 418)
                    wait = await self._set_backoff(attempt, retry_until, strong)
                    logger.warning(f"Kraken {url} HTTP {resp.status_code}, backing off {wait:.1f}s (attempt {attempt + 1}/{max_retries})")
                    continue

                if resp.status_code == 200:
                    try:
                        data = orjson.loads(resp.content)
                    except orjson.JSONDecodeError:
                        last_body_error = "JSONDecodeError"
                        wait = await self._set_backoff(attempt, None, strong=False)
                        logger.warning(f"Kraken {url} non-JSON body, backing off {wait:.1f}s (attempt {attempt + 1}/{max_retries})")
                        continue

                    if isinstance(data, dict):
                        err = data.get("error")
                        if err:
                            err_str = str(err)
                            last_body_error = err_str
                            throttle_until = self._parse_throttle_until(err_str)
                            if throttle_until or "Rate limit exceeded" in err_str or "Too many requests" in err_str:
                                wait = await self._set_backoff(attempt, throttle_until, strong=True)
                                logger.warning(f"Kraken {url} rate-limit body ({err_str}), backing off {wait:.1f}s (attempt {attempt + 1}/{max_retries})")
                                continue
                            raise ValueError(f"Kraken Spot API Error: {err}")
                        if data.get("result") == "error":
                            raise ValueError(f"Kraken Futures API Error: {data.get('errors', data)}")
                        if "result" in data and isinstance(data["result"], dict) and not url.startswith(self.futures_chart_url):
                            return data["result"]
                    return data

                resp.raise_for_status()

            except httpx.HTTPStatusError as e:
                raise ValueError(f"Kraken API Error ({e.response.status_code}) on {url}: {e.response.text}")
            except ValueError:
                raise
            except Exception as e:
                last_body_error = f"{type(e).__name__}: {e}"
                wait = await self._set_backoff(attempt, None, strong=False)
                logger.warning(f"Kraken {url} request error: {e}, backing off {wait:.1f}s (attempt {attempt + 1}/{max_retries})")

        raise ValueError(f"Kraken throttled or unavailable: {url} (last_status={last_status}, last_body_error={last_body_error})")


    @staticmethod
    def _hub_key(url: str, payload: dict) -> str:
        if "/v2" in url:
            params = payload.get("params") or {}
            return "spot:" + ":".join([
                str(params.get("channel", "")),
                str(params.get("event_trigger", "")),
                str(params.get("depth", "")),
                str(params.get("snapshot", "")),
            ])
        return f"fut:{payload.get('feed', '')}"


    @staticmethod
    def _topic_key(url: str, payload: dict) -> str:
        if "/v2" in url:
            params = payload.get("params") or {}
            ch = params.get("channel", "")
            sym = (params.get("symbol") or [""])[0]
            return f"{ch}:{sym}"
        feed = payload.get("feed", "")
        pid = (payload.get("product_ids") or [""])[0]
        return f"{feed}:{pid}"


    async def _get_kraken_hub(self, url: str, payload: dict) -> StreamHub:
        hub_key = self._hub_key(url, payload)
        async with self._hubs_lock:
            hub = self._hubs.get(hub_key)
            if hub is not None:
                return hub

            is_spot = "/v2" in url
            async def _connect():
                return await websockets.connect(url)

            if is_spot:
                def _sub(topics: List[str]):
                    out = []
                    for t in topics:
                        p = self._topic_payloads.get(t)
                        if p is not None:
                            out.append(p)
                    return out
                def _unsub(topics: List[str]):
                    out = []
                    for t in topics:
                        p = self._topic_payloads.get(t)
                        if p is None:
                            continue
                        unsub_p = {"method": "unsubscribe", "params": dict(p.get("params") or {})}
                        out.append(unsub_p)
                    return out
                def _route(msg):
                    if not isinstance(msg, dict):
                        return None
                    ch = msg.get("channel")
                    data = msg.get("data")
                    if not ch or not isinstance(data, list) or not data:
                        return None
                    sym = data[0].get("symbol") if isinstance(data[0], dict) else None
                    if not sym:
                        return None
                    return f"{ch}:{sym}"
            else:
                def _sub(topics: List[str]):
                    out = []
                    for t in topics:
                        p = self._topic_payloads.get(t)
                        if p is not None:
                            out.append(p)
                    return out
                def _unsub(topics: List[str]):
                    out = []
                    for t in topics:
                        p = self._topic_payloads.get(t)
                        if p is None:
                            continue
                        out.append({"event": "unsubscribe", "feed": p.get("feed"), "product_ids": p.get("product_ids")})
                    return out
                def _route(msg):
                    if not isinstance(msg, dict):
                        return None
                    feed = msg.get("feed")
                    pid = msg.get("product_id")
                    if not feed or not pid:
                        return None
                    return f"{feed}:{pid}"

            hub = StreamHub(
                name=f"kraken_{hub_key}",
                connect=_connect,
                subscribe_payload=_sub,
                unsubscribe_payload=_unsub,
                route=_route,
            )
            self._hubs[hub_key] = hub
            return hub


    async def _ws_connect(self, url: str, payload: dict) -> AsyncGenerator[Dict, None]:
        topic = self._topic_key(url, payload)
        self._topic_payloads[topic] = payload
        hub = await self._get_kraken_hub(url, payload)
        q = await hub.subscribe(topic)
        try:
            while True:
                msg = await q.get()
                yield msg
        finally:
            await hub.unsubscribe(topic, q)


    async def get_ticker(self, market_type: MarketType, symbol: str) -> Ticker:
        api_symbol = self.get_api_symbol(symbol, market_type)

        if market_type == MarketType.SPOT:
            data = await self._make_request("GET", f"{self.spot_rest_url}/Ticker", {"pair": api_symbol})
            pair_key = list(data.keys())[0]
            t = data[pair_key]

            c_price = float(t["c"][0])
            open_price = float(t["o"])
            vol = float(t["v"][1])
            vwap = float(t["p"][1])

            return Ticker(
                symbol=self.get_model_symbol(api_symbol, market_type),
                price=c_price,
                open_24h=open_price,
                high_24h=float(t["h"][1]),
                low_24h=float(t["l"][1]),
                volume_24h=vol,
                quote_volume_24h=vwap * vol,
                price_change_percent=((c_price - open_price) / open_price) * 100 if open_price > 0 else 0,
                timestamp=int(time.time() * 1000),
            )
        else:
            data = await self._make_request("GET", f"{self.futures_rest_url}/tickers")
            for t in data.get("tickers", []):
                if t["symbol"] == api_symbol:
                    last = float(t.get("last", 0))
                    vol = float(t.get("vol24h", 0))
                    open_24h = float(t.get("open24h", 0))
                    high_24h = float(t.get("high24h", 0))
                    low_24h = float(t.get("low24h", 0))
                    q_vol = float(t.get("volumeQuote", 0))

                    pc = 0.0
                    if open_24h > 0:
                        pc = ((last - open_24h) / open_24h) * 100

                    return Ticker(
                        symbol=self.get_model_symbol(api_symbol, market_type),
                        price=last,
                        open_24h=open_24h,
                        high_24h=high_24h,
                        low_24h=low_24h,
                        volume_24h=vol,
                        quote_volume_24h=q_vol,
                        price_change_percent=pc,
                        timestamp=int(time.time() * 1000),
                    )
            raise ValueError(f"Symbol {api_symbol} not found")


    async def get_book_ticker(self, market_type: MarketType, symbol: str) -> BookTicker:
        api_symbol = self.get_api_symbol(symbol, market_type)

        if market_type == MarketType.SPOT:
            data = await self._make_request("GET", f"{self.spot_rest_url}/Depth", {"pair": api_symbol, "count": 1})
            pair_key = list(data.keys())[0]
            bids = data[pair_key].get("bids", [])
            asks = data[pair_key].get("asks", [])

            return BookTicker(
                symbol=self.get_model_symbol(api_symbol, market_type),
                bid_price=float(bids[0][0]) if bids else 0.0,
                bid_qty=float(bids[0][1]) if bids else 0.0,
                ask_price=float(asks[0][0]) if asks else 0.0,
                ask_qty=float(asks[0][1]) if asks else 0.0,
                timestamp=self.normalize_timestamp(bids[0][2] if bids else time.time()),
            )
        else:
            data = await self._make_request("GET", f"{self.futures_rest_url}/tickers")
            for t in data.get("tickers", []):
                if t["symbol"] == api_symbol:
                    return BookTicker(
                        symbol=self.get_model_symbol(api_symbol, market_type),
                        bid_price=float(t.get("bid", 0)),
                        bid_qty=float(t.get("bidSize", 0)),
                        ask_price=float(t.get("ask", 0)),
                        ask_qty=float(t.get("askSize", 0)),
                        timestamp=int(time.time() * 1000),
                    )
            raise ValueError(f"Symbol {api_symbol} not found")


    async def get_orderbook(self, market_type: MarketType, symbol: str, depth: int = 20) -> OrderBook:
        api_symbol = self.get_api_symbol(symbol, market_type)

        max_depth = self.get_capabilities()["markets"][market_type]["orderbook"]["max_depth"]
        req_depth = min(depth, max_depth)

        if market_type == MarketType.SPOT:
            data = await self._make_request("GET", f"{self.spot_rest_url}/Depth", {"pair": api_symbol, "count": req_depth})
            pair_key = list(data.keys())[0]
            book = data[pair_key]
            ts = float(book["bids"][0][2]) if book.get("bids") else time.time()
            bids = [[float(p), float(q)] for p, q, _ in book.get("bids", [])]
            asks = [[float(p), float(q)] for p, q, _ in book.get("asks", [])]
            bids.sort(key=lambda r: r[0], reverse=True)
            asks.sort(key=lambda r: r[0])
            return OrderBook(
                symbol=self.get_model_symbol(api_symbol, market_type),
                bids=bids[:depth],
                asks=asks[:depth],
                timestamp=self.normalize_timestamp(ts),
            )
        else:
            data = await self._make_request("GET", f"{self.futures_rest_url}/orderbook", {"symbol": api_symbol})
            book = data.get("orderBook", {})
            bids = [[float(p), float(q)] for p, q in book.get("bids", [])]
            asks = [[float(p), float(q)] for p, q in book.get("asks", [])]
            bids.sort(key=lambda r: r[0], reverse=True)
            asks.sort(key=lambda r: r[0])
            return OrderBook(
                symbol=self.get_model_symbol(api_symbol, market_type),
                bids=bids[:depth],
                asks=asks[:depth],
                timestamp=int(time.time() * 1000),
            )


    async def get_trades(self, market_type: MarketType, symbol: str, limit: int = 100) -> List[Trade]:
        limit = min(limit, self.get_capabilities()["markets"][market_type]["trades"]["max_limit"])
        api_symbol = self.get_api_symbol(symbol, market_type)

        if market_type == MarketType.SPOT:
            data = await self._make_request("GET", f"{self.spot_rest_url}/Trades", {"pair": api_symbol})
            pair_key = [k for k in data.keys() if k != "last"][0]
            raw = data[pair_key]
            trades = [Trade(
                id=str(int(t[2] * 1000000)) + str(i),
                price=float(t[0]),
                qty=float(t[1]),
                side="buy" if t[3] == "b" else "sell",
                timestamp=self.normalize_timestamp(t[2]),
            ) for i, t in enumerate(raw)]
        else:
            data = await self._make_request("GET", f"{self.futures_rest_url}/history", {"symbol": api_symbol})
            raw = data.get("history", [])
            trades = [Trade(
                id=str(t.get("trade_id", t["time"])),
                price=float(t["price"]),
                qty=float(t["size"]),
                side=t.get("side", "buy").lower(),
                timestamp=self.normalize_timestamp(t["time"]),
            ) for t in raw]
        trades.sort(key=lambda t: t.timestamp)
        return trades[-limit:]


    async def get_agg_trades(self, market_type: MarketType, symbol: str, start_time: Optional[int] = None, limit: int = 500) -> List[AggTrade]:
        raise NotImplementedError(f"{self.name} does not support agg_trades for {market_type.value}")


    async def get_candles(self, market_type: MarketType, symbol: str, interval: str, start_time: Optional[int] = None, limit: int = 100) -> List[Candle]:
        api_symbol = self.get_api_symbol(symbol, market_type)

        if market_type == MarketType.SPOT:
            interval_min = self._map_spot_interval(interval)
            interval_secs = int(interval_min) * 60 if str(interval_min).isdigit() else 0

            params = {"pair": api_symbol, "interval": interval_min}
            if start_time and interval_secs > 0:
                anchor_secs = int(start_time / 1000)
                params["since"] = max(0, anchor_secs - limit * interval_secs)

            data = await self._make_request("GET", f"{self.spot_rest_url}/OHLC", params)
            pair_key = [k for k in data.keys() if k != "last"][0]
            results = []

            for b in data[pair_key]:
                results.append(Candle(
                    timestamp=self.normalize_timestamp(b[0]),
                    open=float(b[1]),
                    high=float(b[2]),
                    low=float(b[3]),
                    close=float(b[4]),
                    volume=float(b[6]),
                ))

            results.sort(key=lambda c: c.timestamp)
            if start_time:
                results = [c for c in results if c.timestamp <= start_time]
            return results[-limit:]
        else:
            interval_secs_map = {"1m": 60, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600,
                                 "4h": 14400, "12h": 43200, "1d": 86400, "1w": 604800}
            interval_secs = interval_secs_map.get(interval, 3600)
            now_secs = int(time.time())
            anchor_secs = int(start_time / 1000) if start_time else now_secs
            from_ts = anchor_secs - limit * interval_secs

            url = f"{self.futures_chart_url}/trade/{api_symbol}/{interval}"
            all_candles: List[Candle] = []
            seen_ts: set = set()

            for _ in range(20):
                try:
                    data = await self._make_request("GET", url, {"from": from_ts})
                except (httpx.HTTPStatusError, ValueError):
                    break
                batch = data.get("candles", [])
                if not batch:
                    break

                added = 0
                last_ts_secs = from_ts
                for b in batch:
                    ts_ms = self.normalize_timestamp(b["time"])
                    if ts_ms not in seen_ts:
                        seen_ts.add(ts_ms)
                        all_candles.append(Candle(
                            timestamp=ts_ms,
                            open=float(b["open"]),
                            high=float(b["high"]),
                            low=float(b["low"]),
                            close=float(b["close"]),
                            volume=float(b["volume"]),
                        ))
                        added += 1
                    last_ts_secs = ts_ms // 1000

                if added == 0 or last_ts_secs >= anchor_secs - interval_secs:
                    break

                from_ts = last_ts_secs + interval_secs

            all_candles.sort(key=lambda c: c.timestamp)
            if start_time:
                all_candles = [c for c in all_candles if c.timestamp <= start_time]
            return all_candles[-limit:]


    async def get_mark_price(self, market_type: MarketType, symbol: str) -> MarkPrice:
        if market_type == MarketType.SPOT:
            raise NotImplementedError(f"{self.name} does not support mark_price for {market_type.value}")
        api_symbol = self.get_api_symbol(symbol, market_type)
        data = await self._make_request("GET", f"{self.futures_rest_url}/tickers")

        for t in data.get("tickers", []):
            if t["symbol"] == api_symbol:
                mark = float(t.get("markPrice", 0))
                return MarkPrice(
                    symbol=self.get_model_symbol(api_symbol, market_type),
                    mark_price=mark,
                    index_price=float(t.get("indexPrice", mark)),
                    funding_rate=float(t.get("fundingRate", 0)),
                    next_funding_time=0,
                    timestamp=int(time.time() * 1000),
                )
        raise ValueError(f"Symbol {api_symbol} not found")


    async def get_funding_rate(self, market_type: MarketType, symbol: str, start_time: Optional[int] = None, limit: int = 100) -> List[FundingRate]:
        if market_type == MarketType.SPOT:
            raise NotImplementedError(f"{self.name} does not support funding_rate for {market_type.value}")
        api_symbol = self.get_api_symbol(symbol, market_type)

        data = await self._make_request("GET", f"{self.futures_rest_url}/historical-funding-rates", {"symbol": api_symbol})
        rates = data.get("rates", [])

        results = []
        for r in rates:
            ts = self.normalize_timestamp(r.get("timestamp"))
            if start_time and ts > start_time:
                continue
            results.append(FundingRate(
                symbol=self.get_model_symbol(api_symbol, market_type),
                rate=float(r.get("fundingRate", 0)),
                timestamp=ts,
            ))
        results.sort(key=lambda x: x.timestamp)

        return results[-limit:]


    async def get_open_interest(self, market_type: MarketType, symbol: str, period: str = "1h", start_time: Optional[int] = None, limit: int = 30) -> List[OpenInterest]:
        if market_type == MarketType.SPOT:
            raise NotImplementedError(f"{self.name} does not support open_interest for {market_type.value}")
        api_symbol = self.get_api_symbol(symbol, market_type)

        interval_secs = self._map_analytics_interval(period)
        url = f"{self.futures_chart_url}/analytics/{api_symbol}/open-interest"

        anchor_secs = int(start_time / 1000) if start_time else int(time.time())
        calc_start = max(0, anchor_secs - limit * interval_secs)
        params = {"interval": interval_secs, "since": calc_start}

        data = await self._make_request("GET", url, params)
        elements = self._extract_analytics(data, "openInterest")

        results = [OpenInterest(
            symbol=self.get_model_symbol(api_symbol, market_type),
            open_interest=float(e["value"]),
            value_usd=0.0,
            timestamp=self.normalize_timestamp(e["time"]),
        ) for e in elements]
        results.sort(key=lambda x: x.timestamp)
        if start_time:
            results = [r for r in results if r.timestamp <= start_time]

        return results[-limit:]


    async def get_liquidations(self, market_type: MarketType, symbol: str, start_time: Optional[int] = None, limit: int = 100) -> List[Liquidation]:
        raise NotImplementedError(f"{self.name} does not support liquidations for {market_type.value}")


    async def get_long_short_ratio(self, market_type: MarketType, symbol: str, period: str = "5m", start_time: Optional[int] = None, limit: int = 30) -> List[LongShortRatio]:
        if market_type == MarketType.SPOT:
            raise NotImplementedError(f"{self.name} does not support long_short_ratio for {market_type.value}")
        api_symbol = self.get_api_symbol(symbol, market_type)

        interval_secs = self._map_analytics_interval(period)
        url = f"{self.futures_chart_url}/analytics/{api_symbol}/long-short-ratio"

        anchor_secs = int(start_time / 1000) if start_time else int(time.time())
        calc_start = max(0, anchor_secs - limit * interval_secs)
        params = {"interval": interval_secs, "since": calc_start}

        data = await self._make_request("GET", url, params)
        elements = self._extract_analytics(data, "ratio")

        results = [LongShortRatio(
            symbol=self.get_model_symbol(api_symbol, market_type),
            ratio=float(e["value"]),
            long_account=0.0,
            short_account=0.0,
            timestamp=self.normalize_timestamp(e["time"]),
        ) for e in elements]
        results.sort(key=lambda x: x.timestamp)
        if start_time:
            results = [r for r in results if r.timestamp <= start_time]

        return results[-limit:]


    async def get_exchange_info(self, market_type: MarketType) -> List[SymbolInfo]:
        results = []
        if market_type == MarketType.SPOT:
            data = await self._make_request("GET", f"{self.spot_rest_url}/AssetPairs")
            for k, v in data.items():
                if "." in k:
                    continue
                if v.get("status") != "online":
                    continue
                wsname = v.get("wsname", "/")
                base, quote = wsname.split("/") if "/" in wsname else (v.get("base", ""), v.get("quote", ""))

                results.append(SymbolInfo(
                    symbol=self.get_model_symbol(v.get("altname", k), market_type),
                    native_symbol=v.get("altname", k),
                    base_asset=base,
                    quote_asset=quote,
                    price_precision=int(v.get("pair_decimals", 8)),
                    quantity_precision=int(v.get("lot_decimals", 8)),
                    min_qty=float(v.get("ordermin", 0)),
                    max_qty=0.0,
                    min_notional=float(v.get("costmin", 0)),
                ))
        else:
            data = await self._make_request("GET", f"{self.futures_rest_url}/instruments")
            for v in data.get("instruments", []):
                if not v.get("tradeable"):
                    continue
                t = v.get("type", "")
                sym = v.get("symbol", "")
                if market_type == MarketType.INVERSE and (t != "futures_inverse" or not sym.startswith("PI_")):
                    continue
                if market_type == MarketType.LINEAR and (t != "flexible_futures" or not sym.startswith("PF_")):
                    continue

                tick = float(v.get("tickSize", 0) or 0)
                min_size = float(v.get("minTradeSize", 0) or 0)
                max_size = float(v.get("maxTradeSize", 0) or 0)

                results.append(SymbolInfo(
                    symbol=self.get_model_symbol(v["symbol"], market_type),
                    native_symbol=v["symbol"],
                    base_asset=v.get("baseCurrency", "").upper(),
                    quote_asset=v.get("quoteCurrency", "").upper(),
                    price_precision=self._decimal_places(tick),
                    quantity_precision=self._decimal_places(min_size),
                    min_qty=min_size,
                    max_qty=max_size,
                    min_notional=0.0,
                ))
        return results


    async def get_markets(self, market_type: MarketType) -> List[str]:
        info = await self.get_exchange_info(market_type)
        return [s.symbol for s in info]


    async def stream_ticker(self, market_type: MarketType, symbol: str) -> AsyncGenerator[Ticker, None]:
        api_symbol = self.get_api_symbol(symbol, market_type)

        if market_type == MarketType.SPOT:
            ws_sym = await self._spot_ws_symbol(api_symbol)
            payload = {"method": "subscribe", "params": {"channel": "ticker", "symbol": [ws_sym]}}
            async for data in self._ws_connect(self.spot_ws_url, payload):
                if data.get("type") in ["snapshot", "update"] and data.get("channel") == "ticker":
                    for item in data.get("data", []):
                        last = float(item.get("last", 0))
                        yield Ticker(
                            symbol=self.get_model_symbol(api_symbol, market_type),
                            price=last,
                            open_24h=0.0,
                            high_24h=float(item.get("high", 0)),
                            low_24h=float(item.get("low", 0)),
                            volume_24h=float(item.get("volume", 0)),
                            quote_volume_24h=float(item.get("vwap", 0)) * float(item.get("volume", 0)),
                            price_change_percent=float(item.get("change_pct", 0)),
                            timestamp=self.normalize_timestamp(item.get("timestamp", time.time())),
                        )
        else:
            payload = {"event": "subscribe", "feed": "ticker", "product_ids": [api_symbol]}
            async for data in self._ws_connect(self.futures_ws_url, payload):
                if data.get("feed") == "ticker" and "last" in data:
                    last = float(data["last"])
                    open_24h = float(data.get("open", 0))
                    pc = 0.0
                    if open_24h > 0:
                        pc = (float(data.get("change", 0)) / open_24h) * 100
                    yield Ticker(
                        symbol=self.get_model_symbol(api_symbol, market_type),
                        price=last,
                        open_24h=open_24h,
                        high_24h=float(data.get("high", 0)),
                        low_24h=float(data.get("low", 0)),
                        volume_24h=float(data.get("volume", 0)),
                        quote_volume_24h=float(data.get("volumeQuote", 0)),
                        price_change_percent=pc,
                        timestamp=self.normalize_timestamp(data.get("time", time.time())),
                    )


    async def stream_book_ticker(self, market_type: MarketType, symbol: str) -> AsyncGenerator[BookTicker, None]:
        api_symbol = self.get_api_symbol(symbol, market_type)

        if market_type == MarketType.SPOT:
            ws_sym = await self._spot_ws_symbol(api_symbol)
            payload = {"method": "subscribe", "params": {"channel": "ticker", "symbol": [ws_sym], "event_trigger": "bbo"}}
            async for data in self._ws_connect(self.spot_ws_url, payload):
                if data.get("type") in ["snapshot", "update"] and data.get("channel") == "ticker":
                    for item in data.get("data", []):
                        yield BookTicker(
                            symbol=self.get_model_symbol(api_symbol, market_type),
                            bid_price=float(item.get("bid", 0)),
                            bid_qty=float(item.get("bid_qty", 0)),
                            ask_price=float(item.get("ask", 0)),
                            ask_qty=float(item.get("ask_qty", 0)),
                            timestamp=self.normalize_timestamp(item.get("timestamp", time.time())),
                        )
        else:
            payload = {"event": "subscribe", "feed": "ticker", "product_ids": [api_symbol]}
            async for data in self._ws_connect(self.futures_ws_url, payload):
                if data.get("feed") == "ticker" and "bid" in data:
                    yield BookTicker(
                        symbol=self.get_model_symbol(api_symbol, market_type),
                        bid_price=float(data.get("bid", 0)),
                        bid_qty=float(data.get("bid_size", 0)),
                        ask_price=float(data.get("ask", 0)),
                        ask_qty=float(data.get("ask_size", 0)),
                        timestamp=self.normalize_timestamp(data.get("time", time.time())),
                    )


    async def stream_orderbook(self, market_type: MarketType, symbol: str, depth: int = 20, update_speed: str = "100ms") -> AsyncGenerator[OrderBook, None]:
        api_symbol = self.get_api_symbol(symbol, market_type)

        if market_type == MarketType.SPOT:
            ws_sym = await self._spot_ws_symbol(api_symbol)
            req_depth = 10 if depth <= 10 else 25 if depth <= 25 else 100
            payload = {"method": "subscribe", "params": {"channel": "book", "symbol": [ws_sym], "depth": req_depth}}
            async for data in self._ws_connect(self.spot_ws_url, payload):
                if data.get("channel") == "book" and data.get("type") in ["snapshot", "update"]:
                    for item in data.get("data", []):
                        yield OrderBook(
                            symbol=self.get_model_symbol(api_symbol, market_type),
                            bids=[[float(b["price"]), float(b["qty"])] for b in item.get("bids", [])],
                            asks=[[float(a["price"]), float(a["qty"])] for a in item.get("asks", [])],
                            timestamp=self.normalize_timestamp(item.get("timestamp", time.time())),
                        )
        else:
            payload = {"event": "subscribe", "feed": "book", "product_ids": [api_symbol]}
            async for data in self._ws_connect(self.futures_ws_url, payload):
                if data.get("feed") in ["book", "book_snapshot"] and "bids" in data:
                    yield OrderBook(
                        symbol=self.get_model_symbol(api_symbol, market_type),
                        bids=[[float(b["price"]), float(b["qty"])] for b in data.get("bids", [])[:depth]],
                        asks=[[float(a["price"]), float(a["qty"])] for a in data.get("asks", [])[:depth]],
                        timestamp=self.normalize_timestamp(data.get("timestamp", time.time())),
                    )


    async def stream_trades(self, market_type: MarketType, symbol: str) -> AsyncGenerator[Trade, None]:
        api_symbol = self.get_api_symbol(symbol, market_type)

        if market_type == MarketType.SPOT:
            ws_sym = await self._spot_ws_symbol(api_symbol)
            payload = {"method": "subscribe", "params": {"channel": "trade", "symbol": [ws_sym], "snapshot": True}}
            async for data in self._ws_connect(self.spot_ws_url, payload):
                if data.get("type") in ["snapshot", "update"] and data.get("channel") == "trade":
                    for t in data.get("data", []):
                        yield Trade(
                            id=str(t.get("trade_id", t.get("timestamp", time.time()))),
                            price=float(t.get("price", 0)),
                            qty=float(t.get("qty", 0)),
                            side=t.get("side", "buy").lower(),
                            timestamp=self.normalize_timestamp(t.get("timestamp", time.time())),
                        )
        else:
            payload = {"event": "subscribe", "feed": "trade", "product_ids": [api_symbol]}
            async for data in self._ws_connect(self.futures_ws_url, payload):
                feed = data.get("feed")
                if feed == "trade" and "price" in data:
                    yield Trade(
                        id=str(data.get("uid", data.get("time"))),
                        price=float(data["price"]),
                        qty=float(data.get("qty", 0)),
                        side=data.get("side", "buy").lower(),
                        timestamp=self.normalize_timestamp(data.get("time", time.time())),
                    )
                elif feed == "trade_snapshot" and "trades" in data:
                    for t in data["trades"]:
                        yield Trade(
                            id=str(t.get("uid", t.get("time"))),
                            price=float(t["price"]),
                            qty=float(t.get("qty", 0)),
                            side=t.get("side", "buy").lower(),
                            timestamp=self.normalize_timestamp(t.get("time", time.time())),
                        )


    async def stream_mark_price(self, market_type: MarketType, symbol: str) -> AsyncGenerator[MarkPrice, None]:
        if market_type == MarketType.SPOT:
            raise NotImplementedError(f"{self.name} does not support stream_mark_price for {market_type.value}")
        api_symbol = self.get_api_symbol(symbol, market_type)
        payload = {"event": "subscribe", "feed": "ticker", "product_ids": [api_symbol]}

        async for data in self._ws_connect(self.futures_ws_url, payload):
            if data.get("feed") == "ticker" and "markPrice" in data:
                mark = float(data["markPrice"])
                yield MarkPrice(
                    symbol=self.get_model_symbol(api_symbol, market_type),
                    mark_price=mark,
                    index_price=float(data.get("indexPrice", mark)),
                    funding_rate=float(data.get("funding_rate", 0)),
                    next_funding_time=0,
                    timestamp=self.normalize_timestamp(data.get("time", time.time())),
                )
