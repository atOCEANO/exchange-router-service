import httpx
import orjson
import asyncio
import random
import time
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
    MarketType, SymbolInfo, OpenInterest, FundingRate, Liquidation, LongShortRatio,
)


logger = logging.getLogger("kraken_adapter")


class KrakenAdapter(BaseExchange):
    FUT_TICKERS_TTL_S = 1.0


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

        self._fut_tickers_data: Optional[Any] = None
        self._fut_tickers_at   = 0.0
        self._fut_tickers_lock = asyncio.Lock()

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
        await self._step("spot_ws_map",  self._ensure_spot_ws_map())


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
                    "mark_price": {
                        "rest": False,
                        "ws":   False,
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
                        "paginated":    False,
                        "max_limit":    720,
                        "retention_ms": None,
                        "intervals": ["1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w"],
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
                        "intervals": ["1m", "5m", "15m", "30m", "1h", "4h", "12h", "1d", "1w"],
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
                        "intervals": ["1m", "5m", "15m", "30m", "1h", "4h", "12h", "1d"],
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
                    "mark_price": {
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
                        "intervals": ["1m", "5m", "15m", "30m", "1h", "4h", "12h", "1d", "1w"],
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
                        "intervals": ["1m", "5m", "15m", "30m", "1h", "4h", "12h", "1d"],
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
        mapping = {
            "1m": 1,
            "5m": 5,
            "15m": 15,
            "30m": 30,
            "1h": 60,
            "4h": 240,
            "1d": 1440,
            "1w": 10080,
        }
        if interval not in mapping:
            raise ValueError(f"Kraken does not support interval {interval}")
        return mapping[interval]


    def _map_analytics_interval(self, interval: str) -> int:
        mapping = {
            "1m": 60,
            "5m": 300,
            "15m": 900,
            "30m": 1800,
            "1h": 3600,
            "4h": 14400,
            "12h": 43200,
            "1d": 86400,
        }
        if interval not in mapping:
            raise ValueError(f"Kraken does not support interval {interval}")
        return mapping[interval]


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
        while True:
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
                pair     = params.get("pair") if endpoint and isinstance(params, dict) else None
                if pair:
                    last = self._last_pair_call_at.get((endpoint, str(pair)), 0.0)
                    if last > 0:
                        since = now - last
                        pair_interval_s = self._pair_interval_ms / 1000.0
                        if since < pair_interval_s:
                            delays.append(pair_interval_s - since)

                wait = max(delays) if delays else 0.0

                if wait <= 0:
                    self._last_request_at = now
                    if pair:
                        self._last_pair_call_at[(endpoint, str(pair))] = now
                    return

            await asyncio.sleep(wait + random.uniform(0.01, 0.1))


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
                if e.response.status_code >= 500:
                    raise UpstreamUnavailableError(f"Kraken API Error ({e.response.status_code}) on {url}: {e.response.text}")
                raise ValueError(f"Kraken API Error ({e.response.status_code}) on {url}: {e.response.text}")
            except ValueError:
                raise
            except Exception as e:
                last_body_error = f"{type(e).__name__}: {e}"
                wait = await self._set_backoff(attempt, None, strong=False)
                logger.warning(f"Kraken {url} request error: {e}, backing off {wait:.1f}s (attempt {attempt + 1}/{max_retries})")

        raise UpstreamUnavailableError(f"Kraken throttled or unavailable: {url} (last_status={last_status}, last_body_error={last_body_error})")


    async def _futures_tickers(self) -> Any:
        async with self._fut_tickers_lock:
            now = time.monotonic()
            if self._fut_tickers_data is not None and now - self._fut_tickers_at < self.FUT_TICKERS_TTL_S:
                return self._fut_tickers_data

            data = await self._make_request("GET", f"{self.futures_rest_url}/tickers")

            self._fut_tickers_data = data
            self._fut_tickers_at   = time.monotonic()

            return data


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
                    pid  = msg.get("product_id")
                    if not feed or not pid:
                        return None
                    if feed.endswith("_snapshot"):
                        feed = feed[: -len("_snapshot")]
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
        model_sym  = self.get_model_symbol(api_symbol, market_type)
        info       = await self._info_for(market_type, model_sym)
        quote      = info.quote_asset if info else ""

        if market_type == MarketType.SPOT:
            data = await self._make_request("GET", f"{self.spot_rest_url}/Ticker", {"pair": api_symbol})
            pair_key = list(data.keys())[0]
            t = data[pair_key]

            c_price    = float(t["c"][0])
            open_price = float(t["o"])
            vol        = float(t["v"][1])

            return Ticker(
                symbol               = model_sym,
                market_type          = market_type,
                quote                = quote,
                price                = c_price,
                open_24h             = open_price,
                high_24h             = float(t["h"][1]),
                low_24h              = float(t["l"][1]),
                volume_24h           = build_volume_value(
                    native        = vol,
                    volume_unit   = "base",
                    contract_size = None,
                    close         = c_price,
                ),
                price_change_percent = ((c_price - open_price) / open_price) * 100 if open_price > 0 else 0,
                timestamp            = int(time.time() * 1000),
            )

        data       = await self._futures_tickers()
        server_ts  = self.normalize_timestamp(data["serverTime"]) if data.get("serverTime") else int(time.time() * 1000)
        is_inverse = market_type == MarketType.INVERSE
        unit       = "contract" if is_inverse else "base"

        for t in data.get("tickers", []):
            if t["symbol"] == api_symbol:
                last     = float(t.get("last", 0))
                vol      = float(t.get("vol24h", 0))
                open_24h = float(t.get("open24h", 0))

                pc = 0.0
                if open_24h > 0:
                    pc = ((last - open_24h) / open_24h) * 100

                return Ticker(
                    symbol               = model_sym,
                    market_type          = market_type,
                    quote                = quote,
                    price                = last,
                    open_24h             = open_24h,
                    high_24h             = float(t.get("high24h", 0)),
                    low_24h              = float(t.get("low24h", 0)),
                    volume_24h           = build_volume_value(
                        native        = vol,
                        volume_unit   = unit,
                        contract_size = info.contract_size if info else None,
                        close         = last,
                    ),
                    price_change_percent = pc,
                    timestamp            = server_ts,
                )
        raise ValueError(f"Symbol {api_symbol} not found")


    async def get_book_ticker(self, market_type: MarketType, symbol: str) -> BookTicker:
        api_symbol    = self.get_api_symbol(symbol, market_type)
        model_sym     = self.get_model_symbol(api_symbol, market_type)
        info          = await self._info_for(market_type, model_sym)
        quote         = info.quote_asset if info else ""
        contract_size = info.contract_size if info else None
        is_inverse    = market_type == MarketType.INVERSE
        unit          = "contract" if is_inverse else "base"

        if market_type == MarketType.SPOT:
            data = await self._make_request("GET", f"{self.spot_rest_url}/Depth", {"pair": api_symbol, "count": 1})
            pair_key = list(data.keys())[0]
            bids = data[pair_key].get("bids", [])
            asks = data[pair_key].get("asks", [])
            if not bids or not asks:
                raise ValueError(f"No quotes available for {api_symbol} on Kraken spot")

            bid_price = float(bids[0][0])
            ask_price = float(asks[0][0])
            return BookTicker(
                symbol      = model_sym,
                market_type = market_type,
                quote       = quote,
                bid_price   = bid_price,
                bid_qty     = build_qty_value(
                    native        = float(bids[0][1]),
                    qty_unit      = "base",
                    contract_size = None,
                    price         = bid_price,
                ),
                ask_price   = ask_price,
                ask_qty     = build_qty_value(
                    native        = float(asks[0][1]),
                    qty_unit      = "base",
                    contract_size = None,
                    price         = ask_price,
                ),
                timestamp   = self.normalize_timestamp(bids[0][2]),
            )

        data      = await self._futures_tickers()
        server_ts = self.normalize_timestamp(data["serverTime"]) if data.get("serverTime") else int(time.time() * 1000)

        for t in data.get("tickers", []):
            if t["symbol"] == api_symbol:
                bid_price = float(t.get("bid", 0))
                ask_price = float(t.get("ask", 0))
                return BookTicker(
                    symbol      = model_sym,
                    market_type = market_type,
                    quote       = quote,
                    bid_price   = bid_price,
                    bid_qty     = build_qty_value(
                        native        = float(t.get("bidSize", 0)),
                        qty_unit      = unit,
                        contract_size = contract_size,
                        price         = bid_price,
                    ),
                    ask_price   = ask_price,
                    ask_qty     = build_qty_value(
                        native        = float(t.get("askSize", 0)),
                        qty_unit      = unit,
                        contract_size = contract_size,
                        price         = ask_price,
                    ),
                    timestamp   = server_ts,
                )
        raise ValueError(f"Symbol {api_symbol} not found")


    async def get_orderbook(self, market_type: MarketType, symbol: str, depth: int = 20) -> OrderBook:
        api_symbol = self.get_api_symbol(symbol, market_type)
        model_sym  = self.get_model_symbol(api_symbol, market_type)
        info       = await self._info_for(market_type, model_sym)
        quote      = info.quote_asset if info else ""

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
                symbol      = model_sym,
                market_type = market_type,
                quote       = quote,
                bids        = bids[:req_depth],
                asks        = asks[:req_depth],
                qty_unit    = "base",
                timestamp   = self.normalize_timestamp(ts),
            )

        data      = await self._make_request("GET", f"{self.futures_rest_url}/orderbook", {"symbol": api_symbol})
        server_ts = self.normalize_timestamp(data["serverTime"]) if data.get("serverTime") else int(time.time() * 1000)

        book = data.get("orderBook", {})
        bids = [[float(p), float(q)] for p, q in book.get("bids", [])]
        asks = [[float(p), float(q)] for p, q in book.get("asks", [])]
        bids.sort(key=lambda r: r[0], reverse=True)
        asks.sort(key=lambda r: r[0])
        return OrderBook(
            symbol      = model_sym,
            market_type = market_type,
            quote       = quote,
            bids        = bids[:req_depth],
            asks        = asks[:req_depth],
            qty_unit    = "contract" if market_type == MarketType.INVERSE else "base",
            timestamp   = server_ts,
        )


    async def get_trades(self, market_type: MarketType, symbol: str, limit: int = 100) -> List[Trade]:
        limit         = min(limit, self.get_capabilities()["markets"][market_type]["trades"]["max_limit"])
        api_symbol    = self.get_api_symbol(symbol, market_type)
        model_sym     = self.get_model_symbol(api_symbol, market_type)
        info          = await self._info_for(market_type, model_sym)
        quote         = info.quote_asset if info else ""
        contract_size = info.contract_size if info else None
        is_inverse    = market_type == MarketType.INVERSE
        unit          = "contract" if is_inverse else "base"

        if market_type == MarketType.SPOT:
            data = await self._make_request("GET", f"{self.spot_rest_url}/Trades", {"pair": api_symbol})
            pair_key = [k for k in data.keys() if k != "last"][0]
            raw = data[pair_key]
            trades = []
            for i, t in enumerate(raw):
                price    = float(t[0])
                trade_id = str(t[6]) if len(t) > 6 else str(int(t[2] * 1000000)) + str(i)
                trades.append(Trade(
                    id          = trade_id,
                    symbol      = model_sym,
                    market_type = market_type,
                    quote       = quote,
                    price       = price,
                    qty         = build_qty_value(
                        native        = float(t[1]),
                        qty_unit      = unit,
                        contract_size = contract_size,
                        price         = price,
                    ),
                    side        = "buy" if t[3] == "b" else "sell",
                    timestamp   = self.normalize_timestamp(t[2]),
                ))
        else:
            data = await self._make_request("GET", f"{self.futures_rest_url}/history", {"symbol": api_symbol})
            raw = data.get("history", [])
            trades = []
            for t in raw:
                side_raw = t.get("side")
                if side_raw is None:
                    continue
                side_norm = side_raw.lower()
                if side_norm not in ("buy", "sell"):
                    continue
                price = float(t["price"])
                trades.append(Trade(
                    id          = str(t.get("uid") or t.get("time")),
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
                    side        = side_norm,
                    timestamp   = self.normalize_timestamp(t["time"]),
                ))
        trades.sort(key=lambda t: t.timestamp)
        return trades[-limit:]


    async def get_agg_trades(self, market_type: MarketType, symbol: str, start_time: Optional[int] = None, limit: int = 500) -> List[AggTrade]:
        raise NotImplementedError(f"{self.name} does not support agg_trades for {market_type.value}")


    async def get_candles(self, market_type: MarketType, symbol: str, interval: str, start_time: Optional[int] = None, limit: int = 100) -> List[Candle]:
        api_symbol    = self.get_api_symbol(symbol, market_type)
        model_sym     = self.get_model_symbol(api_symbol, market_type)
        info          = await self._info_for(market_type, model_sym)
        quote         = info.quote_asset if info else ""
        contract_size = info.contract_size if info else None
        is_inverse    = market_type == MarketType.INVERSE
        unit          = "contract" if is_inverse else "base"

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
                close = float(b[4])
                results.append(Candle(
                    symbol      = model_sym,
                    market_type = market_type,
                    quote       = quote,
                    interval    = interval,
                    timestamp   = self.normalize_timestamp(b[0]),
                    open        = float(b[1]),
                    high        = float(b[2]),
                    low         = float(b[3]),
                    close       = close,
                    volume      = build_volume_value(
                        native        = float(b[6]),
                        volume_unit   = unit,
                        contract_size = contract_size,
                        close         = close,
                    ),
                ))

            results.sort(key=lambda c: c.timestamp)
            if start_time:
                results = [c for c in results if c.timestamp <= start_time]
            return results[-limit:]
        else:
            interval_secs_map = {
                "1m": 60,
                "5m": 300,
                "15m": 900,
                "30m": 1800,
                "1h": 3600,
                "4h": 14400,
                "12h": 43200,
                "1d": 86400,
                "1w": 604800,
            }
            if interval not in interval_secs_map:
                raise ValueError(f"Kraken does not support interval {interval}")
            interval_secs = interval_secs_map[interval]
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
                        close = float(b["close"])
                        all_candles.append(Candle(
                            symbol      = model_sym,
                            market_type = market_type,
                            quote       = quote,
                            interval    = interval,
                            timestamp   = ts_ms,
                            open        = float(b["open"]),
                            high        = float(b["high"]),
                            low         = float(b["low"]),
                            close       = close,
                            volume      = build_volume_value(
                                native        = float(b["volume"]),
                                volume_unit   = unit,
                                contract_size = contract_size,
                                close         = close,
                            ),
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
        model_sym  = self.get_model_symbol(api_symbol, market_type)
        info       = await self._info_for(market_type, model_sym)
        quote      = info.quote_asset if info else ""

        data      = await self._futures_tickers()
        server_ts = self.normalize_timestamp(data["serverTime"]) if data.get("serverTime") else int(time.time() * 1000)

        for t in data.get("tickers", []):
            if t["symbol"] == api_symbol:
                mark_raw = t.get("markPrice")
                if mark_raw is None:
                    raise ValueError(f"No markPrice available for {api_symbol} on Kraken")
                mark = float(mark_raw)
                if mark <= 0:
                    raise ValueError(f"Non-positive markPrice {mark} for {api_symbol} on Kraken")
                abs_rate_raw = t.get("fundingRate")

                rel_rate: Optional[float] = None
                if abs_rate_raw is not None and mark > 0:
                    abs_rate = float(abs_rate_raw)
                    rel_rate = abs_rate / mark if market_type == MarketType.LINEAR else abs_rate * mark

                idx_raw = t.get("indexPrice")
                idx_price: Optional[float] = float(idx_raw) if idx_raw is not None else None

                funding_block = None
                if rel_rate is not None:
                    funding_block = build_funding_current(
                        kind           = "continuous",
                        per_cycle      = rel_rate,
                        cycle_ms       = 3_600_000,
                        valid_until_ts = server_ts + 3_600_000,
                    )

                return MarkPrice(
                    symbol      = model_sym,
                    market_type = market_type,
                    quote       = quote,
                    mark_price  = mark,
                    index_price = idx_price,
                    funding     = funding_block,
                    timestamp   = server_ts,
                )
        raise ValueError(f"Symbol {api_symbol} not found")


    async def get_funding_rate(self, market_type: MarketType, symbol: str, start_time: Optional[int] = None, limit: int = 100) -> List[FundingRate]:
        if market_type == MarketType.SPOT:
            raise NotImplementedError(f"{self.name} does not support funding_rate for {market_type.value}")
        api_symbol = self.get_api_symbol(symbol, market_type)

        data = await self._make_request("GET", f"{self.futures_rest_url}/historical-funding-rates", {"symbol": api_symbol})
        rates = data.get("rates", [])

        model_sym = self.get_model_symbol(api_symbol, market_type)
        info      = await self._info_for(market_type, model_sym)
        quote     = info.quote_asset if info else ""

        results = []
        for r in rates:
            ts = self.normalize_timestamp(r.get("timestamp"))
            if start_time and ts > start_time:
                continue
            results.append(FundingRate(
                symbol      = model_sym,
                market_type = market_type,
                quote       = quote,
                rate        = build_funding_historical(
                    kind      = "continuous",
                    per_cycle = float(r.get("relativeFundingRate", 0)),
                    cycle_ms  = 3_600_000,
                ),
                timestamp   = ts,
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

        model_sym     = self.get_model_symbol(api_symbol, market_type)
        info          = await self._info_for(market_type, model_sym)
        quote         = info.quote_asset if info else ""
        contract_size = info.contract_size if info else None
        is_inverse    = market_type == MarketType.INVERSE
        oi_unit       = "contract" if is_inverse else "base"
        results = [OpenInterest(
            symbol        = model_sym,
            market_type   = market_type,
            quote         = quote,
            interval      = period,
            open_interest = build_oi_value(
                native          = float(e["value"]),
                oi_unit         = oi_unit,
                contract_size   = contract_size,
                candle_close    = None,
                candle_close_ts = None,
            ),
            timestamp     = self.normalize_timestamp(e["time"]),
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

        model_sym = self.get_model_symbol(api_symbol, market_type)
        results = [LongShortRatio(
            symbol        = model_sym,
            market_type   = market_type,
            interval      = period,
            ratio         = float(e["value"]),
            long_account  = None,
            short_account = None,
            account_scope = "opaque",
            timestamp     = self.normalize_timestamp(e["time"]),
        ) for e in elements]
        results.sort(key=lambda x: x.timestamp)
        if start_time:
            results = [r for r in results if r.timestamp <= start_time]

        return results[-limit:]


    async def _fetch_exchange_info(self, market_type: MarketType) -> List[SymbolInfo]:
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

                raw_ordermin = v.get("ordermin")
                raw_costmin = v.get("costmin")
                spot_min_qty: Optional[float] = float(raw_ordermin) if raw_ordermin is not None else None
                spot_min_notional: Optional[float] = float(raw_costmin) if raw_costmin is not None else None
                if spot_min_qty is not None and spot_min_qty <= 0:
                    spot_min_qty = None
                if spot_min_notional is not None and spot_min_notional <= 0:
                    spot_min_notional = None

                results.append(SymbolInfo(
                    symbol             = self.get_model_symbol(v.get("altname", k), market_type),
                    native_symbol      = v.get("altname", k),
                    base_asset         = base,
                    quote_asset        = quote,
                    price_precision    = int(v.get("pair_decimals", 8)),
                    quantity_precision = int(v.get("lot_decimals", 8)),
                    min_qty            = spot_min_qty,
                    max_qty            = None,
                    min_notional       = spot_min_notional,
                    qty_unit           = "base",
                    contract_size      = None,
                    funding            = None,
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

                tick     = float(v.get("tickSize", 0) or 0)
                raw_min_size = v.get("minTradeSize")
                raw_max_size = v.get("maxTradeSize")
                min_size: Optional[float] = float(raw_min_size) if raw_min_size is not None else None
                max_size: Optional[float] = float(raw_max_size) if raw_max_size is not None else None
                if min_size is not None and min_size <= 0:
                    min_size = None
                if max_size is not None and max_size <= 0:
                    max_size = None

                base_asset  = (v.get("baseCurrency") or "").upper()
                quote_asset = (v.get("quoteCurrency") or "").upper()
                if not base_asset or not quote_asset:
                    stripped = self.get_model_symbol(v["symbol"], market_type)
                    for q in ("USDT", "USDC", "USD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD"):
                        if stripped.endswith(q) and len(stripped) > len(q):
                            base_asset  = stripped[:-len(q)]
                            quote_asset = q
                            break

                qty_unit = "contract" if market_type == MarketType.INVERSE else "base"
                contract_size: Optional[float] = None
                if market_type == MarketType.INVERSE:
                    cs_raw = v.get("contractSize")
                    if cs_raw is not None:
                        try:
                            contract_size = abs(float(cs_raw))
                        except (TypeError, ValueError):
                            contract_size = None
                    if contract_size is None:
                        contract_size = 1.0

                results.append(SymbolInfo(
                    symbol             = self.get_model_symbol(v["symbol"], market_type),
                    native_symbol      = v["symbol"],
                    base_asset         = base_asset,
                    quote_asset        = quote_asset,
                    price_precision    = self._decimal_places(tick),
                    quantity_precision = self._decimal_places(min_size or 0),
                    min_qty            = min_size,
                    max_qty            = max_size,
                    min_notional       = None,
                    qty_unit           = qty_unit,
                    contract_size      = contract_size,
                    funding            = build_funding_convention("continuous"),
                ))
        return results


    async def get_exchange_info(self, market_type: MarketType) -> List[SymbolInfo]:
        cache = await self._ensure_info_cache(market_type)
        return list(cache.values())


    async def get_symbol_info(self, market_type: MarketType, symbol: str) -> SymbolInfo:
        cache = await self._ensure_info_cache(market_type)
        model_sym = self.get_model_symbol(self.get_api_symbol(symbol, market_type), market_type)
        info = cache.get(model_sym)
        if info is None:
            raise ValueError(f"Symbol {symbol} not found on Kraken {market_type.value}")
        return info


    async def stream_ticker(self, market_type: MarketType, symbol: str) -> AsyncGenerator[Ticker, None]:
        api_symbol    = self.get_api_symbol(symbol, market_type)
        model_sym     = self.get_model_symbol(api_symbol, market_type)
        info          = await self._info_for(market_type, model_sym)
        quote         = info.quote_asset if info else ""
        contract_size = info.contract_size if info else None
        is_inverse    = market_type == MarketType.INVERSE
        unit          = "contract" if is_inverse else "base"

        if market_type == MarketType.SPOT:
            ws_sym  = await self._spot_ws_symbol(api_symbol)
            payload = {"method": "subscribe", "params": {"channel": "ticker", "symbol": [ws_sym]}}
            async for data in self._ws_connect(self.spot_ws_url, payload):
                try:
                    if data.get("type") in ["snapshot", "update"] and data.get("channel") == "ticker":
                        for item in data.get("data", []):
                            last = float(item.get("last", 0))
                            yield Ticker(
                                symbol               = model_sym,
                                market_type          = market_type,
                                quote                = quote,
                                price                = last,
                                open_24h             = last - float(item.get("change", 0)),
                                high_24h             = float(item.get("high", 0)),
                                low_24h              = float(item.get("low", 0)),
                                volume_24h           = build_volume_value(
                                    native        = float(item.get("volume", 0)),
                                    volume_unit   = "base",
                                    contract_size = None,
                                    close         = last,
                                ),
                                price_change_percent = float(item.get("change_pct", 0)),
                                timestamp            = self.normalize_timestamp(item.get("timestamp", time.time())),
                            )
                except (ValueError, TypeError, KeyError, IndexError, AttributeError) as e:
                    logger.warning(f"Kraken ticker {api_symbol} malformed frame skipped: {e!r}")
        else:
            payload = {"event": "subscribe", "feed": "ticker", "product_ids": [api_symbol]}
            async for data in self._ws_connect(self.futures_ws_url, payload):
                try:
                    if data.get("feed") == "ticker" and "last" in data:
                        last     = float(data["last"])
                        open_24h = float(data.get("open", 0))
                        pc       = 0.0
                        if open_24h > 0:
                            pc = (float(data.get("change", 0)) / open_24h) * 100
                        yield Ticker(
                            symbol               = model_sym,
                            market_type          = market_type,
                            quote                = quote,
                            price                = last,
                            open_24h             = open_24h,
                            high_24h             = float(data.get("high", 0)),
                            low_24h              = float(data.get("low", 0)),
                            volume_24h           = build_volume_value(
                                native        = float(data.get("volume", 0)),
                                volume_unit   = unit,
                                contract_size = contract_size,
                                close         = last,
                            ),
                            price_change_percent = pc,
                            timestamp            = self.normalize_timestamp(data.get("time", time.time())),
                        )
                except (ValueError, TypeError, KeyError, IndexError, AttributeError) as e:
                    logger.warning(f"Kraken ticker {api_symbol} malformed frame skipped: {e!r}")


    async def stream_book_ticker(self, market_type: MarketType, symbol: str) -> AsyncGenerator[BookTicker, None]:
        api_symbol    = self.get_api_symbol(symbol, market_type)
        model_sym     = self.get_model_symbol(api_symbol, market_type)
        info          = await self._info_for(market_type, model_sym)
        quote         = info.quote_asset if info else ""
        contract_size = info.contract_size if info else None
        is_inverse    = market_type == MarketType.INVERSE
        unit          = "contract" if is_inverse else "base"

        if market_type == MarketType.SPOT:
            ws_sym  = await self._spot_ws_symbol(api_symbol)
            payload = {"method": "subscribe", "params": {"channel": "ticker", "symbol": [ws_sym], "event_trigger": "bbo"}}
            async for data in self._ws_connect(self.spot_ws_url, payload):
                try:
                    if data.get("type") in ["snapshot", "update"] and data.get("channel") == "ticker":
                        for item in data.get("data", []):
                            bid_price = float(item.get("bid", 0))
                            ask_price = float(item.get("ask", 0))
                            yield BookTicker(
                                symbol      = model_sym,
                                market_type = market_type,
                                quote       = quote,
                                bid_price   = bid_price,
                                bid_qty     = build_qty_value(
                                    native        = float(item.get("bid_qty", 0)),
                                    qty_unit      = "base",
                                    contract_size = None,
                                    price         = bid_price,
                                ),
                                ask_price   = ask_price,
                                ask_qty     = build_qty_value(
                                    native        = float(item.get("ask_qty", 0)),
                                    qty_unit      = "base",
                                    contract_size = None,
                                    price         = ask_price,
                                ),
                                timestamp   = self.normalize_timestamp(item.get("timestamp", time.time())),
                            )
                except (ValueError, TypeError, KeyError, IndexError, AttributeError) as e:
                    logger.warning(f"Kraken book_ticker {api_symbol} malformed frame skipped: {e!r}")
        else:
            payload = {"event": "subscribe", "feed": "ticker", "product_ids": [api_symbol]}
            async for data in self._ws_connect(self.futures_ws_url, payload):
                try:
                    if data.get("feed") == "ticker" and "bid" in data:
                        bid_price = float(data.get("bid", 0))
                        ask_price = float(data.get("ask", 0))
                        yield BookTicker(
                            symbol      = model_sym,
                            market_type = market_type,
                            quote       = quote,
                            bid_price   = bid_price,
                            bid_qty     = build_qty_value(
                                native        = float(data.get("bid_size", 0)),
                                qty_unit      = unit,
                                contract_size = contract_size,
                                price         = bid_price,
                            ),
                            ask_price   = ask_price,
                            ask_qty     = build_qty_value(
                                native        = float(data.get("ask_size", 0)),
                                qty_unit      = unit,
                                contract_size = contract_size,
                                price         = ask_price,
                            ),
                            timestamp   = self.normalize_timestamp(data.get("time", time.time())),
                        )
                except (ValueError, TypeError, KeyError, IndexError, AttributeError) as e:
                    logger.warning(f"Kraken book_ticker {api_symbol} malformed frame skipped: {e!r}")


    async def stream_orderbook(self, market_type: MarketType, symbol: str, depth: int = 20, update_speed: str = "100ms") -> AsyncGenerator[OrderBook, None]:
        api_symbol = self.get_api_symbol(symbol, market_type)
        model_sym  = self.get_model_symbol(api_symbol, market_type)
        info       = await self._info_for(market_type, model_sym)
        quote      = info.quote_asset if info else ""

        if market_type == MarketType.SPOT:
            ws_sym    = await self._spot_ws_symbol(api_symbol)
            req_depth = 10 if depth <= 10 else 25 if depth <= 25 else 100
            payload   = {"method": "subscribe", "params": {"channel": "book", "symbol": [ws_sym], "depth": req_depth}}

            bids: Dict[float, float] = {}
            asks: Dict[float, float] = {}
            bootstrapped             = False

            async for data in self._ws_connect(self.spot_ws_url, payload):
                try:
                    if data.get("channel") != "book" or data.get("type") not in ("snapshot", "update"):
                        continue
                    if data.get("type") == "snapshot":
                        bids.clear()
                        asks.clear()
                        bootstrapped = True
                    if not bootstrapped:
                        continue
                    for item in data.get("data", []):
                        for b in item.get("bids", []):
                            p, q = float(b["price"]), float(b["qty"])
                            if q == 0: bids.pop(p, None)
                            else:      bids[p] = q
                        for a in item.get("asks", []):
                            p, q = float(a["price"]), float(a["qty"])
                            if q == 0: asks.pop(p, None)
                            else:      asks[p] = q

                        sorted_bids = sorted(bids.items(), key=lambda kv: -kv[0])[:depth]
                        sorted_asks = sorted(asks.items(), key=lambda kv: kv[0])[:depth]

                        yield OrderBook(
                            symbol      = model_sym,
                            market_type = market_type,
                            quote       = quote,
                            bids        = [[p, q] for p, q in sorted_bids],
                            asks        = [[p, q] for p, q in sorted_asks],
                            qty_unit    = "base",
                            timestamp   = self.normalize_timestamp(item.get("timestamp", time.time())),
                        )
                except (ValueError, TypeError, KeyError, IndexError, AttributeError) as e:
                    logger.warning(f"Kraken orderbook {api_symbol} parse error mid-delta: {e!r}; ending stream so clients resync")
                    return
        else:
            payload = {"event": "subscribe", "feed": "book", "product_ids": [api_symbol]}

            bids: Dict[float, float] = {}
            asks: Dict[float, float] = {}
            bootstrapped             = False

            async for data in self._ws_connect(self.futures_ws_url, payload):
                try:
                    feed = data.get("feed")
                    if feed == "book_snapshot":
                        bids.clear()
                        asks.clear()
                        for b in data.get("bids", []):
                            p, q = float(b["price"]), float(b["qty"])
                            if q > 0: bids[p] = q
                        for a in data.get("asks", []):
                            p, q = float(a["price"]), float(a["qty"])
                            if q > 0: asks[p] = q
                        bootstrapped = True
                    elif feed == "book" and "side" in data and "price" in data:
                        if not bootstrapped:
                            continue
                        p, q = float(data["price"]), float(data.get("qty", 0))
                        side = data.get("side", "").lower()
                        book = bids if side == "buy" else asks if side == "sell" else None
                        if book is None:
                            continue
                        if q == 0: book.pop(p, None)
                        else:      book[p] = q
                    else:
                        continue
                    if not bids or not asks:
                        continue

                    sorted_bids = sorted(bids.items(), key=lambda kv: -kv[0])[:depth]
                    sorted_asks = sorted(asks.items(), key=lambda kv: kv[0])[:depth]

                    yield OrderBook(
                        symbol      = model_sym,
                        market_type = market_type,
                        quote       = quote,
                        bids        = [[p, q] for p, q in sorted_bids],
                        asks        = [[p, q] for p, q in sorted_asks],
                        qty_unit    = "contract" if market_type == MarketType.INVERSE else "base",
                        timestamp   = self.normalize_timestamp(data.get("timestamp", time.time())),
                    )
                except (ValueError, TypeError, KeyError, IndexError, AttributeError) as e:
                    logger.warning(f"Kraken orderbook {api_symbol} parse error mid-delta: {e!r}; ending stream so clients resync")
                    return


    async def stream_trades(self, market_type: MarketType, symbol: str) -> AsyncGenerator[Trade, None]:
        api_symbol    = self.get_api_symbol(symbol, market_type)
        model_sym     = self.get_model_symbol(api_symbol, market_type)
        info          = await self._info_for(market_type, model_sym)
        quote         = info.quote_asset if info else ""
        contract_size = info.contract_size if info else None
        is_inverse    = market_type == MarketType.INVERSE
        unit          = "contract" if is_inverse else "base"

        def _mk_trade(trade_id: str, price: float, qty_native: float, side_norm: str, ts) -> Trade:
            return Trade(
                id          = trade_id,
                symbol      = model_sym,
                market_type = market_type,
                quote       = quote,
                price       = price,
                qty         = build_qty_value(
                    native        = qty_native,
                    qty_unit      = unit,
                    contract_size = contract_size,
                    price         = price,
                ),
                side        = side_norm,
                timestamp   = self.normalize_timestamp(ts),
            )

        if market_type == MarketType.SPOT:
            ws_sym  = await self._spot_ws_symbol(api_symbol)
            payload = {"method": "subscribe", "params": {"channel": "trade", "symbol": [ws_sym], "snapshot": True}}
            async for data in self._ws_connect(self.spot_ws_url, payload):
                try:
                    if data.get("type") in ["snapshot", "update"] and data.get("channel") == "trade":
                        for t in data.get("data", []):
                            side_raw = t.get("side")
                            if side_raw is None:
                                continue
                            side_norm = side_raw.lower()
                            if side_norm not in ("buy", "sell"):
                                continue
                            yield _mk_trade(
                                trade_id   = str(t.get("trade_id", t.get("timestamp", time.time()))),
                                price      = float(t.get("price", 0)),
                                qty_native = float(t.get("qty", 0)),
                                side_norm  = side_norm,
                                ts         = t.get("timestamp", time.time()),
                            )
                except (ValueError, TypeError, KeyError, IndexError, AttributeError) as e:
                    logger.warning(f"Kraken trades {api_symbol} malformed frame skipped: {e!r}")
        else:
            payload = {"event": "subscribe", "feed": "trade", "product_ids": [api_symbol]}
            async for data in self._ws_connect(self.futures_ws_url, payload):
                try:
                    feed = data.get("feed")
                    if feed == "trade" and "price" in data:
                        side_raw = data.get("side")
                        if side_raw is None:
                            continue
                        side_norm = side_raw.lower()
                        if side_norm not in ("buy", "sell"):
                            continue
                        yield _mk_trade(
                            trade_id   = str(data.get("uid", data.get("time"))),
                            price      = float(data["price"]),
                            qty_native = float(data.get("qty", 0)),
                            side_norm  = side_norm,
                            ts         = data.get("time", time.time()),
                        )
                    elif feed == "trade_snapshot" and "trades" in data:
                        snapshot_trades = sorted(data["trades"], key=lambda t: t.get("time", 0))
                        for t in snapshot_trades:
                            side_raw = t.get("side")
                            if side_raw is None:
                                continue
                            side_norm = side_raw.lower()
                            if side_norm not in ("buy", "sell"):
                                continue
                            yield _mk_trade(
                                trade_id   = str(t.get("uid", t.get("time"))),
                                price      = float(t["price"]),
                                qty_native = float(t.get("qty", 0)),
                                side_norm  = side_norm,
                                ts         = t.get("time", time.time()),
                            )
                except (ValueError, TypeError, KeyError, IndexError, AttributeError) as e:
                    logger.warning(f"Kraken trades {api_symbol} malformed frame skipped: {e!r}")


    async def stream_mark_price(self, market_type: MarketType, symbol: str) -> AsyncGenerator[MarkPrice, None]:
        if market_type == MarketType.SPOT:
            raise NotImplementedError(f"{self.name} does not support stream_mark_price for {market_type.value}")
        api_symbol = self.get_api_symbol(symbol, market_type)
        model_sym  = self.get_model_symbol(api_symbol, market_type)
        info       = await self._info_for(market_type, model_sym)
        quote      = info.quote_asset if info else ""
        payload    = {"event": "subscribe", "feed": "ticker", "product_ids": [api_symbol]}

        async for data in self._ws_connect(self.futures_ws_url, payload):
            try:
                if data.get("feed") == "ticker" and "markPrice" in data:
                    mark = float(data["markPrice"])
                    idx_raw = data.get("indexPrice")
                    idx_price: Optional[float] = float(idx_raw) if idx_raw is not None else None
                    rel_raw = data.get("relative_funding_rate")
                    rel_rate: Optional[float] = float(rel_raw) if rel_raw is not None else None
                    if rel_rate is None:
                        abs_raw = data.get("funding_rate")
                        if abs_raw is not None and mark > 0:
                            abs_rate = float(abs_raw)
                            rel_rate = abs_rate / mark if market_type == MarketType.LINEAR else abs_rate * mark
                    ts = self.normalize_timestamp(data.get("time", time.time()))

                    funding_block = None
                    if rel_rate is not None:
                        funding_block = build_funding_current(
                            kind           = "continuous",
                            per_cycle      = rel_rate,
                            cycle_ms       = 3_600_000,
                            valid_until_ts = ts + 3_600_000,
                        )

                    yield MarkPrice(
                        symbol      = model_sym,
                        market_type = market_type,
                        quote       = quote,
                        mark_price  = mark,
                        index_price = idx_price,
                        funding     = funding_block,
                        timestamp   = ts,
                    )
            except (ValueError, TypeError, KeyError, IndexError, AttributeError) as e:
                logger.warning(f"Kraken mark_price {api_symbol} malformed frame skipped: {e!r}")
