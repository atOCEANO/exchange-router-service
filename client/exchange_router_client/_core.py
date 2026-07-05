import asyncio
import json
import random
from typing import Any, Dict, List, Optional

import httpx
import pandas as pd
import websockets

from . import frames
from . import rows
from ._warnings import emit
from .batch import BatchResult
from .errors import BadRequest, NotSupported, RouterError, RouterUnreachable, error_for_status


MAX_SLEEP_S = 60.0

SERIES_ROUTES = ("candles", "trades", "agg_trades", "funding_rate", "open_interest", "liquidations", "long_short_ratio")


def _detail(response: httpx.Response) -> str:
    try:
        body = response.json()
        return body.get("detail") or body.get("error") or str(body)
    except Exception:
        return response.text or f"HTTP {response.status_code}"


def _retry_after(response: httpx.Response) -> Optional[float]:
    raw = response.headers.get("Retry-After")
    if raw is None:
        return None

    try:
        return float(raw)
    except ValueError:
        return None


def _sleep_for(attempt: int, retry_after: Optional[float]) -> float:
    if retry_after is not None:
        return min(retry_after, MAX_SLEEP_S)

    base = 1.0
    return base * attempt + random.uniform(0, base)


class AsyncCore:

    def __init__(self, base_url: str = "http://localhost:8040", timeout: int = 30, max_retries: int = 3, verbose: bool = True):
        self.base_url    = base_url.rstrip("/")
        self.timeout     = timeout
        self.max_retries = max_retries
        self.verbose     = verbose
        self._http: Optional[httpx.AsyncClient] = None
        self._capabilities: Dict[str, Dict] = {}


    def _ensure_http(self) -> httpx.AsyncClient:
        if self._http is None:
            self._http = httpx.AsyncClient(
                timeout = self.timeout,
                headers = {"User-Agent": "exchange-router-client/4.0.1"},
            )

        return self._http


    async def close(self) -> None:
        if self._http is not None:
            await self._http.aclose()


    async def _request(self, method: str, endpoint: str, params: Optional[Dict] = None) -> Any:
        url     = f"{self.base_url}/{endpoint}"
        attempt = 0

        while True:
            try:
                response = await self._ensure_http().request(method, url, params=params)
                response.raise_for_status()
                return response.json()

            except httpx.HTTPStatusError as e:
                status      = e.response.status_code
                detail      = _detail(e.response)
                retry_after = _retry_after(e.response)

                if status not in (429, 500, 502, 503, 504):
                    raise error_for_status(status, detail, retry_after)

                attempt += 1
                if attempt > self.max_retries:
                    raise error_for_status(status, detail, retry_after)

                await asyncio.sleep(_sleep_for(attempt, retry_after))

            except httpx.TransportError as e:
                attempt += 1
                if attempt > self.max_retries:
                    raise RouterUnreachable(f"{type(e).__name__}: {e}")

                await asyncio.sleep(_sleep_for(attempt, None))

            except httpx.HTTPError as e:
                raise RouterUnreachable(f"{type(e).__name__}: {e}")


    async def _ensure_capabilities(self, exchange: str) -> Dict:
        if exchange not in self._capabilities:
            try:
                self._capabilities[exchange] = await self._request("GET", f"{exchange}/capabilities") or {}
            except RouterError:
                self._capabilities[exchange] = {}

        return self._capabilities[exchange]


    def _route_block(self, exchange: str, market_type: str, route: str) -> Dict:
        caps    = self._capabilities.get(exchange) or {}
        markets = caps.get("markets", {}) or {}
        mt      = markets.get(market_type) or {}
        return mt.get(route) or {}


    async def _preflight(self, exchange: str, market_type: str, route: str, symbol: Optional[str] = None,
                         interval: Optional[str] = None, interval_label: str = "Interval",
                         depth: Optional[int] = None, verbose: bool = False) -> None:
        caps  = await self._ensure_capabilities(exchange)
        block = self._route_block(exchange, market_type, route)

        if caps and block:
            if block.get("rest") is False:
                raise NotSupported(f"{route} is not exposed on {exchange}/{market_type}")

            intervals = block.get("intervals")
            if interval is not None and intervals and interval not in intervals:
                raise BadRequest(f"{interval_label} '{interval}' is not valid for {exchange} {market_type} {route}. valid: {', '.join(intervals)}")

            depths = block.get("depths")
            if depth is not None and depths and depth not in depths:
                emit([f"orderbook {exchange}/{market_type}/{symbol}: depth {depth} not in {depths}; the server snaps to the nearest"], verbose)


    def _ctx(self, exchange: str, market_type: str, symbol: str, requested: Optional[int], route: str) -> Dict[str, Any]:
        block = self._route_block(exchange, market_type, route)
        return {
            "exchange":    exchange,
            "market_type": market_type,
            "symbol":      symbol,
            "requested":   requested,
            "paginated":   bool(block.get("paginated")),
        }


    async def _series(self, route: str, exchange: str, market_type: str, symbol: str, params: Dict,
                      interval: Optional[str], interval_label: str, verbose: Optional[bool]) -> pd.DataFrame:
        v = self.verbose if verbose is None else verbose

        await self._preflight(exchange, market_type, route, symbol=symbol, interval=interval, interval_label=interval_label, verbose=v)

        rows = await self._request("GET", f"{exchange}/{market_type}/{route}/{symbol}", params)
        ctx  = self._ctx(exchange, market_type, symbol, params.get("limit"), route)

        df, warnings = frames.build(route, rows, ctx)
        emit(warnings, v)

        return df


    async def get_status(self) -> Dict:
        return await self._request("GET", "status")


    async def get_version(self) -> str:
        data = await self._request("GET", "version")
        return data.get("version", "")


    async def get_exchanges(self) -> List[str]:
        data = await self._request("GET", "exchanges")
        return data.get("exchanges", [])


    async def get_market_types(self, exchange: str) -> List[str]:
        data = await self._request("GET", f"{exchange}/market_types")
        return [str(mt) for mt in data.get("market_types", [])]


    async def get_capabilities(self, exchange: str) -> Dict:
        return await self._ensure_capabilities(exchange)


    async def get_markets(self, exchange: str, market_type: str) -> Dict:
        return await self._request("GET", f"{exchange}/{market_type}/markets")


    async def get_symbol_info(self, exchange: str, market_type: str, symbol: str) -> rows.Row:
        return rows.symbol_info_row(await self._request("GET", f"{exchange}/{market_type}/markets/{symbol}"))


    async def get_ticker(self, exchange: str, market_type: str, symbol: str, verbose: Optional[bool] = None) -> rows.Row:
        await self._preflight(exchange, market_type, "ticker", symbol=symbol)
        return rows.ticker_row(await self._request("GET", f"{exchange}/{market_type}/ticker/{symbol}"))


    async def get_book_ticker(self, exchange: str, market_type: str, symbol: str, verbose: Optional[bool] = None) -> rows.Row:
        await self._preflight(exchange, market_type, "book_ticker", symbol=symbol)
        return rows.book_ticker_row(await self._request("GET", f"{exchange}/{market_type}/book_ticker/{symbol}"))


    async def get_mark_price(self, exchange: str, market_type: str, symbol: str, verbose: Optional[bool] = None) -> rows.Row:
        v = self.verbose if verbose is None else verbose

        await self._preflight(exchange, market_type, "mark_price", symbol=symbol, verbose=v)
        data = await self._request("GET", f"{exchange}/{market_type}/mark_price/{symbol}")

        warnings = []
        if data.get("index_price") is None:
            warnings.append(f"mark_price {exchange}/{market_type}/{symbol}: index_price is null (upstream omitted it)")
        if data.get("funding") is None:
            warnings.append(f"mark_price {exchange}/{market_type}/{symbol}: funding is null (upstream omitted it)")
        emit(warnings, v)

        return rows.mark_price_row(data)


    async def get_orderbook(self, exchange: str, market_type: str, symbol: str, depth: int = 20, verbose: Optional[bool] = None) -> pd.DataFrame:
        v = self.verbose if verbose is None else verbose

        await self._preflight(exchange, market_type, "orderbook", symbol=symbol, depth=depth, verbose=v)
        data = await self._request("GET", f"{exchange}/{market_type}/orderbook/{symbol}", {"depth": depth})

        ctx = {"exchange": exchange, "market_type": market_type, "symbol": symbol}
        return frames.orderbook(data, ctx)


    async def get_candles(self, exchange: str, market_type: str, symbol: str, interval: str = "1h", limit: int = 100, start: Optional[int] = None, verbose: Optional[bool] = None) -> pd.DataFrame:
        params = {"interval": interval, "limit": limit}
        if start is not None:
            params["start"] = start

        return await self._series("candles", exchange, market_type, symbol, params, interval, "Interval", verbose)


    async def get_trades(self, exchange: str, market_type: str, symbol: str, limit: int = 100, verbose: Optional[bool] = None) -> pd.DataFrame:
        return await self._series("trades", exchange, market_type, symbol, {"limit": limit}, None, "Interval", verbose)


    async def get_agg_trades(self, exchange: str, market_type: str, symbol: str, limit: int = 500, start: Optional[int] = None, verbose: Optional[bool] = None) -> pd.DataFrame:
        params = {"limit": limit}
        if start is not None:
            params["start"] = start

        return await self._series("agg_trades", exchange, market_type, symbol, params, None, "Interval", verbose)


    async def get_funding_rate(self, exchange: str, market_type: str, symbol: str, limit: int = 100, start: Optional[int] = None, verbose: Optional[bool] = None) -> pd.DataFrame:
        params = {"limit": limit}
        if start is not None:
            params["start"] = start

        return await self._series("funding_rate", exchange, market_type, symbol, params, None, "Interval", verbose)


    async def get_open_interest(self, exchange: str, market_type: str, symbol: str, period: str = "1h", limit: int = 30, start: Optional[int] = None, verbose: Optional[bool] = None) -> pd.DataFrame:
        params = {"period": period, "limit": limit}
        if start is not None:
            params["start"] = start

        return await self._series("open_interest", exchange, market_type, symbol, params, period, "Period", verbose)


    async def get_liquidations(self, exchange: str, market_type: str, symbol: str, limit: int = 100, start: Optional[int] = None, verbose: Optional[bool] = None) -> pd.DataFrame:
        params = {"limit": limit}
        if start is not None:
            params["start"] = start

        return await self._series("liquidations", exchange, market_type, symbol, params, None, "Interval", verbose)


    async def get_long_short_ratio(self, exchange: str, market_type: str, symbol: str, period: str = "5m", limit: int = 30, start: Optional[int] = None, verbose: Optional[bool] = None) -> pd.DataFrame:
        params = {"period": period, "limit": limit}
        if start is not None:
            params["start"] = start

        return await self._series("long_short_ratio", exchange, market_type, symbol, params, period, "Period", verbose)


    async def fetch_many(self, route: str, exchange: str, market_type: str, symbols: List[str], verbose: Optional[bool] = None, max_concurrent: int = 8, **kwargs) -> BatchResult:
        if route not in SERIES_ROUTES:
            raise ValueError(f"fetch_many supports {SERIES_ROUTES}, not '{route}'")

        v         = self.verbose if verbose is None else verbose
        semaphore = asyncio.Semaphore(max_concurrent)
        method    = getattr(self, f"get_{route}")

        await self._ensure_capabilities(exchange)

        async def one(symbol: str):
            async with semaphore:
                try:
                    df = await method(exchange, market_type, symbol, verbose=False, **kwargs)
                    return symbol, df, None
                except Exception as error:
                    return symbol, None, error

        gathered = await asyncio.gather(*[one(s) for s in symbols])

        ok, degraded, failed = {}, {}, {}
        for symbol, df, error in gathered:
            if error is not None:
                failed[symbol] = error
                continue

            messages = list(df.attrs.get("warnings", []))
            if messages:
                degraded[symbol] = (df, messages)
            else:
                ok[symbol] = df

        result = BatchResult(ok, degraded, failed, len(symbols))
        emit([f"{route}_many {exchange}/{market_type}: {result.summary()}. See .report()."], v and bool(degraded or failed))

        return result


    async def subscribe(self, exchange: str, market_type: str, channel: str, symbol: str):
        ws_url = self.base_url.replace("http", "ws", 1) + f"/ws/{exchange}/{market_type}"

        async with websockets.connect(ws_url) as ws:
            await ws.send(json.dumps({"channel": channel, "symbol": symbol}))

            while True:
                message = await ws.recv()
                yield json.loads(message)


    async def stream(self, exchange: str, market_type: str, channel: str, symbol: str, reconnect: bool = True):
        while True:
            try:
                async for message in self.subscribe(exchange, market_type, channel, symbol):
                    yield message

            except (websockets.ConnectionClosed, websockets.WebSocketException, OSError):
                if not reconnect:
                    raise

                await asyncio.sleep(2)
