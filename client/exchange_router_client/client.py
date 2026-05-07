import httpx
import websockets
import json
import pandas as pd
import asyncio
import logging
from typing import Optional, List, Dict, Any

logger = logging.getLogger("ExchangeRouterClient")

class ExchangeRouterClient:

    def __init__(self, 
                 base_url: str = "http://localhost:8040",
                 timeout: int = 30,
                 max_retries: int = 3):
        
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.client = httpx.AsyncClient(timeout=timeout)

    async def close(self):
        await self.client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, _exc_type, _exc_val, _exc_tb):
        await self.close()

    async def _request(self, method: str, endpoint: str, params: Optional[Dict] = None) -> Any:
        url = f"{self.base_url}/{endpoint}"
        attempt = 0
        last_error: Optional[str] = None

        while attempt < self.max_retries:
            try:
                response = await self.client.request(method, url, params=params)
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as e:
                if 400 <= e.response.status_code < 500:
                    try:
                        detail = e.response.json().get('detail', str(e))
                    except Exception:
                        detail = str(e)
                    raise ValueError(f"Router API Error ({e.response.status_code}): {detail}")

                last_error = f"HTTP {e.response.status_code}"
                logger.warning(f"Server Error {e.response.status_code}. Retrying ({attempt+1}/{self.max_retries})...")
                attempt += 1
                await asyncio.sleep(1 * attempt)
            except httpx.TransportError as e:
                last_error = f"{type(e).__name__}: {e}"
                logger.warning(f"Transport Error: {e}. Retrying ({attempt+1}/{self.max_retries})...")
                attempt += 1
                await asyncio.sleep(1 * attempt)
            except Exception as e:
                raise RuntimeError(f"Unexpected Error: {e}")

        raise ConnectionError(f"Request to {url} failed after {self.max_retries} attempts (last error: {last_error}).")

    def _normalize_frame(self, data: List[Dict], time_col: str = "timestamp") -> pd.DataFrame:
        if not data:
            return pd.DataFrame()
        
        df = pd.DataFrame(data)
        
        if time_col in df.columns:
            df["datetime"] = pd.to_datetime(df[time_col], unit="ms")
            df.set_index("datetime", inplace=True)
            df.drop(columns=[time_col], inplace=True)
            
        df.columns = [c.lower() for c in df.columns]
        
        cols = ['open', 'high', 'low', 'close', 'volume']
        for c in cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors='coerce')
                
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
        return data.get("market_types", [])

    async def get_capabilities(self, exchange: str) -> Dict:
        return await self._request("GET", f"{exchange}/capabilities")

    async def get_markets(self, exchange: str, market_type: str) -> List[str]:
        return await self._request("GET", f"{exchange}/{market_type}/markets")

    async def get_exchange_info(self, exchange: str, market_type: str) -> pd.DataFrame:
        data = await self._request("GET", f"{exchange}/{market_type}/info")
        return pd.DataFrame(data)

    async def get_ticker(self, exchange: str, market_type: str, symbol: str) -> Dict:
        return await self._request("GET", f"{exchange}/{market_type}/ticker/{symbol}")

    async def get_book_ticker(self, exchange: str, market_type: str, symbol: str) -> Dict:
        return await self._request("GET", f"{exchange}/{market_type}/book_ticker/{symbol}")

    async def get_orderbook(self, exchange: str, market_type: str, symbol: str, depth: int = 20) -> Dict:
        return await self._request("GET", f"{exchange}/{market_type}/orderbook/{symbol}", {"depth": depth})

    async def get_mark_price(self, exchange: str, market_type: str, symbol: str) -> Dict:
        return await self._request("GET", f"{exchange}/{market_type}/mark_price/{symbol}")

    async def get_candles(self,
                    exchange: str,
                    market_type: str,
                    symbol: str,
                    interval: str = "1h",
                    limit: int = 100,
                    start: Optional[int] = None) -> pd.DataFrame:
        params = {"interval": interval, "limit": limit}
        if start is not None: params["start"] = start

        data = await self._request("GET", f"{exchange}/{market_type}/candles/{symbol}", params)
        return self._normalize_frame(data)

    async def get_trades(self, exchange: str, market_type: str, symbol: str, limit: int = 100) -> pd.DataFrame:
        params = {"limit": limit}
        data = await self._request("GET", f"{exchange}/{market_type}/trades/{symbol}", params)
        return self._normalize_frame(data)

    async def get_agg_trades(self, exchange: str, market_type: str, symbol: str, start: Optional[int] = None, limit: int = 500) -> pd.DataFrame:
        params = {"limit": limit}
        if start is not None: params["start"] = start
        data = await self._request("GET", f"{exchange}/{market_type}/agg_trades/{symbol}", params)
        return self._normalize_frame(data)

    async def get_funding_rate(self, exchange: str, market_type: str, symbol: str, start: Optional[int] = None, limit: int = 100) -> pd.DataFrame:
        params = {"limit": limit}
        if start is not None: params["start"] = start
        data = await self._request("GET", f"{exchange}/{market_type}/funding_rate/{symbol}", params)
        return self._normalize_frame(data)

    async def get_open_interest(self, exchange: str, market_type: str, symbol: str, period: str = "1h", start: Optional[int] = None, limit: int = 30) -> pd.DataFrame:
        params = {"period": period, "limit": limit}
        if start is not None: params["start"] = start
        data = await self._request("GET", f"{exchange}/{market_type}/open_interest/{symbol}", params)
        return self._normalize_frame(data)

    async def get_liquidations(self, exchange: str, market_type: str, symbol: str, start: Optional[int] = None, limit: int = 100) -> pd.DataFrame:
        params = {"limit": limit}
        if start is not None: params["start"] = start
        data = await self._request("GET", f"{exchange}/{market_type}/liquidations/{symbol}", params)
        return self._normalize_frame(data)

    async def get_long_short_ratio(self, exchange: str, market_type: str, symbol: str, period: str = "5m", start: Optional[int] = None, limit: int = 30) -> pd.DataFrame:
        params = {"period": period, "limit": limit}
        if start is not None: params["start"] = start
        data = await self._request("GET", f"{exchange}/{market_type}/long_short_ratio/{symbol}", params)
        return self._normalize_frame(data)


    async def fetch_multi_candles(self,
                            exchange: str,
                            market_type: str,
                            symbols: List[str],
                            interval: str = "1h",
                            limit: int = 1000,
                            start: Optional[int] = None,
                            max_concurrent: int = 4) -> Dict[str, pd.DataFrame]:
        results = {}
        semaphore = asyncio.Semaphore(max_concurrent)

        logger.info(f"Fetching {len(symbols)} assets from {exchange}...")

        async def _fetch(sym: str):
            async with semaphore:
                try:
                    df = await self.get_candles(exchange, market_type, sym, interval, limit, start)
                    return sym, df
                except Exception as e:
                    logger.error(f"Failed to fetch {sym}: {e}")
                    return sym, pd.DataFrame()
                    
        tasks = [_fetch(sym) for sym in symbols]
        fetched_data = await asyncio.gather(*tasks)
        
        for sym, df in fetched_data:
            if not df.empty:
                results[sym] = df
                
        return results

    async def subscribe(self, exchange: str, market_type: str, channel: str, symbol: str):
        ws_url = self.base_url.replace("http", "ws", 1) + f"/ws/{exchange}/{market_type}"

        async with websockets.connect(ws_url) as ws:
            payload = {"channel": channel, "symbol": symbol}
            await ws.send(json.dumps(payload))

            while True:
                msg = await ws.recv()
                yield json.loads(msg)