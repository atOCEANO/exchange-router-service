import httpx
import json
import asyncio
import time
import websockets
import logging
from typing import List, AsyncGenerator, Dict, Any, Optional

from src.exchanges.base import BaseExchange
from src.models import (
    Ticker, BookTicker, MarkPrice, OrderBook, Candle, Trade, AggTrade,
    MarketType, SymbolInfo, OpenInterest, FundingRate, Liquidation, LongShortRatio
)

logger = logging.getLogger("kraken_adapter")

CANDLE_INTERVALS =["1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w"]
ANALYTICS_INTERVALS =["1m", "5m", "15m", "30m", "1h", "4h", "12h", "1d"]


class KrakenAdapter(BaseExchange):
    def __init__(self):
        super().__init__()
        self.http_client = httpx.AsyncClient(timeout=30.0)

        self._backoff_until = 0.0
        self._backoff_lock = asyncio.Lock()

        self.spot_rest_url = "https://api.kraken.com/0/public"
        self.spot_ws_url = "wss://ws.kraken.com/v2"
        self.futures_rest_url = "https://futures.kraken.com/derivatives/api/v3"
        self.futures_chart_url = "https://futures.kraken.com/api/charts/v1"
        self.futures_ws_url = "wss://futures.kraken.com/ws/v1"

        self._spot_ws_map: Dict[str, str] = {}
        self._spot_ws_map_lock = asyncio.Lock()

    async def shutdown(self):
        await self.http_client.aclose()

    @property
    def name(self) -> str:
        return "kraken"

    @property
    def supported_market_types(self) -> List[MarketType]:
        return[MarketType.SPOT, MarketType.LINEAR, MarketType.INVERSE]

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
        if "/" in api_symbol: return self._to_v2_wsname(api_symbol)
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
                if "." in k: continue
                wsname = v.get("wsname")
                if not wsname: continue
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

    def get_capabilities(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "markets": {
                MarketType.SPOT: {
                    "candles":          {"rest": True,  "ws": False, "intervals": CANDLE_INTERVALS},
                    "ticker":           {"rest": True,  "ws": True},
                    "book_ticker":      {"rest": True,  "ws": True},
                    "trades":           {"rest": True,  "ws": True},
                    "agg_trades":       {"rest": False, "ws": False},
                    "orderbook":        {"rest": True,  "ws": True},
                    "mark_price":       {"rest": False, "ws": False},
                    "open_interest":    {"rest": False, "ws": False},
                    "funding_rate":     {"rest": False, "ws": False},
                    "long_short_ratio": {"rest": False, "ws": False},
                    "liquidations":     {"rest": False, "ws": False},
                },
                MarketType.LINEAR: {
                    "candles":          {"rest": True, "ws": False, "intervals": CANDLE_INTERVALS},
                    "ticker":           {"rest": True, "ws": True},
                    "book_ticker":      {"rest": True, "ws": True},
                    "trades":           {"rest": True, "ws": True},
                    "agg_trades":       {"rest": False, "ws": False},
                    "orderbook":        {"rest": True, "ws": True},
                    "mark_price":       {"rest": True, "ws": True},
                    "open_interest":    {"rest": True, "ws": False, "intervals": ANALYTICS_INTERVALS},
                    "funding_rate":     {"rest": True, "ws": False},
                    "long_short_ratio": {"rest": True, "ws": False, "intervals": ANALYTICS_INTERVALS},
                    "liquidations":     {"rest": False, "ws": False},
                },
                MarketType.INVERSE: {
                    "candles":          {"rest": True, "ws": False, "intervals": CANDLE_INTERVALS},
                    "ticker":           {"rest": True, "ws": True},
                    "book_ticker":      {"rest": True, "ws": True},
                    "trades":           {"rest": True, "ws": True},
                    "agg_trades":       {"rest": False, "ws": False},
                    "orderbook":        {"rest": True, "ws": True},
                    "mark_price":       {"rest": True, "ws": True},
                    "open_interest":    {"rest": True, "ws": False, "intervals": ANALYTICS_INTERVALS},
                    "funding_rate":     {"rest": True, "ws": False},
                    "long_short_ratio": {"rest": True, "ws": False, "intervals": ANALYTICS_INTERVALS},
                    "liquidations":     {"rest": False, "ws": False},
                },
            }
        }

    async def _make_request(self, method: str, url: str, params: Optional[Dict] = None) -> Any:
        retries = 3
        while retries > 0:
            now = time.time()
            if now < self._backoff_until:
                wait_time = self._backoff_until - now + 0.1
                await asyncio.sleep(wait_time)

            try:
                resp = await self.http_client.request(method, url, params=params)

                if resp.status_code == 200:
                    data = resp.json()
                    
                    if isinstance(data, dict):
                        if data.get("error"):
                            raise ValueError(f"Kraken Spot API Error: {data['error']}")
                        if data.get("result") == "error":
                            raise ValueError(f"Kraken Futures API Error: {data.get('errors', data)}")
                            
                        if "result" in data and isinstance(data["result"], dict) and not url.startswith(self.futures_chart_url):
                            return data["result"]
                    return data

                if resp.status_code in[429, 418, 502, 503]:
                    retry_after = int(resp.headers.get("Retry-After", 5))
                    async with self._backoff_lock:
                        self._backoff_until = max(self._backoff_until, time.time() + retry_after)
                    
                    logger.error(f"Rate Limit/Server Error ({resp.status_code}). Backoff set for {retry_after}s.")
                    retries -= 1
                    continue

                resp.raise_for_status()

            except httpx.HTTPStatusError as e:
                raise ValueError(f"Kraken API Error ({e.response.status_code}) on {url}: {e.response.text}")
            except Exception as e:
                if isinstance(e, ValueError): raise e
                logger.error(f"Request Error on {url}: {e}")
                retries -= 1
                await asyncio.sleep(1)

        raise Exception(f"Max retries exceeded for {url}")

    async def get_exchange_info(self, market_type: MarketType) -> List[SymbolInfo]:
        results =[]
        if market_type == MarketType.SPOT:
            data = await self._make_request("GET", f"{self.spot_rest_url}/AssetPairs")
            for k, v in data.items():
                if "." in k: continue
                wsname = v.get("wsname", "/")
                base, quote = wsname.split("/") if "/" in wsname else (v.get("base", ""), v.get("quote", ""))
                
                results.append(SymbolInfo(
                    symbol=self.get_model_symbol(v.get("altname", k), market_type),
                    native_symbol=v.get("altname", k),
                    base_asset=base,
                    quote_asset=quote,
                    price_precision=int(v.get("pair_decimals", 8)),
                    quantity_precision=int(v.get("lot_decimals", 8)),
                    min_qty=float(v.get("ordermin", 0)), max_qty=0.0, min_notional=0.0,
                    status="TRADING" if v.get("status") == "online" else "OFFLINE"
                ))
        else:
            data = await self._make_request("GET", f"{self.futures_rest_url}/instruments")
            for v in data.get("instruments",[]):
                t = v.get("type", "")
                sym = v.get("symbol", "")
                if market_type == MarketType.INVERSE and (t != "futures_inverse" or not sym.startswith("PI_")): continue
                if market_type == MarketType.LINEAR and (t != "flexible_futures" or not sym.startswith("PF_")): continue
                
                results.append(SymbolInfo(
                    symbol=self.get_model_symbol(v["symbol"], market_type),
                    native_symbol=v["symbol"],
                    base_asset=v.get("baseCurrency", ""),
                    quote_asset=v.get("quoteCurrency", ""),
                    price_precision=8, quantity_precision=8, min_qty=0.0, max_qty=0.0, min_notional=0.0,
                    status="TRADING" if v.get("tradeable") else "OFFLINE"
                ))
        return results

    async def get_markets(self, market_type: MarketType) -> List[str]:
        info = await self.get_exchange_info(market_type)
        return[s.symbol for s in info]

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
                timestamp=int(time.time() * 1000)
            )
        else:
            data = await self._make_request("GET", f"{self.futures_rest_url}/tickers")
            for t in data.get("tickers", []):
                if t["symbol"] == api_symbol:
                    last = float(t.get("last", 0))
                    vol = float(t.get("vol24h", 0))
                    open_24h = float(t.get("open24h", last))
                    high_24h = float(t.get("high24h", last))
                    low_24h = float(t.get("low24h", last))
                    q_vol = float(t.get("volumeQuote", 0))
                    
                    pc = 0.0
                    if open_24h > 0:
                        pc = ((last - open_24h) / open_24h) * 100

                    return Ticker(
                        symbol=self.get_model_symbol(api_symbol, market_type),
                        price=last,
                        open_24h=open_24h, high_24h=high_24h, low_24h=low_24h,
                        volume_24h=vol, quote_volume_24h=q_vol,
                        price_change_percent=pc,
                        timestamp=self.normalize_timestamp(t.get("lastTime", time.time()))
                    )
            raise ValueError(f"Symbol {api_symbol} not found")

    async def get_book_ticker(self, market_type: MarketType, symbol: str) -> BookTicker:
        api_symbol = self.get_api_symbol(symbol, market_type)
        if market_type == MarketType.SPOT:
            data = await self._make_request("GET", f"{self.spot_rest_url}/Depth", {"pair": api_symbol, "count": 1})
            pair_key = list(data.keys())[0]
            bids = data[pair_key].get("bids", [])
            asks = data[pair_key].get("asks",[])
            
            return BookTicker(
                symbol=self.get_model_symbol(api_symbol, market_type),
                bid_price=float(bids[0][0]) if bids else 0.0,
                bid_qty=float(bids[0][1]) if bids else 0.0,
                ask_price=float(asks[0][0]) if asks else 0.0,
                ask_qty=float(asks[0][1]) if asks else 0.0,
                timestamp=self.normalize_timestamp(bids[0][2] if bids else time.time())
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
                        timestamp=self.normalize_timestamp(t.get("lastTime", time.time()))
                    )
            raise ValueError(f"Symbol {api_symbol} not found")

    async def get_mark_price(self, market_type: MarketType, symbol: str) -> MarkPrice:
        if market_type == MarketType.SPOT: raise NotImplementedError()
        api_symbol = self.get_api_symbol(symbol, market_type)
        data = await self._make_request("GET", f"{self.futures_rest_url}/tickers")
        
        for t in data.get("tickers",[]):
            if t["symbol"] == api_symbol:
                mark = float(t.get("markPrice", 0))
                return MarkPrice(
                    symbol=self.get_model_symbol(api_symbol, market_type),
                    mark_price=mark,
                    index_price=float(t.get("indexPrice", mark)),
                    funding_rate=float(t.get("fundingRate", 0)),
                    next_funding_time=0,
                    timestamp=self.normalize_timestamp(t.get("lastTime", time.time()))
                )
        raise ValueError(f"Symbol {api_symbol} not found")

    async def get_orderbook(self, market_type: MarketType, symbol: str, depth: int = 20) -> OrderBook:
        api_symbol = self.get_api_symbol(symbol, market_type)
        if market_type == MarketType.SPOT:
            data = await self._make_request("GET", f"{self.spot_rest_url}/Depth", {"pair": api_symbol, "count": depth})
            pair_key = list(data.keys())[0]
            book = data[pair_key]
            ts = float(book["bids"][0][2]) if book.get("bids") else time.time()
            return OrderBook(
                symbol=self.get_model_symbol(api_symbol, market_type),
                bids=[[float(p), float(q)] for p, q, _ in book.get("bids",[])],
                asks=[[float(p), float(q)] for p, q, _ in book.get("asks",[])],
                timestamp=self.normalize_timestamp(ts)
            )
        else:
            data = await self._make_request("GET", f"{self.futures_rest_url}/orderbook", {"symbol": api_symbol})
            book = data.get("orderBook", {})
            return OrderBook(
                symbol=self.get_model_symbol(api_symbol, market_type),
                bids=[[float(p), float(q)] for p, q in book.get("bids", [])[:depth]],
                asks=[[float(p), float(q)] for p, q in book.get("asks", [])[:depth]],
                timestamp=int(time.time() * 1000)
            )

    async def get_trades(self, market_type: MarketType, symbol: str, limit: int = 100) -> List[Trade]:
        api_symbol = self.get_api_symbol(symbol, market_type)
        if market_type == MarketType.SPOT:
            data = await self._make_request("GET", f"{self.spot_rest_url}/Trades", {"pair": api_symbol})
            pair_key =[k for k in data.keys() if k != "last"][0]
            trades = data[pair_key][-limit:]
            return[Trade(
                id=str(int(t[2] * 1000000)) + str(i),
                price=float(t[0]),
                qty=float(t[1]),
                side="buy" if t[3] == "b" else "sell",
                timestamp=self.normalize_timestamp(t[2])
            ) for i, t in enumerate(trades)]
        else:
            data = await self._make_request("GET", f"{self.futures_rest_url}/history", {"symbol": api_symbol})
            trades = data.get("history", [])[:limit]
            return [Trade(
                id=str(t.get("trade_id", t["time"])),
                price=float(t["price"]),
                qty=float(t["size"]),
                side=t.get("side", "buy").lower(),
                timestamp=self.normalize_timestamp(t["time"])
            ) for t in trades]

    async def get_agg_trades(self, market_type: MarketType, symbol: str, start_time: Optional[int] = None, limit: int = 500) -> List[AggTrade]:
        raise NotImplementedError("Kraken does not support aggregated trades.")

    async def get_candles(self, market_type: MarketType, symbol: str, interval: str, start_time: Optional[int] = None, limit: int = 100) -> List[Candle]:
        api_symbol = self.get_api_symbol(symbol, market_type)
        if market_type == MarketType.SPOT:
            curr_start = int(start_time / 1000) if start_time else None
            params = {"pair": api_symbol, "interval": self._map_spot_interval(interval)}
            if curr_start: params["since"] = curr_start
            
            data = await self._make_request("GET", f"{self.spot_rest_url}/OHLC", params)
            pair_key =[k for k in data.keys() if k != "last"][0]
            results = []
            
            for b in data[pair_key]:
                results.append(Candle(
                    timestamp=self.normalize_timestamp(b[0]),
                    open=float(b[1]), high=float(b[2]), low=float(b[3]), close=float(b[4]),
                    volume=float(b[6])
                ))
            return results[-limit:]
        else:
            interval_secs_map = {"1m": 60, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600,
                                 "4h": 14400, "12h": 43200, "1d": 86400, "1w": 604800}
            interval_secs = interval_secs_map.get(interval, 3600)
            from_ts = int(start_time / 1000) if start_time else int(time.time()) - limit * interval_secs
            url = f"{self.futures_chart_url}/trade/{api_symbol}/{interval}"
            params = {"from": from_ts}
            data = await self._make_request("GET", url, params)

            results = [Candle(
                timestamp=self.normalize_timestamp(b["time"]),
                open=float(b["open"]), high=float(b["high"]), low=float(b["low"]), close=float(b["close"]),
                volume=float(b["volume"])
            ) for b in data.get("candles", [])]
            return results[-limit:]

    def _extract_analytics(self, data: Any, field_name: str) -> List[Dict]:
        results: List[Dict] = []
        if not isinstance(data, dict):
            logger.warning(f"Kraken analytics: expected dict, got {type(data).__name__}")
            return results
        res = data.get("result")
        if not isinstance(res, dict):
            logger.warning(f"Kraken analytics: missing 'result' dict. Top-level keys: {list(data.keys())}")
            return results
        timestamps = res.get("timestamp")
        payload = res.get("data")
        if not isinstance(timestamps, list) or not isinstance(payload, dict):
            logger.warning(f"Kraken analytics: unexpected result shape. result keys: {list(res.keys())}")
            return results
        values = payload.get(field_name)
        if not isinstance(values, list):
            logger.warning(
                f"Kraken analytics: field '{field_name}' missing from data. "
                f"Available keys: {list(payload.keys())}"
            )
            return results
        for t, v in zip(timestamps, values):
            if v is not None:
                results.append({"time": t, "value": v})
        return results

    async def get_open_interest(self, market_type: MarketType, symbol: str, period: str = "1h", start_time: Optional[int] = None, limit: int = 30) -> List[OpenInterest]:
        if market_type == MarketType.SPOT: raise NotImplementedError()
        api_symbol = self.get_api_symbol(symbol, market_type)
        
        interval_secs = self._map_analytics_interval(period)
        url = f"{self.futures_chart_url}/analytics/{api_symbol}/open-interest"
        
        calc_start = int(start_time / 1000) if start_time else int(time.time()) - (limit * interval_secs)
        params = {"interval": interval_secs, "since": calc_start}

        data = await self._make_request("GET", url, params)
        elements = self._extract_analytics(data, "openInterest")
        
        results =[OpenInterest(
            symbol=self.get_model_symbol(api_symbol, market_type),
            open_interest=float(e["value"]),
            value_usd=0.0,
            timestamp=self.normalize_timestamp(e["time"])
        ) for e in elements]
        results.sort(key=lambda x: x.timestamp)
        return results[-limit:]

    async def get_long_short_ratio(self, market_type: MarketType, symbol: str, period: str = "5m", start_time: Optional[int] = None, limit: int = 30) -> List[LongShortRatio]:
        if market_type == MarketType.SPOT: raise NotImplementedError()
        api_symbol = self.get_api_symbol(symbol, market_type)
        
        interval_secs = self._map_analytics_interval(period)
        url = f"{self.futures_chart_url}/analytics/{api_symbol}/long-short-ratio"
        
        calc_start = int(start_time / 1000) if start_time else int(time.time()) - (limit * interval_secs)
        params = {"interval": interval_secs, "since": calc_start}

        data = await self._make_request("GET", url, params)
        elements = self._extract_analytics(data, "ratio")
        
        results =[LongShortRatio(
            symbol=self.get_model_symbol(api_symbol, market_type),
            ratio=float(e["value"]),
            long_account=0.0, 
            short_account=0.0,
            timestamp=self.normalize_timestamp(e["time"])
        ) for e in elements]
        results.sort(key=lambda x: x.timestamp)
        return results[-limit:]

    async def get_funding_rate(self, market_type: MarketType, symbol: str, start_time: Optional[int] = None, limit: int = 100) -> List[FundingRate]:
        if market_type == MarketType.SPOT: raise NotImplementedError()
        api_symbol = self.get_api_symbol(symbol, market_type)
        
        data = await self._make_request("GET", f"{self.futures_rest_url}/historical-funding-rates", {"symbol": api_symbol})
        rates = data.get("rates", [])
        
        results =[]
        for r in rates:
            ts = self.normalize_timestamp(r.get("timestamp"))
            if start_time and ts < start_time: continue
            results.append(FundingRate(
                symbol=self.get_model_symbol(api_symbol, market_type),
                rate=float(r.get("fundingRate", 0)),
                timestamp=ts
            ))
        results.sort(key=lambda x: x.timestamp)
        return results[-limit:]

    async def get_liquidations(self, market_type: MarketType, symbol: str, start_time: Optional[int] = None, limit: int = 100) -> List[Liquidation]:
        raise NotImplementedError("Kraken only provides aggregated liquidation volume, not individual precise liquidations.")

    async def _ws_connect(self, url: str, payload: dict) -> AsyncGenerator[Dict, None]:
        reconnect_delay = 1
        while True:
            try:
                async with websockets.connect(url) as ws:
                    await ws.send(json.dumps(payload))
                    reconnect_delay = 1
                    async for msg in ws:
                        data = json.loads(msg)
                        if data.get("method") == "subscribe" and data.get("success") is False:
                            err = data.get("error") or data.get("result") or data
                            logger.error(f"Kraken spot WS subscribe failed for {payload}: {err}")
                            raise RuntimeError(f"Kraken spot WS subscribe rejected: {err}")
                        if data.get("event") in ("alert", "error"):
                            msg_text = data.get("message") or data
                            logger.error(f"Kraken futures WS error for {payload}: {msg_text}")
                            raise RuntimeError(f"Kraken futures WS error: {msg_text}")
                        yield data
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Kraken WS Disconnected ({url}): {e}. Reconnecting in {reconnect_delay}s...")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 30)

    async def stream_ticker(self, market_type: MarketType, symbol: str) -> AsyncGenerator[Ticker, None]:
        api_symbol = self.get_api_symbol(symbol, market_type)
        if market_type == MarketType.SPOT:
            ws_sym = await self._spot_ws_symbol(api_symbol)
            payload = {"method": "subscribe", "params": {"channel": "ticker", "symbol": [ws_sym]}}
            async for data in self._ws_connect(self.spot_ws_url, payload):
                if data.get("type") in ["snapshot", "update"] and data.get("channel") == "ticker":
                    for item in data.get("data",[]):
                        last = float(item.get("last", 0))
                        yield Ticker(
                            symbol=self.get_model_symbol(api_symbol, market_type),
                            price=last, open_24h=float(item.get("open", last)),
                            high_24h=float(item.get("high", last)), low_24h=float(item.get("low", last)),
                            volume_24h=float(item.get("volume", 0)), 
                            quote_volume_24h=float(item.get("vwap", 0)) * float(item.get("volume", 0)),
                            price_change_percent=float(item.get("change_pct", 0)), 
                            timestamp=self.normalize_timestamp(item.get("timestamp", time.time()))
                        )
        else:
            payload = {"event": "subscribe", "feed": "ticker", "product_ids": [api_symbol]}
            async for data in self._ws_connect(self.futures_ws_url, payload):
                if data.get("feed") == "ticker" and "last" in data:
                    last = float(data["last"])
                    yield Ticker(
                        symbol=self.get_model_symbol(api_symbol, market_type),
                        price=last, open_24h=last, high_24h=last, low_24h=last,
                        volume_24h=float(data.get("vol24h", 0)), quote_volume_24h=0.0,
                        price_change_percent=float(data.get("funding_rate", 0)), 
                        timestamp=self.normalize_timestamp(data.get("time", time.time()))
                    )

    async def stream_book_ticker(self, market_type: MarketType, symbol: str) -> AsyncGenerator[BookTicker, None]:
        api_symbol = self.get_api_symbol(symbol, market_type)
        if market_type == MarketType.SPOT:
            ws_sym = await self._spot_ws_symbol(api_symbol)
            payload = {"method": "subscribe", "params": {"channel": "ticker", "symbol": [ws_sym], "event_trigger": "bbo"}}
            async for data in self._ws_connect(self.spot_ws_url, payload):
                if data.get("type") in ["snapshot", "update"] and data.get("channel") == "ticker":
                    for item in data.get("data",[]):
                        yield BookTicker(
                            symbol=self.get_model_symbol(api_symbol, market_type),
                            bid_price=float(item.get("bid", 0)), bid_qty=float(item.get("bid_qty", 0)),
                            ask_price=float(item.get("ask", 0)), ask_qty=float(item.get("ask_qty", 0)),
                            timestamp=self.normalize_timestamp(item.get("timestamp", time.time()))
                        )
        else:
            payload = {"event": "subscribe", "feed": "ticker", "product_ids":[api_symbol]}
            async for data in self._ws_connect(self.futures_ws_url, payload):
                if data.get("feed") == "ticker" and "bid" in data:
                    yield BookTicker(
                        symbol=self.get_model_symbol(api_symbol, market_type),
                        bid_price=float(data.get("bid", 0)), bid_qty=float(data.get("bid_size", 0)),
                        ask_price=float(data.get("ask", 0)), ask_qty=float(data.get("ask_size", 0)),
                        timestamp=self.normalize_timestamp(data.get("time", time.time()))
                    )

    async def stream_mark_price(self, market_type: MarketType, symbol: str) -> AsyncGenerator[MarkPrice, None]:
        if market_type == MarketType.SPOT: raise NotImplementedError()
        api_symbol = self.get_api_symbol(symbol, market_type)
        payload = {"event": "subscribe", "feed": "ticker", "product_ids": [api_symbol]}
        async for data in self._ws_connect(self.futures_ws_url, payload):
            if data.get("feed") == "ticker" and "markPrice" in data:
                mark = float(data["markPrice"])
                yield MarkPrice(
                    symbol=self.get_model_symbol(api_symbol, market_type),
                    mark_price=mark, index_price=float(data.get("indexPrice", mark)),
                    funding_rate=float(data.get("funding_rate", 0)), next_funding_time=0,
                    timestamp=self.normalize_timestamp(data.get("time", time.time()))
                )

    async def stream_trades(self, market_type: MarketType, symbol: str) -> AsyncGenerator[Trade, None]:
        api_symbol = self.get_api_symbol(symbol, market_type)
        if market_type == MarketType.SPOT:
            ws_sym = await self._spot_ws_symbol(api_symbol)
            payload = {"method": "subscribe", "params": {"channel": "trade", "symbol": [ws_sym]}}
            async for data in self._ws_connect(self.spot_ws_url, payload):
                if data.get("type") in ["snapshot", "update"] and data.get("channel") == "trade":
                    for t in data.get("data",[]):
                        yield Trade(
                            id=str(t.get("trade_id", t.get("timestamp", time.time()))),
                            price=float(t.get("price", 0)), qty=float(t.get("qty", 0)),
                            side=t.get("side", "buy").lower(), 
                            timestamp=self.normalize_timestamp(t.get("timestamp", time.time()))
                        )
        else:
            payload = {"event": "subscribe", "feed": "trade", "product_ids": [api_symbol]}
            async for data in self._ws_connect(self.futures_ws_url, payload):
                feed = data.get("feed")
                if feed == "trade" and "price" in data:
                    yield Trade(
                        id=str(data.get("uid", data.get("time"))),
                        price=float(data["price"]), qty=float(data.get("qty", 0)),
                        side=data.get("side", "buy").lower(),
                        timestamp=self.normalize_timestamp(data.get("time", time.time()))
                    )
                elif feed == "trade_snapshot" and "trades" in data:
                    for t in data["trades"]:
                        yield Trade(
                            id=str(t.get("uid", t.get("time"))),
                            price=float(t["price"]), qty=float(t.get("qty", 0)),
                            side=t.get("side", "buy").lower(),
                            timestamp=self.normalize_timestamp(t.get("time", time.time()))
                        )

    async def stream_orderbook(self, market_type: MarketType, symbol: str, depth: int = 20, update_speed: str = "100ms") -> AsyncGenerator[OrderBook, None]:
        api_symbol = self.get_api_symbol(symbol, market_type)
        if market_type == MarketType.SPOT:
            ws_sym = await self._spot_ws_symbol(api_symbol)
            req_depth = 10 if depth <= 10 else 25 if depth <= 25 else 100
            payload = {"method": "subscribe", "params": {"channel": "book", "symbol": [ws_sym], "depth": req_depth}}
            async for data in self._ws_connect(self.spot_ws_url, payload):
                if data.get("channel") == "book" and data.get("type") in ["snapshot", "update"]:
                    for item in data.get("data",[]):
                        yield OrderBook(
                            symbol=self.get_model_symbol(api_symbol, market_type),
                            bids=[[float(b["price"]), float(b["qty"])] for b in item.get("bids", [])],
                            asks=[[float(a["price"]), float(a["qty"])] for a in item.get("asks",[])],
                            timestamp=self.normalize_timestamp(item.get("timestamp", time.time()))
                        )
        else:
            payload = {"event": "subscribe", "feed": "book", "product_ids": [api_symbol]}
            async for data in self._ws_connect(self.futures_ws_url, payload):
                if data.get("feed") in ["book", "book_snapshot"] and "bids" in data:
                    yield OrderBook(
                        symbol=self.get_model_symbol(api_symbol, market_type),
                        bids=[[float(b["price"]), float(b["qty"])] for b in data.get("bids", [])[:depth]],
                        asks=[[float(a["price"]), float(a["qty"])] for a in data.get("asks", [])[:depth]],
                        timestamp=self.normalize_timestamp(data.get("timestamp", time.time()))
                    )