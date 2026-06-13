from typing import List, Optional

from ._core import AsyncCore
from .batch import BatchResult
from .handle import AsyncMarket


class AsyncExchangeRouterClient(AsyncCore):

    def market(self, exchange: str, market_type: str, symbol: str) -> AsyncMarket:
        return AsyncMarket(self, exchange, market_type, symbol)


    async def markets(self, exchange: str, market_type: str) -> List[AsyncMarket]:
        data = await self.get_markets(exchange, market_type)
        return [AsyncMarket(self, exchange, market_type, m["symbol"]) for m in data["markets"]]


    async def candles_many(self, exchange: str, market_type: str, symbols: List[str], interval: str = "1h", limit: int = 100, start: Optional[int] = None, verbose: Optional[bool] = None, max_concurrent: int = 8) -> BatchResult:
        return await self.fetch_many("candles", exchange, market_type, symbols, verbose=verbose, max_concurrent=max_concurrent, interval=interval, limit=limit, start=start)


    async def trades_many(self, exchange: str, market_type: str, symbols: List[str], limit: int = 100, verbose: Optional[bool] = None, max_concurrent: int = 8) -> BatchResult:
        return await self.fetch_many("trades", exchange, market_type, symbols, verbose=verbose, max_concurrent=max_concurrent, limit=limit)


    async def agg_trades_many(self, exchange: str, market_type: str, symbols: List[str], limit: int = 500, start: Optional[int] = None, verbose: Optional[bool] = None, max_concurrent: int = 8) -> BatchResult:
        return await self.fetch_many("agg_trades", exchange, market_type, symbols, verbose=verbose, max_concurrent=max_concurrent, limit=limit, start=start)


    async def funding_rate_many(self, exchange: str, market_type: str, symbols: List[str], limit: int = 100, start: Optional[int] = None, verbose: Optional[bool] = None, max_concurrent: int = 8) -> BatchResult:
        return await self.fetch_many("funding_rate", exchange, market_type, symbols, verbose=verbose, max_concurrent=max_concurrent, limit=limit, start=start)


    async def open_interest_many(self, exchange: str, market_type: str, symbols: List[str], period: str = "1h", limit: int = 30, start: Optional[int] = None, verbose: Optional[bool] = None, max_concurrent: int = 8) -> BatchResult:
        return await self.fetch_many("open_interest", exchange, market_type, symbols, verbose=verbose, max_concurrent=max_concurrent, period=period, limit=limit, start=start)


    async def liquidations_many(self, exchange: str, market_type: str, symbols: List[str], limit: int = 100, start: Optional[int] = None, verbose: Optional[bool] = None, max_concurrent: int = 8) -> BatchResult:
        return await self.fetch_many("liquidations", exchange, market_type, symbols, verbose=verbose, max_concurrent=max_concurrent, limit=limit, start=start)


    async def long_short_ratio_many(self, exchange: str, market_type: str, symbols: List[str], period: str = "5m", limit: int = 30, start: Optional[int] = None, verbose: Optional[bool] = None, max_concurrent: int = 8) -> BatchResult:
        return await self.fetch_many("long_short_ratio", exchange, market_type, symbols, verbose=verbose, max_concurrent=max_concurrent, period=period, limit=limit, start=start)


    async def __aenter__(self) -> "AsyncExchangeRouterClient":
        return self


    async def __aexit__(self, _exc_type, _exc_val, _exc_tb) -> None:
        await self.close()
