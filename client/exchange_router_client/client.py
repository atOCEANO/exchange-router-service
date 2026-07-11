import asyncio
import queue
import threading
from typing import Any, Dict, List, Optional

import pandas as pd

from ._core import AsyncCore
from .batch import BatchResult
from .handle import SyncMarket
from .rows import Row


STREAM_BUFFER_MAX = 1024


class _LoopThread:

    def __init__(self):
        self._loop   = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()


    def _run(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()


    def run(self, coro) -> Any:
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result()


    def iterate(self, async_gen):
        sentinel = object()
        items: "queue.Queue" = queue.Queue(maxsize=STREAM_BUFFER_MAX)
        handle: Dict[str, Any] = {}

        def offer(item):
            while True:
                try:
                    items.put_nowait(item)
                    return
                except queue.Full:
                    try:
                        items.get_nowait()
                    except queue.Empty:
                        pass

        async def pump():
            handle["task"] = asyncio.current_task()
            try:
                async for item in async_gen:
                    offer(item)
            except Exception as error:
                offer(error)
            finally:
                await async_gen.aclose()
                offer(sentinel)

        future = asyncio.run_coroutine_threadsafe(pump(), self._loop)

        try:
            while True:
                item = items.get()
                if item is sentinel:
                    break
                if isinstance(item, BaseException):
                    raise item
                yield item
        finally:
            future.cancel()
            task = handle.get("task")
            if task is not None:
                self._loop.call_soon_threadsafe(task.cancel)


    def stop(self) -> None:
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=5)


class ExchangeRouterClient:

    def __init__(self, base_url: str = "http://localhost:8040", timeout: int = 30, max_retries: int = 3, verbose: bool = True):
        self._loop = _LoopThread()
        self._core = AsyncCore(base_url=base_url, timeout=timeout, max_retries=max_retries, verbose=verbose)


    @property
    def verbose(self) -> bool:
        return self._core.verbose


    @verbose.setter
    def verbose(self, value: bool) -> None:
        self._core.verbose = value


    def close(self) -> None:
        self._loop.run(self._core.close())
        self._loop.stop()


    def __enter__(self) -> "ExchangeRouterClient":
        return self


    def __exit__(self, _exc_type, _exc_val, _exc_tb) -> None:
        self.close()


    def get_status(self) -> Dict:
        return self._loop.run(self._core.get_status())


    def get_version(self) -> str:
        return self._loop.run(self._core.get_version())


    def get_exchanges(self) -> List[str]:
        return self._loop.run(self._core.get_exchanges())


    def get_market_types(self, exchange: str) -> List[str]:
        return self._loop.run(self._core.get_market_types(exchange))


    def get_capabilities(self, exchange: str) -> Dict:
        return self._loop.run(self._core.get_capabilities(exchange))


    def get_markets(self, exchange: str, market_type: str) -> Dict:
        return self._loop.run(self._core.get_markets(exchange, market_type))


    def get_symbol_info(self, exchange: str, market_type: str, symbol: str) -> Row:
        return self._loop.run(self._core.get_symbol_info(exchange, market_type, symbol))


    def get_ticker(self, exchange: str, market_type: str, symbol: str, verbose: Optional[bool] = None) -> Row:
        return self._loop.run(self._core.get_ticker(exchange, market_type, symbol, verbose))


    def get_book_ticker(self, exchange: str, market_type: str, symbol: str, verbose: Optional[bool] = None) -> Row:
        return self._loop.run(self._core.get_book_ticker(exchange, market_type, symbol, verbose))


    def get_mark_price(self, exchange: str, market_type: str, symbol: str, verbose: Optional[bool] = None) -> Row:
        return self._loop.run(self._core.get_mark_price(exchange, market_type, symbol, verbose))


    def get_orderbook(self, exchange: str, market_type: str, symbol: str, depth: int = 20, verbose: Optional[bool] = None) -> pd.DataFrame:
        return self._loop.run(self._core.get_orderbook(exchange, market_type, symbol, depth, verbose))


    def get_candles(self, exchange: str, market_type: str, symbol: str, interval: str = "1h", limit: int = 100, start: Optional[int] = None, verbose: Optional[bool] = None) -> pd.DataFrame:
        return self._loop.run(self._core.get_candles(exchange, market_type, symbol, interval, limit, start, verbose))


    def get_trades(self, exchange: str, market_type: str, symbol: str, limit: int = 100, verbose: Optional[bool] = None) -> pd.DataFrame:
        return self._loop.run(self._core.get_trades(exchange, market_type, symbol, limit, verbose))


    def get_agg_trades(self, exchange: str, market_type: str, symbol: str, limit: int = 500, start: Optional[int] = None, verbose: Optional[bool] = None) -> pd.DataFrame:
        return self._loop.run(self._core.get_agg_trades(exchange, market_type, symbol, limit, start, verbose))


    def get_funding_rate(self, exchange: str, market_type: str, symbol: str, limit: int = 100, start: Optional[int] = None, verbose: Optional[bool] = None) -> pd.DataFrame:
        return self._loop.run(self._core.get_funding_rate(exchange, market_type, symbol, limit, start, verbose))


    def get_open_interest(self, exchange: str, market_type: str, symbol: str, period: str = "1h", limit: int = 30, start: Optional[int] = None, verbose: Optional[bool] = None) -> pd.DataFrame:
        return self._loop.run(self._core.get_open_interest(exchange, market_type, symbol, period, limit, start, verbose))


    def get_liquidations(self, exchange: str, market_type: str, symbol: str, limit: int = 100, start: Optional[int] = None, verbose: Optional[bool] = None) -> pd.DataFrame:
        return self._loop.run(self._core.get_liquidations(exchange, market_type, symbol, limit, start, verbose))


    def get_long_short_ratio(self, exchange: str, market_type: str, symbol: str, period: str = "5m", limit: int = 30, start: Optional[int] = None, verbose: Optional[bool] = None) -> pd.DataFrame:
        return self._loop.run(self._core.get_long_short_ratio(exchange, market_type, symbol, period, limit, start, verbose))


    def market(self, exchange: str, market_type: str, symbol: str) -> SyncMarket:
        return SyncMarket(self, exchange, market_type, symbol)


    def markets(self, exchange: str, market_type: str) -> List[SyncMarket]:
        data = self.get_markets(exchange, market_type)
        return [SyncMarket(self, exchange, market_type, m["symbol"]) for m in data["markets"]]


    def candles_many(self, exchange: str, market_type: str, symbols: List[str], interval: str = "1h", limit: int = 100, start: Optional[int] = None, verbose: Optional[bool] = None, max_concurrent: int = 8) -> BatchResult:
        return self._loop.run(self._core.fetch_many("candles", exchange, market_type, symbols, verbose=verbose, max_concurrent=max_concurrent, interval=interval, limit=limit, start=start))


    def trades_many(self, exchange: str, market_type: str, symbols: List[str], limit: int = 100, verbose: Optional[bool] = None, max_concurrent: int = 8) -> BatchResult:
        return self._loop.run(self._core.fetch_many("trades", exchange, market_type, symbols, verbose=verbose, max_concurrent=max_concurrent, limit=limit))


    def agg_trades_many(self, exchange: str, market_type: str, symbols: List[str], limit: int = 500, start: Optional[int] = None, verbose: Optional[bool] = None, max_concurrent: int = 8) -> BatchResult:
        return self._loop.run(self._core.fetch_many("agg_trades", exchange, market_type, symbols, verbose=verbose, max_concurrent=max_concurrent, limit=limit, start=start))


    def funding_rate_many(self, exchange: str, market_type: str, symbols: List[str], limit: int = 100, start: Optional[int] = None, verbose: Optional[bool] = None, max_concurrent: int = 8) -> BatchResult:
        return self._loop.run(self._core.fetch_many("funding_rate", exchange, market_type, symbols, verbose=verbose, max_concurrent=max_concurrent, limit=limit, start=start))


    def open_interest_many(self, exchange: str, market_type: str, symbols: List[str], period: str = "1h", limit: int = 30, start: Optional[int] = None, verbose: Optional[bool] = None, max_concurrent: int = 8) -> BatchResult:
        return self._loop.run(self._core.fetch_many("open_interest", exchange, market_type, symbols, verbose=verbose, max_concurrent=max_concurrent, period=period, limit=limit, start=start))


    def liquidations_many(self, exchange: str, market_type: str, symbols: List[str], limit: int = 100, start: Optional[int] = None, verbose: Optional[bool] = None, max_concurrent: int = 8) -> BatchResult:
        return self._loop.run(self._core.fetch_many("liquidations", exchange, market_type, symbols, verbose=verbose, max_concurrent=max_concurrent, limit=limit, start=start))


    def long_short_ratio_many(self, exchange: str, market_type: str, symbols: List[str], period: str = "5m", limit: int = 30, start: Optional[int] = None, verbose: Optional[bool] = None, max_concurrent: int = 8) -> BatchResult:
        return self._loop.run(self._core.fetch_many("long_short_ratio", exchange, market_type, symbols, verbose=verbose, max_concurrent=max_concurrent, period=period, limit=limit, start=start))


    def stream(self, exchange: str, market_type: str, channel: str, symbol: str, reconnect: bool = True):
        return self._loop.iterate(self._core.stream(exchange, market_type, channel, symbol, reconnect))


    def subscribe(self, exchange: str, market_type: str, channel: str, symbol: str):
        return self._loop.iterate(self._core.subscribe(exchange, market_type, channel, symbol))
