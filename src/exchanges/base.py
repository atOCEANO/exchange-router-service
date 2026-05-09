import asyncio
import logging
import orjson
from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any, Union, AsyncGenerator, Callable, Awaitable, Iterable
from datetime import datetime, timezone
from src.models import (
    Ticker, BookTicker, MarkPrice, OrderBook, Candle, Trade, AggTrade,
    MarketType, SymbolInfo, FundingRate, OpenInterest, Liquidation, LongShortRatio
)


class StreamHub:
    """Single upstream WebSocket multiplexed across many topics.

    One persistent connection per hub. Subscribers acquire a per-topic queue
    via `subscribe(topic)` and consume messages until they call `unsubscribe`.
    The hub sends SUBSCRIBE upstream when a topic's refcount goes 0->1 and
    UNSUBSCRIBE when it goes 1->0; on reconnect it re-subscribes every topic
    that still has subscribers.

    Adapters supply five callbacks:
        connect()              -> coroutine returning a connected websocket
        subscribe_payload(ts)  -> iterable of payloads to send to subscribe
        unsubscribe_payload(ts) -> iterable of payloads to send to unsubscribe
        route(message)         -> topic key for the message, or None to drop
        keepalive_payload      -> payload to send periodically (None to disable)

    Payloads are sent as JSON if they are dicts/lists, or raw text if they
    are strings (for OKX's "ping").
    """


    def __init__(self,
                 name: str,
                 connect: Callable[[], Awaitable[Any]],
                 subscribe_payload: Callable[[List[str]], Iterable[Any]],
                 unsubscribe_payload: Callable[[List[str]], Iterable[Any]],
                 route: Callable[[Any], Optional[str]],
                 keepalive_payload: Optional[Any] = None,
                 keepalive_interval: float = 0.0,
                 queue_size: int = 1024):
        self.name = name
        self._connect            = connect
        self._sub_payload        = subscribe_payload
        self._unsub_payload      = unsubscribe_payload
        self._route              = route
        self._keepalive_payload  = keepalive_payload
        self._keepalive_interval = keepalive_interval
        self._queue_size         = queue_size

        self._queues: Dict[str, List[asyncio.Queue]] = {}
        self._lock      = asyncio.Lock()
        self._connected = asyncio.Event()
        self._task: Optional[asyncio.Task] = None
        self._ws        = None
        self._closed    = False
        self._log       = logging.getLogger(name)


    async def subscribe(self, topic: str) -> asyncio.Queue:
        q = asyncio.Queue(maxsize=self._queue_size)
        is_new_topic = False
        async with self._lock:
            if topic not in self._queues:
                self._queues[topic] = []
                is_new_topic = True
            self._queues[topic].append(q)
            if self._task is None or self._task.done():
                self._closed = False
                self._task = asyncio.create_task(self._run())

        if is_new_topic:
            try:
                await asyncio.wait_for(self._connected.wait(), timeout=30)
            except asyncio.TimeoutError:
                self._log.warning(f"subscribe({topic}) timed out waiting for connection; will pick up on reconnect")
                return q
            await self._send_payloads(self._sub_payload([topic]))
        return q


    async def unsubscribe(self, topic: str, q: asyncio.Queue) -> None:
        drop_topic = False
        async with self._lock:
            qs = self._queues.get(topic) or []
            qs = [x for x in qs if x is not q]
            if qs:
                self._queues[topic] = qs
            else:
                self._queues.pop(topic, None)
                drop_topic = True

        if drop_topic and self._connected.is_set():
            await self._send_payloads(self._unsub_payload([topic]))


    async def close(self) -> None:
        self._closed = True
        task = self._task
        self._task = None
        if task is not None:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass


    async def _send_payloads(self, payloads: Iterable[Any]) -> None:
        ws = self._ws
        if ws is None:
            return
        for payload in payloads:
            try:
                if isinstance(payload, (str, bytes)):
                    await ws.send(payload)
                else:
                    await ws.send(orjson.dumps(payload).decode())
            except Exception as e:
                self._log.warning(f"send failed: {e}")
                return


    async def _keepalive_loop(self, ws: Any) -> None:
        try:
            while True:
                await asyncio.sleep(self._keepalive_interval)
                if self._keepalive_payload is None:
                    continue
                if isinstance(self._keepalive_payload, (str, bytes)):
                    await ws.send(self._keepalive_payload)
                else:
                    await ws.send(orjson.dumps(self._keepalive_payload).decode())
        except (asyncio.CancelledError, Exception):
            return


    @staticmethod
    def _parse(raw: Any) -> Any:
        if isinstance(raw, (bytes, bytearray)):
            try:
                return orjson.loads(raw)
            except orjson.JSONDecodeError:
                return None
        if isinstance(raw, str):
            try:
                return orjson.loads(raw)
            except orjson.JSONDecodeError:
                return None
        return raw


    async def _run(self) -> None:
        reconnect_delay = 1
        while not self._closed:
            ka_task: Optional[asyncio.Task] = None
            try:
                self._log.info("connecting upstream WS")
                ws = await self._connect()
                self._ws = ws

                async with self._lock:
                    topics = list(self._queues.keys())
                if topics:
                    await self._send_payloads(self._sub_payload(topics))

                self._connected.set()
                reconnect_delay = 1

                if self._keepalive_payload is not None and self._keepalive_interval > 0:
                    ka_task = asyncio.create_task(self._keepalive_loop(ws))

                async for raw in ws:
                    msg = self._parse(raw)
                    if msg is None:
                        continue
                    topic = self._route(msg)
                    if topic is None:
                        continue
                    queues = self._queues.get(topic) or []
                    for q in queues:
                        try:
                            q.put_nowait(msg)
                        except asyncio.QueueFull:
                            pass

            except asyncio.CancelledError:
                break
            except Exception as e:
                self._log.warning(f"upstream WS disconnected ({e}). Reconnecting in {reconnect_delay}s...")
            finally:
                self._connected.clear()
                self._ws = None
                if ka_task is not None:
                    ka_task.cancel()

            if not self._closed:
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 30)







class BaseExchange(ABC):
    def __init__(self):
        pass


    @abstractmethod
    async def shutdown(self):
        pass


    def normalize_symbol(self, symbol: str) -> str:
        return symbol.replace("/", "").replace("-", "").upper()


    def normalize_timestamp(self, ts: Union[int, float, str]) -> int:
        if isinstance(ts, str):
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                return int(dt.timestamp() * 1000)
            except ValueError:
                try:
                    ts = float(ts)
                except ValueError:
                    raise ValueError(f"Could not parse timestamp string: {ts}")
        if isinstance(ts, (int, float)):
            if ts < 100_000_000_000:
                return int(ts * 1000)
            return int(ts)
        raise ValueError(f"Unsupported timestamp format: {type(ts)}")


    def normalize_interval(self, interval: str) -> str:
        return interval


    @property
    @abstractmethod
    def name(self) -> str:
        pass


    @property
    @abstractmethod
    def supported_market_types(self) -> List[MarketType]:
        pass


    @abstractmethod
    def get_capabilities(self) -> Dict[str, Any]:
        pass


    async def get_status(self) -> Dict[str, Any]:
        return {"status": "online", "exchange": self.name}


    @abstractmethod
    async def get_exchange_info(self, market_type: MarketType) -> List[SymbolInfo]:
        pass


    @abstractmethod
    async def get_markets(self, market_type: MarketType) -> List[str]:
        pass


    @abstractmethod
    async def get_ticker(self, market_type: MarketType, symbol: str) -> Ticker:
        pass


    @abstractmethod
    async def get_book_ticker(self, market_type: MarketType, symbol: str) -> BookTicker:
        pass


    async def get_mark_price(self, market_type: MarketType, symbol: str) -> MarkPrice:
        raise NotImplementedError(f"Mark Price not supported on {self.name} {market_type}")


    @abstractmethod
    async def get_orderbook(self, market_type: MarketType, symbol: str, depth: int = 20) -> OrderBook:
        pass


    @abstractmethod
    async def get_trades(self, market_type: MarketType, symbol: str, limit: int = 100) -> List[Trade]:
        pass


    @abstractmethod
    async def get_agg_trades(self, market_type: MarketType, symbol: str, start_time: Optional[int] = None, limit: int = 500) -> List[AggTrade]:
        pass


    @abstractmethod
    async def get_candles(self, market_type: MarketType, symbol: str, interval: str, start_time: Optional[int] = None, limit: int = 100) -> List[Candle]:
        pass


    @abstractmethod
    async def stream_ticker(self, market_type: MarketType, symbol: str) -> AsyncGenerator[Ticker, None]:
        pass


    @abstractmethod
    async def stream_book_ticker(self, market_type: MarketType, symbol: str) -> AsyncGenerator[BookTicker, None]:
        pass


    async def stream_mark_price(self, market_type: MarketType, symbol: str) -> AsyncGenerator[MarkPrice, None]:
        if False: yield
        raise NotImplementedError(f"Mark Price stream not supported on {self.name} {market_type}")


    async def stream_agg_trades(self, market_type: MarketType, symbol: str) -> AsyncGenerator[AggTrade, None]:
        if False: yield
        raise NotImplementedError(f"AggTrade stream not supported on {self.name} {market_type}")


    @abstractmethod
    async def stream_trades(self, market_type: MarketType, symbol: str) -> AsyncGenerator[Trade, None]:
        pass


    @abstractmethod
    async def stream_orderbook(self, market_type: MarketType, symbol: str, depth: int = 20, update_speed: str = "100ms") -> AsyncGenerator[OrderBook, None]:
        pass


    async def stream_liquidations(self, market_type: MarketType, symbol: str) -> AsyncGenerator[Liquidation, None]:
        if False: yield
        raise NotImplementedError(f"Liquidation stream not supported on {self.name} {market_type}")


    async def get_open_interest(self, market_type: MarketType, symbol: str, period: str = "1h", start_time: Optional[int] = None, limit: int = 30) -> List[OpenInterest]:
        raise NotImplementedError(f"Open Interest not supported on {self.name} {market_type}")


    async def get_funding_rate(self, market_type: MarketType, symbol: str, start_time: Optional[int] = None, limit: int = 100) -> List[FundingRate]:
        raise NotImplementedError(f"Funding Rate not supported on {self.name} {market_type}")


    async def get_liquidations(self, market_type: MarketType, symbol: str, start_time: Optional[int] = None, limit: int = 100) -> List[Liquidation]:
        raise NotImplementedError(f"Liquidations not supported on {self.name} {market_type}")


    async def get_long_short_ratio(self, market_type: MarketType, symbol: str, period: str = "5m", start_time: Optional[int] = None, limit: int = 30) -> List[LongShortRatio]:
        raise NotImplementedError(f"L/S Ratio not supported on {self.name} {market_type}")