import asyncio
import logging
import time
import httpx
import orjson
from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any, Set, Union, AsyncGenerator, Callable, Awaitable, Iterable
from datetime import datetime
from src.models import (
    Ticker, BookTicker, MarkPrice, OrderBook, Candle, Trade, AggTrade,
    MarketType, SymbolInfo, FundingRate, OpenInterest, Liquidation, LongShortRatio,
    QtyValue, VolumeValue, OiValue, UsdBasis,
    FundingCurrent, FundingHistorical, FundingConvention,
)


logger = logging.getLogger(__name__)


class AdapterError(Exception):
    pass


class UpstreamUnavailableError(AdapterError):
    def __init__(self, message: str, retry_after: Optional[float] = None):
        super().__init__(message)
        self.retry_after = retry_after


def build_qty_value(native: float, qty_unit: str, contract_size: Optional[float], price: float) -> QtyValue:
    if qty_unit == "contract":
        if contract_size is None:
            raise ValueError("build_qty_value: contract_size is required when qty_unit == 'contract'")
        usd = native * contract_size
    else:
        usd = native * price

    return QtyValue(
        native        = native,
        unit          = qty_unit,
        contract_size = contract_size if qty_unit == "contract" else None,
        usd           = usd,
    )


def build_volume_value(native: float, volume_unit: str, contract_size: Optional[float], close: float) -> VolumeValue:
    if volume_unit == "contract":
        if contract_size is None:
            raise ValueError("build_volume_value: contract_size is required when volume_unit == 'contract'")
        usd   = native * contract_size
        basis = UsdBasis(method="contract_size")
    else:
        usd   = native * close
        basis = UsdBasis(method="close")

    return VolumeValue(
        native        = native,
        unit          = volume_unit,
        contract_size = contract_size if volume_unit == "contract" else None,
        usd           = usd,
        usd_basis     = basis,
    )


def build_oi_value(native: float, oi_unit: str, contract_size: Optional[float], candle_close: Optional[float] = None, candle_close_ts: Optional[int] = None) -> OiValue:
    if oi_unit == "contract":
        if contract_size is None:
            raise ValueError("build_oi_value: contract_size is required when oi_unit == 'contract'")
        usd   = native * contract_size
        basis = UsdBasis(method="contract_size")
        return OiValue(
            native        = native,
            unit          = oi_unit,
            contract_size = contract_size,
            usd           = usd,
            usd_basis     = basis,
        )

    if candle_close is not None and candle_close_ts is not None:
        usd   = native * candle_close
        basis = UsdBasis(method="candle_close", close=candle_close, close_ts=candle_close_ts)
    else:
        usd   = None
        basis = UsdBasis(method="candle_close", close=None, close_ts=None)

    return OiValue(
        native        = native,
        unit          = oi_unit,
        contract_size = None,
        usd           = usd,
        usd_basis     = basis,
    )


def build_funding_current(kind: str, per_cycle: float, cycle_ms: int, valid_until_ts: int) -> FundingCurrent:
    return FundingCurrent(
        kind           = kind,
        per_cycle      = per_cycle,
        cycle_ms       = cycle_ms,
        valid_until_ts = valid_until_ts,
    )


def build_funding_historical(kind: str, per_cycle: float, cycle_ms: int) -> FundingHistorical:
    return FundingHistorical(
        kind      = kind,
        per_cycle = per_cycle,
        cycle_ms  = cycle_ms,
    )


def build_funding_convention(kind: str) -> FundingConvention:
    return FundingConvention(kind=kind)


class StreamHub:
    def __init__(self,
                 name: str,
                 connect: Callable[[], Awaitable[Any]],
                 subscribe_payload: Callable[[List[str]], Iterable[Any]],
                 unsubscribe_payload: Callable[[List[str]], Iterable[Any]],
                 route: Callable[[Any], Optional[str]],
                 keepalive_payload: Optional[Any] = None,
                 keepalive_interval: float = 0.0,
                 queue_size: int = 1024):
        self.name                = name
        self._connect            = connect
        self._sub_payload        = subscribe_payload
        self._unsub_payload      = unsubscribe_payload
        self._route              = route
        self._keepalive_payload  = keepalive_payload
        self._keepalive_interval = keepalive_interval
        self._queue_size         = queue_size

        self._queues: Dict[str, List[asyncio.Queue]] = {}
        self._subscribed_topics: Set[str] = set()
        self._lock      = asyncio.Lock()
        self._connected = asyncio.Event()
        self._task: Optional[asyncio.Task] = None
        self._ws        = None
        self._closed    = False
        self._log       = logging.getLogger(name)


    async def subscribe(self, topic: str) -> asyncio.Queue:
        q            = asyncio.Queue(maxsize=self._queue_size)
        is_new_topic = False

        async with self._lock:
            if topic not in self._queues:
                self._queues[topic] = []
                is_new_topic       = True
            self._queues[topic].append(q)
            if self._task is None or self._task.done():
                self._closed = False
                self._task   = asyncio.create_task(self._run())

        if is_new_topic:
            try:
                await asyncio.wait_for(self._connected.wait(), timeout=30)
            except asyncio.TimeoutError:
                self._log.warning(f"subscribe({topic}) timed out waiting for connection; will pick up on reconnect")
                return q

            send_now = False
            async with self._lock:
                if topic not in self._subscribed_topics:
                    self._subscribed_topics.add(topic)
                    send_now = True
            if send_now:
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
                self._subscribed_topics.discard(topic)

        if drop_topic and self._connected.is_set():
            await self._send_payloads(self._unsub_payload([topic]))


    async def close(self) -> None:
        self._closed = True
        task         = self._task
        self._task   = None

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
            ws = None

            try:
                self._log.info("connecting upstream WS")
                ws       = await self._connect()
                self._ws = ws

                async with self._lock:
                    topics                  = list(self._queues.keys())
                    self._subscribed_topics = set(topics)
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
                            try:
                                q.get_nowait()
                            except asyncio.QueueEmpty:
                                pass
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
                if ws is not None:
                    try:
                        await ws.close()
                    except Exception:
                        pass

            if not self._closed:
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 30)


class BaseExchange(ABC):
    INFO_REFRESH_S = 24 * 60 * 60
    INFO_RETRY_S   = 60 * 60


    def __init__(self):
        self._info_cache: Dict[MarketType, Dict[str, SymbolInfo]] = {}
        self._info_cache_at: Dict[MarketType, float] = {}
        self._info_cache_lock = asyncio.Lock()
        self._warm_task: Optional[asyncio.Task] = None


    @abstractmethod
    async def shutdown(self):
        pass


    def normalize_symbol(self, symbol: str) -> str:
        return symbol.replace("/", "").replace("-", "").upper()


    def normalize_timestamp(self, ts: Union[int, float, str]) -> int:
        if isinstance(ts, str):
            try:
                ts = float(ts)
            except ValueError:
                try:
                    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                    return int(dt.timestamp() * 1000)
                except ValueError:
                    raise ValueError(f"Could not parse timestamp string: {ts}")
        if isinstance(ts, (int, float)):
            if ts < 100_000_000_000:
                return int(ts * 1000)
            return int(ts)
        raise ValueError(f"Unsupported timestamp format: {type(ts)}")


    async def _paginate_backwards(self, fetch_func_by_end: Callable, total_limit: int, limit_per_req: int) -> List[Any]:
        chunks = []
        seen = set()
        collected = 0
        current_end = None
        max_requests = 100
        req_count = 0

        while collected < total_limit and req_count < max_requests:
            try:
                batch = await fetch_func_by_end(current_end, limit_per_req)
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
            collected += len(new_items)
            req_count += 1
            next_end = batch[0].timestamp
            if next_end <= 0 or (current_end is not None and next_end >= current_end):
                break
            current_end = next_end

        if req_count >= max_requests and collected < total_limit:
            logging.getLogger(f"{self.name}_adapter").warning(f"{self.name} pagination hit {max_requests}-page safety cap with {collected}/{total_limit} records; result truncated")

        chunks.reverse()
        out = [item for chunk in chunks for item in chunk]
        out.sort(key=lambda x: x.timestamp)
        return out[-total_limit:]


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


    async def preload(self) -> None:
        if type(self)._warm is BaseExchange._warm:
            return

        self._warm_task = asyncio.create_task(self._run_warm())


    async def _run_warm(self) -> None:
        started = time.monotonic()
        logger.info(f"[{self.name}] preload: starting")

        try:
            await self._warm()
            elapsed = time.monotonic() - started
            logger.info(f"[{self.name}] preload: done in {elapsed:.1f}s")

        except Exception:
            elapsed = time.monotonic() - started
            logger.exception(f"[{self.name}] preload: aborted after {elapsed:.1f}s")


    async def _step(self, name: str, awaitable: Awaitable[Any]) -> None:
        started = time.monotonic()
        logger.info(f"[{self.name}] preload: warming {name}...")

        try:
            await awaitable
            elapsed = time.monotonic() - started
            logger.info(f"[{self.name}] preload: {name} ready in {elapsed:.1f}s")

        except Exception:
            elapsed = time.monotonic() - started
            logger.exception(f"[{self.name}] preload: {name} failed after {elapsed:.1f}s")
            raise


    async def _warm(self) -> None:
        return None


    async def _fetch_exchange_info(self, market_type: MarketType) -> List[SymbolInfo]:
        raise NotImplementedError(f"Exchange info fetch not implemented on {self.name}")


    async def _ensure_info_cache(self, market_type: MarketType) -> Dict[str, SymbolInfo]:
        async with self._info_cache_lock:
            now     = time.monotonic()
            cached  = self._info_cache.get(market_type)
            elapsed = now - self._info_cache_at.get(market_type, 0.0)

            if cached is not None and elapsed < self.INFO_REFRESH_S:
                return cached

            try:
                infos = await self._fetch_exchange_info(market_type)
            except Exception:
                if cached is None:
                    raise
                logger.warning(f"[{self.name}] exchange info refresh failed for {market_type.value}; serving cached metadata")
                self._info_cache_at[market_type] = now - self.INFO_REFRESH_S + self.INFO_RETRY_S
                return cached

            self._info_cache[market_type] = {info.symbol: info for info in infos}
            self._info_cache_at[market_type] = now

            return self._info_cache[market_type]


    async def _info_for(self, market_type: MarketType, model_symbol: str) -> Optional[SymbolInfo]:
        cache = await self._ensure_info_cache(market_type)
        return cache.get(model_symbol)


    @abstractmethod
    async def get_exchange_info(self, market_type: MarketType) -> List[SymbolInfo]:
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
