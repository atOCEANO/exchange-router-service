import asyncio
import logging
import uuid
from typing import Dict, Optional
from fastapi import WebSocket


class StreamManager:

    def __init__(self):
        self.active_streams: Dict[str, Dict[uuid.UUID, WebSocket]] = {}
        self.upstream_tasks: Dict[str, asyncio.Task] = {}
        self._lock = asyncio.Lock()


    async def subscribe(self, key: str, websocket: WebSocket, adapter, market_type, channel, symbol) -> uuid.UUID:
        client_id = uuid.uuid4()
        async with self._lock:
            if key not in self.active_streams:
                self.active_streams[key] = {}
                self.upstream_tasks[key] = asyncio.create_task(
                    self._upstream_handler(key, adapter, market_type, channel, symbol)
                )
            self.active_streams[key][client_id] = websocket
        return client_id


    async def unsubscribe(self, key: str, client_id: uuid.UUID):
        task: Optional[asyncio.Task] = None
        async with self._lock:
            if key in self.active_streams:
                self.active_streams[key].pop(client_id, None)
                if not self.active_streams[key]:
                    task = self.upstream_tasks.pop(key, None)
                    self.active_streams.pop(key, None)
        if task is not None:
            task.cancel()
            try:
                await asyncio.wait_for(task, timeout=2.0)
            except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
                pass


    async def shutdown(self):
        tasks = list(self.upstream_tasks.values())
        self.upstream_tasks.clear()
        self.active_streams.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


    async def _upstream_handler(self, key, adapter, market_type, channel, symbol):
        try:
            if   channel == "ticker":       stream = adapter.stream_ticker(market_type, symbol)
            elif channel == "book_ticker":  stream = adapter.stream_book_ticker(market_type, symbol)
            elif channel == "mark_price":   stream = adapter.stream_mark_price(market_type, symbol)
            elif channel == "agg_trades":   stream = adapter.stream_agg_trades(market_type, symbol)
            elif channel == "trades":       stream = adapter.stream_trades(market_type, symbol)
            elif channel == "orderbook":    stream = adapter.stream_orderbook(market_type, symbol)
            elif channel == "liquidations": stream = adapter.stream_liquidations(market_type, symbol)
            else: return

            async for item in stream:
                data   = item.model_dump_json()
                bucket = self.active_streams.get(key)
                if not bucket:
                    break

                client_items = list(bucket.items())
                results      = await asyncio.gather(
                    *[asyncio.wait_for(client.send_text(data), timeout=5.0) for _, client in client_items],
                    return_exceptions=True,
                )
                dead = [cid for (cid, _), res in zip(client_items, results) if isinstance(res, BaseException)]
                if dead:
                    async with self._lock:
                        current = self.active_streams.get(key)
                        if current is not None:
                            for cid in dead:
                                current.pop(cid, None)
                            if not current:
                                self.active_streams.pop(key, None)
                                self.upstream_tasks.pop(key, None)
                                break
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logging.error(f"Upstream stream error for {key}: {e}")
        finally:
            close_tasks = []
            async with self._lock:
                if self.upstream_tasks.get(key) is asyncio.current_task():
                    self.upstream_tasks.pop(key, None)
                    bucket = self.active_streams.pop(key, None)
                    if bucket:
                        for client in bucket.values():
                            close_tasks.append(asyncio.create_task(client.close(code=1011, reason="Upstream disconnected")))
            for t in close_tasks:
                try:
                    await t
                except Exception:
                    pass
