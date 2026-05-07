import asyncio
import logging
import uuid
from typing import Dict
from fastapi import WebSocket


class StreamManager:
    """Manages fan-out of upstream exchange streams to multiple local clients."""

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
        async with self._lock:
            if key in self.active_streams:
                self.active_streams[key].pop(client_id, None)
                if not self.active_streams[key]:
                    self.upstream_tasks[key].cancel()
                    self.upstream_tasks.pop(key, None)
                    self.active_streams.pop(key, None)

    async def shutdown(self):
        for task in self.upstream_tasks.values():
            task.cancel()
        self.upstream_tasks.clear()
        self.active_streams.clear()

    async def _upstream_handler(self, key, adapter, market_type, channel, symbol):
        try:
            if channel == "ticker":         stream = adapter.stream_ticker(market_type, symbol)
            elif channel == "book_ticker":  stream = adapter.stream_book_ticker(market_type, symbol)
            elif channel == "mark_price":   stream = adapter.stream_mark_price(market_type, symbol)
            elif channel == "agg_trades":   stream = adapter.stream_agg_trades(market_type, symbol)
            elif channel == "trades":       stream = adapter.stream_trades(market_type, symbol)
            elif channel == "orderbook":    stream = adapter.stream_orderbook(market_type, symbol)
            elif channel == "liquidations": stream = adapter.stream_liquidations(market_type, symbol)
            else: return

            async for item in stream:
                data = item.model_dump_json()
                if key in self.active_streams:
                    clients = list(self.active_streams[key].values())
                    await asyncio.gather(*[client.send_text(data) for client in clients], return_exceptions=True)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logging.error(f"Upstream stream error for {key}: {e}")
        finally:
            if key in self.active_streams:
                clients = list(self.active_streams[key].values())
                for client in clients:
                    asyncio.create_task(client.close(code=1011, reason="Upstream disconnected"))
