import asyncio
import json
import time
from typing import Any, Dict, List, Optional, Type

import websockets
from pydantic import BaseModel

from src.models import (
    AggTrade, BookTicker, Liquidation, MarkPrice, OrderBook, Ticker, Trade,
)
from tests.validator.config import API_URL, WS_MIN_FRAMES, WS_TEST_DURATION
from tests.validator.results import ErrorType, ProbeResult

from tests.validator.probes.base import Probe, ProbeContext, validate_one


WS_CHANNEL_MODEL: Dict[str, Type[BaseModel]] = {
    "ticker":       Ticker,
    "book_ticker":  BookTicker,
    "trades":       Trade,
    "agg_trades":   AggTrade,
    "orderbook":    OrderBook,
    "mark_price":   MarkPrice,
    "liquidations": Liquidation,
}


class WebSocketProbe(Probe):
    kind = "ws"


    def __init__(self, channel: str):
        self.channel    = channel
        self.route      = channel
        self.name       = f"ws:{channel}"
        self.model      = WS_CHANNEL_MODEL.get(channel)
        self.min_frames = WS_MIN_FRAMES.get(channel, 1)


    def applies(self, ctx: ProbeContext) -> bool:
        return bool(ctx.caps_slice and ctx.caps_slice.get(self.channel, {}).get("ws"))


    async def run(self, ctx: ProbeContext) -> List[ProbeResult]:
        if API_URL.startswith("https://"):
            base_ws = API_URL.replace("https://", "wss://")
        else:
            base_ws = API_URL.replace("http://", "ws://")
        ws_url = f"{base_ws}/ws/{ctx.exchange}/{ctx.market_type}"

        started = time.time()
        frames_received = 0
        first_frame_ms: Optional[float] = None
        last_error: Optional[str] = None
        bad_frame: Optional[Dict[str, Any]] = None

        try:
            async with websockets.connect(ws_url, open_timeout=15) as ws:
                await ws.send(json.dumps({"channel": self.channel, "symbol": ctx.symbol}))
                deadline = started + WS_TEST_DURATION

                while True:
                    remaining = deadline - time.time()
                    if remaining <= 0:
                        break
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                    except asyncio.TimeoutError:
                        break

                    if first_frame_ms is None:
                        first_frame_ms = (time.time() - started) * 1000.0

                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        bad_frame = {"reason": "non-json", "preview": str(raw)[:120]}
                        break

                    if isinstance(msg, dict) and msg.get("error"):
                        bad_frame = {"reason": "upstream_error", "error": msg["error"]}
                        break

                    if self.model is not None:
                        shape_err = validate_one(msg, self.model)
                        if shape_err:
                            err_msg, ev = shape_err
                            bad_frame = {"reason": "schema", "error": err_msg, **ev}
                            break

                    frames_received += 1
        except asyncio.TimeoutError:
            last_error = "connection timeout"
        except websockets.exceptions.WebSocketException as e:
            last_error = f"ws error: {e}"
        except Exception as e:
            last_error = f"unexpected: {e}"

        ended = time.time()

        if last_error and frames_received == 0:
            return [self.result(
                ctx, self.name, started,
                status="fail",
                error_type=ErrorType.NETWORK,
                message=last_error,
                evidence={"channel": self.channel, "url": ws_url},
                ended=ended,
            )]

        if bad_frame:
            return [self.result(
                ctx, self.name, started,
                status="fail",
                error_type=ErrorType.SCHEMA if bad_frame["reason"] == "schema" else ErrorType.LOGIC,
                message=f"bad frame: {bad_frame.get('error') or bad_frame.get('preview') or bad_frame['reason']}",
                evidence={"channel": self.channel, "frames_before_failure": frames_received, **bad_frame},
                ended=ended,
            )]

        evidence = {
            "channel":         self.channel,
            "frames_received": frames_received,
            "min_required":    self.min_frames,
            "first_frame_ms":  first_frame_ms,
            "window_s":        WS_TEST_DURATION,
        }

        if frames_received == 0:
            if self.min_frames == 0:
                return [self.result(
                    ctx, self.name, started,
                    status="pass",
                    error_type=ErrorType.OK,
                    message=f"connected, no frames in {WS_TEST_DURATION:.0f}s window (acceptable for {self.channel})",
                    evidence=evidence,
                    ended=ended,
                )]
            return [self.result(
                ctx, self.name, started,
                status="fail",
                error_type=ErrorType.EMPTY,
                message=f"no frames received in {WS_TEST_DURATION:.0f}s (required >= {self.min_frames})",
                evidence=evidence,
                ended=ended,
            )]

        if frames_received < self.min_frames:
            return [self.result(
                ctx, self.name, started,
                status="warn",
                error_type=ErrorType.LOGIC,
                message=f"got {frames_received}/{self.min_frames} frames (flaky stream or quiet market)",
                evidence=evidence,
                ended=ended,
            )]

        return [self.result(
            ctx, self.name, started,
            status="pass",
            error_type=ErrorType.OK,
            message=f"received {frames_received} frames, first at {first_frame_ms:.0f}ms",
            evidence=evidence,
            ended=ended,
        )]
