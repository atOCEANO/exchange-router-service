import time
from typing import List

from src.models import OrderBook
from tools.auditor.config import SNAPSHOT_FRESHNESS_MS
from tools.auditor.aggregator import ErrorType, ProbeResult

from tools.auditor.probes.base import (
    Probe, ProbeContext, fetch, now_ms, validate_one,
)


class OrderBookProbe(Probe):
    kind  = "rest"
    route = "orderbook"
    name  = "orderbook"


    def applies(self, ctx: ProbeContext) -> bool:
        return bool(ctx.caps_slice and ctx.caps_slice.get("orderbook", {}).get("rest"))


    async def run(self, ctx: ProbeContext) -> List[ProbeResult]:
        ob_caps = ctx.caps_slice["orderbook"]
        declared = ob_caps.get("depths")
        if declared:
            depths = sorted({d for d in declared if 1 <= d <= 100})
            if not depths:
                depths = [min(declared)]
        else:
            max_depth = min(ob_caps.get("max_depth", 100), 100)
            depths = sorted({1, 20, max_depth})

        out: List[ProbeResult] = []
        for depth in depths:
            out.append(await self._probe_depth(ctx, depth))
        return out


    async def _probe_depth(self, ctx: ProbeContext, depth: int) -> ProbeResult:
        endpoint = f"/{ctx.exchange}/{ctx.market_type}/orderbook/{ctx.symbol}"
        probe_name = f"orderbook:depth={depth}"
        started = time.time()
        status, data, err = await fetch(ctx.client, endpoint, {"depth": depth})

        if status != 200:
            return self.http_failure(ctx, probe_name, started, status, data, err)

        shape_err = validate_one(data, OrderBook)
        if shape_err:
            msg, ev = shape_err
            return self.result(
                ctx, probe_name, started,
                status="fail",
                error_type=ErrorType.SCHEMA,
                message=msg,
                evidence=ev,
            )

        bids, asks = data["bids"], data["asks"]

        if len(bids) > depth or len(asks) > depth:
            return self.result(
                ctx, probe_name, started,
                status="fail",
                error_type=ErrorType.LOGIC,
                message=f"got {len(bids)} bids / {len(asks)} asks > requested depth {depth}",
                evidence={"requested_depth": depth, "got_bids": len(bids), "got_asks": len(asks)},
            )

        for i in range(len(bids) - 1):
            if bids[i][0] < bids[i + 1][0]:
                return self.result(
                    ctx, probe_name, started,
                    status="fail",
                    error_type=ErrorType.LOGIC,
                    message=f"bids not strictly descending at idx {i}",
                    evidence={"side": "bids", "idx": i, "prev": bids[i][0], "this": bids[i + 1][0]},
                )

        for i in range(len(asks) - 1):
            if asks[i][0] > asks[i + 1][0]:
                return self.result(
                    ctx, probe_name, started,
                    status="fail",
                    error_type=ErrorType.LOGIC,
                    message=f"asks not strictly ascending at idx {i}",
                    evidence={"side": "asks", "idx": i, "prev": asks[i][0], "this": asks[i + 1][0]},
                )

        if bids and asks and bids[0][0] >= asks[0][0]:
            return self.result(
                ctx, probe_name, started,
                status="fail",
                error_type=ErrorType.LOGIC,
                message=f"crossed book: top bid {bids[0][0]} >= top ask {asks[0][0]}",
                evidence={"top_bid": bids[0][0], "top_ask": asks[0][0]},
            )

        for side, levels in (("bids", bids), ("asks", asks)):
            for i, lvl in enumerate(levels):
                p, q = lvl[0], lvl[1]
                if p <= 0 or q <= 0:
                    return self.result(
                        ctx, probe_name, started,
                        status="fail",
                        error_type=ErrorType.LOGIC,
                        message=f"non-positive level on {side}[{i}]: price={p}, qty={q}",
                        evidence={"side": side, "idx": i, "price": p, "qty": q},
                    )

        age_ms = now_ms() - data["timestamp"]
        if age_ms > SNAPSHOT_FRESHNESS_MS:
            return self.result(
                ctx, probe_name, started,
                status="warn",
                error_type=ErrorType.LOGIC,
                message=f"stale orderbook: {age_ms/1000:.0f}s old",
                evidence={"age_ms": age_ms, "timestamp": data["timestamp"]},
            )

        spread = asks[0][0] - bids[0][0] if bids and asks else 0

        r = self.result(
            ctx, probe_name, started,
            status="pass",
            error_type=ErrorType.OK,
            message=f"depth={depth}, got {len(bids)}/{len(asks)} bids/asks, top spread={spread:.6g}",
            evidence={"depth": depth, "got_bids": len(bids), "got_asks": len(asks), "spread": spread},
        )
        r.sample = data
        return r
