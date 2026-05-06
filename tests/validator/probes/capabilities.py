import time
from typing import Any, Dict, List

from tests.validator.results import ErrorType, ProbeResult

from tests.validator.probes.base import Probe, ProbeContext, fetch


class CapabilitiesConsistencyProbe(Probe):
    kind  = "rest"
    route = "capabilities_drift"
    name  = "capabilities_drift"


    def applies(self, ctx: ProbeContext) -> bool:
        return ctx.market_type is not None


    async def run(self, ctx: ProbeContext) -> List[ProbeResult]:
        out: List[ProbeResult] = []
        slice_caps = ctx.caps_slice or {}
        symbol = ctx.symbol or "BTCUSDT"

        snapshot_routes = [
            "ticker",
            "book_ticker",
            "mark_price",
        ]
        list_routes = [
            "agg_trades",
            "candles",
            "funding_rate",
            "open_interest",
            "liquidations",
            "long_short_ratio",
        ]

        for route in snapshot_routes + list_routes + ["trades", "orderbook"]:
            rc = slice_caps.get(route, {})
            if "rest" not in rc:
                continue
            if rc.get("rest"):
                continue
            endpoint = f"/{ctx.exchange}/{ctx.market_type}/{route}/{symbol}"
            params: Dict[str, Any] = {}
            if route in ("candles", "open_interest", "long_short_ratio"):
                intervals = rc.get("intervals") or ["1h"]
                params["interval" if route == "candles" else "period"] = intervals[0]
            started = time.time()
            status, data, err = await fetch(ctx.client, endpoint, params)
            ended = time.time()
            probe_name = f"drift:{route}_rest_false"

            if err is not None:
                out.append(self.result(
                    ctx, probe_name, started,
                    status="warn",
                    error_type=ErrorType.NETWORK,
                    message=err,
                    evidence={"endpoint": endpoint},
                    ended=ended,
                ))
                continue
            if status == 501:
                out.append(self.result(
                    ctx, probe_name, started,
                    status="pass",
                    error_type=ErrorType.OK,
                    message=f"{route} rest=False returns 501 as expected",
                    evidence={"endpoint": endpoint, "status": 501},
                    ended=ended,
                ))
            elif status == 200:
                out.append(self.result(
                    ctx, probe_name, started,
                    status="fail",
                    error_type=ErrorType.LOGIC,
                    message=f"{route} advertises rest=False but returned 200",
                    evidence={"endpoint": endpoint, "status": 200, "advertised": False},
                    ended=ended,
                ))
            else:
                out.append(self.result(
                    ctx, probe_name, started,
                    status="pass",
                    error_type=ErrorType.OK,
                    message=f"{route} rest=False returns {status} (acceptable, not 200)",
                    evidence={"endpoint": endpoint, "status": status},
                    ended=ended,
                ))

        ob = slice_caps.get("orderbook", {})
        max_depth = ob.get("max_depth")
        if ob.get("rest") and max_depth and ctx.symbol:
            endpoint = f"/{ctx.exchange}/{ctx.market_type}/orderbook/{ctx.symbol}"
            started = time.time()
            status, data, err = await fetch(ctx.client, endpoint, {"depth": max_depth + 1})
            ended = time.time()
            probe_name = "drift:orderbook_overdepth"
            if err is not None:
                out.append(self.result(
                    ctx, probe_name, started,
                    status="warn",
                    error_type=ErrorType.NETWORK,
                    message=err,
                    evidence={"endpoint": endpoint},
                    ended=ended,
                ))
            elif status == 200 and isinstance(data, dict):
                got = len(data.get("bids", []))
                if got > max_depth:
                    out.append(self.result(
                        ctx, probe_name, started,
                        status="fail",
                        error_type=ErrorType.LOGIC,
                        message=f"depth={max_depth + 1} returned {got} bids, exceeds advertised max_depth={max_depth}",
                        evidence={"max_depth": max_depth, "requested": max_depth + 1, "got": got},
                        ended=ended,
                    ))
                else:
                    out.append(self.result(
                        ctx, probe_name, started,
                        status="pass",
                        error_type=ErrorType.OK,
                        message=f"depth={max_depth + 1} clamped to {got} (<=max_depth={max_depth})",
                        evidence={"max_depth": max_depth, "got": got},
                        ended=ended,
                    ))
            else:
                out.append(self.result(
                    ctx, probe_name, started,
                    status="pass",
                    error_type=ErrorType.OK,
                    message=f"depth={max_depth + 1} returned HTTP {status} (acceptable rejection)",
                    evidence={"max_depth": max_depth, "status": status},
                    ended=ended,
                ))

        return out
