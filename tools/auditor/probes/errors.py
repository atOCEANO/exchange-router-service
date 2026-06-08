import time
from typing import Any, Dict, List, Tuple

from tools.auditor.aggregator import ErrorType, ProbeResult

from tools.auditor.probes.base import Probe, ProbeContext, fetch


class ErrorPathProbe(Probe):
    kind  = "rest"
    route = "error_paths"
    name  = "error_paths"


    def applies(self, ctx: ProbeContext) -> bool:
        return ctx.market_type is None


    async def run(self, ctx: ProbeContext) -> List[ProbeResult]:
        out: List[ProbeResult] = []

        cases: List[Tuple[str, str, Dict[str, Any], Tuple[int, ...], str]] = [
            ("error:unknown_exchange", "/zzznotanexchange/status",                 {}, (404,),     "404"),
            ("error:bad_market_type",  f"/{ctx.exchange}/notamarket/markets",      {}, (400, 422), "400/422"),
            ("error:bad_symbol",       f"/{ctx.exchange}/spot/ticker/ZZZNOTREAL",  {}, (400, 404), "400/404"),
        ]

        for probe_name, path, params, expected, expected_str in cases:
            started = time.time()
            status, data, err = await fetch(ctx.client, path, params)
            ended = time.time()
            if err is not None:
                r = self.result(
                    ctx, probe_name, started,
                    status="fail",
                    error_type=ErrorType.NETWORK,
                    message=err,
                    evidence={"endpoint": path},
                    ended=ended,
                )
                out.append(r)
                continue
            if status in expected:
                r = self.result(
                    ctx, probe_name, started,
                    status="pass",
                    error_type=ErrorType.OK,
                    message=f"HTTP {status} (expected {expected_str})",
                    evidence={"endpoint": path, "status": status},
                    ended=ended,
                )
            else:
                r = self.result(
                    ctx, probe_name, started,
                    status="fail",
                    error_type=ErrorType.LOGIC,
                    message=f"got {status}, expected {expected_str}",
                    evidence={"endpoint": path, "status": status, "expected": expected_str},
                    ended=ended,
                )
            r.sample = data
            out.append(r)

        return out
