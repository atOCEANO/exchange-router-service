import time
from typing import Any, Dict, List, Tuple

from tests.validator.results import ErrorType, ProbeResult

from tests.validator.probes.base import Probe, ProbeContext, fetch


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
            ("error:bad_market_type",  f"/{ctx.exchange}/notamarket/info",         {}, (400, 422), "400/422"),
            ("error:bad_symbol",       f"/{ctx.exchange}/spot/ticker/ZZZNOTREAL",  {}, (400, 404), "400/404"),
        ]

        for probe_name, path, params, expected, expected_str in cases:
            started = time.time()
            status, data, err = await fetch(ctx.client, path, params)
            ended = time.time()
            if err is not None:
                out.append(self.result(
                    ctx, probe_name, started,
                    status="fail",
                    error_type=ErrorType.NETWORK,
                    message=err,
                    evidence={"endpoint": path},
                    ended=ended,
                ))
                continue
            if status in expected:
                out.append(self.result(
                    ctx, probe_name, started,
                    status="pass",
                    error_type=ErrorType.OK,
                    message=f"HTTP {status} (expected {expected_str})",
                    evidence={"endpoint": path, "status": status},
                    ended=ended,
                ))
            else:
                out.append(self.result(
                    ctx, probe_name, started,
                    status="fail",
                    error_type=ErrorType.LOGIC,
                    message=f"got {status}, expected {expected_str}",
                    evidence={"endpoint": path, "status": status, "expected": expected_str},
                    ended=ended,
                ))

        return out
