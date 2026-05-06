import time
from typing import List

from tests.validator.results import ErrorType, ProbeResult

from tests.validator.probes.base import Probe, ProbeContext, fetch


class MarketsProbe(Probe):
    kind  = "rest"
    route = "markets"
    name  = "markets"


    def applies(self, ctx: ProbeContext) -> bool:
        return ctx.market_type is not None


    async def run(self, ctx: ProbeContext) -> List[ProbeResult]:
        endpoint = f"/{ctx.exchange}/{ctx.market_type}/markets"
        started = time.time()
        status, data, err = await fetch(ctx.client, endpoint)

        if status != 200:
            return [self.http_failure(ctx, self.name, started, status, data, err)]
        if not isinstance(data, list):
            return [self.result(
                ctx, self.name, started,
                status="fail",
                error_type=ErrorType.SCHEMA,
                message="response is not a list",
                evidence={"got_type": type(data).__name__},
            )]

        bad: List[str] = []
        seen: set = set()
        dups: List[str] = []
        for s in data:
            if not isinstance(s, str) or not s:
                bad.append(repr(s))
                continue
            if "/" in s or s != s.upper():
                bad.append(s)
            if s in seen:
                dups.append(s)
            else:
                seen.add(s)

        if bad:
            return [self.result(
                ctx, self.name, started,
                status="warn",
                error_type=ErrorType.LOGIC,
                message=f"{len(data)} symbols, {len(bad)} formatting issues",
                evidence={"count": len(data), "bad_sample": bad[:5]},
            )]
        if dups:
            return [self.result(
                ctx, self.name, started,
                status="fail",
                error_type=ErrorType.LOGIC,
                message=f"{len(dups)} duplicate symbols",
                evidence={"duplicates": dups[:5]},
            )]

        return [self.result(
            ctx, self.name, started,
            status="pass",
            error_type=ErrorType.OK,
            message=f"{len(data)} symbols",
            evidence={"count": len(data)},
        )]
