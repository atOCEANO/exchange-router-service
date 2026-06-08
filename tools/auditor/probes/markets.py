import time
from typing import List

from tools.auditor.aggregator import ErrorType, ProbeResult
from tools.auditor.config import SAMPLE_MAX_ITEMS

from tools.auditor.probes.base import Probe, ProbeContext, fetch


class MarketsProbe(Probe):
    kind  = "rest"
    route = "markets"
    name  = "markets"


    def applies(self, ctx: ProbeContext) -> bool:
        return ctx.market_type is not None


    async def run(self, ctx: ProbeContext) -> List[ProbeResult]:
        endpoint = f"/{ctx.exchange}/{ctx.market_type}/markets"
        started = time.time()
        status, body, err = await fetch(ctx.client, endpoint)

        if status != 200:
            return [self.http_failure(ctx, self.name, started, status, body, err)]
        if not isinstance(body, dict) or "markets" not in body:
            return [self.result(
                ctx, self.name, started,
                status="fail",
                error_type=ErrorType.SCHEMA,
                message="response is not the lite-list shape",
                evidence={"got_type": type(body).__name__, "keys": list(body.keys()) if isinstance(body, dict) else None},
            )]

        entries = body["markets"]
        bad: List[str]  = []
        seen: set        = set()
        dups: List[str]  = []
        for entry in entries:
            if not isinstance(entry, dict) or "symbol" not in entry:
                bad.append(repr(entry))
                continue
            s = entry["symbol"]
            if not isinstance(s, str) or not s:
                bad.append(repr(s))
                continue
            if "/" in s or s != s.upper():
                bad.append(s)
            if s in seen:
                dups.append(s)
            else:
                seen.add(s)

            for required_key in ("base_asset", "quote_asset", "qty_unit", "funding"):
                if required_key not in entry:
                    bad.append(f"{s}: missing {required_key}")
                    break

        data   = entries
        sample = data[:SAMPLE_MAX_ITEMS]

        if bad:
            r = self.result(
                ctx, self.name, started,
                status="warn",
                error_type=ErrorType.LOGIC,
                message=f"{len(data)} symbols, {len(bad)} formatting issues",
                evidence={"count": len(data), "bad_sample": bad[:5]},
            )
            r.sample = sample
            return [r]
        if dups:
            r = self.result(
                ctx, self.name, started,
                status="fail",
                error_type=ErrorType.LOGIC,
                message=f"{len(dups)} duplicate symbols",
                evidence={"duplicates": dups[:5]},
            )
            r.sample = sample
            return [r]

        r = self.result(
            ctx, self.name, started,
            status="pass",
            error_type=ErrorType.OK,
            message=f"{len(data)} symbols",
            evidence={"count": len(data)},
        )
        r.sample = sample
        return [r]
