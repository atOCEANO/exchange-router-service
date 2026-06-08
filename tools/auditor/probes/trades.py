import time
from typing import List

from src.models import Trade
from tools.auditor.aggregator import ErrorType, ProbeResult
from tools.auditor.config import SAMPLE_MAX_ITEMS

from tools.auditor.probes.base import (
    Probe, ProbeContext, check_ascending_ts, fetch, validate_list,
)


class TradesProbe(Probe):
    kind  = "rest"
    route = "trades"
    name  = "trades"


    def applies(self, ctx: ProbeContext) -> bool:
        return bool(ctx.caps_slice and ctx.caps_slice.get("trades", {}).get("rest"))


    async def run(self, ctx: ProbeContext) -> List[ProbeResult]:
        tcaps = ctx.caps_slice["trades"]
        upstream_max = tcaps.get("max_limit") or 1000
        big = min(upstream_max, 1000)
        limits = sorted({1, min(100, big), big})

        out: List[ProbeResult] = []
        for lim in limits:
            out.append(await self._probe_limit(ctx, lim))
        return out


    async def _probe_limit(self, ctx: ProbeContext, lim: int) -> ProbeResult:
        endpoint = f"/{ctx.exchange}/{ctx.market_type}/trades/{ctx.symbol}"
        probe_name = f"trades:limit={lim}"
        started = time.time()
        status, data, err = await fetch(ctx.client, endpoint, {"limit": lim})

        if status != 200:
            return self.http_failure(ctx, probe_name, started, status, data, err)
        if not isinstance(data, list):
            return self.result(
                ctx, probe_name, started,
                status="fail",
                error_type=ErrorType.SCHEMA,
                message="response is not a list",
                evidence={"got_type": type(data).__name__},
            )

        n = len(data)
        if n == 0:
            return self.result(
                ctx, probe_name, started,
                status="fail",
                error_type=ErrorType.EMPTY,
                message="empty trades",
                evidence={"limit": lim, "endpoint": endpoint},
            )
        if n > lim:
            return self.result(
                ctx, probe_name, started,
                status="fail",
                error_type=ErrorType.LOGIC,
                message=f"got {n} trades, limit was {lim}",
                evidence={"limit": lim, "got": n},
            )

        shape_err = validate_list(data, Trade)
        if shape_err:
            msg, ev = shape_err
            return self.result(
                ctx, probe_name, started,
                status="fail",
                error_type=ErrorType.SCHEMA,
                message=msg,
                evidence=ev,
            )

        ids = [t["id"] for t in data if t.get("id") is not None]
        if len(set(ids)) != len(ids):
            seen, dup = set(), None
            for tid in ids:
                if tid in seen:
                    dup = tid
                    break
                seen.add(tid)
            return self.result(
                ctx, probe_name, started,
                status="fail",
                error_type=ErrorType.LOGIC,
                message="duplicate trade id",
                evidence={"duplicate_id": dup, "total": len(ids)},
            )

        violation = check_ascending_ts(data)
        if violation:
            return self.result(
                ctx, probe_name, started,
                status="fail",
                error_type=ErrorType.LOGIC,
                message=f"non-ascending timestamps at idx {violation['idx']}",
                evidence=violation,
            )

        if lim == 1 and n != 1:
            return self.result(
                ctx, probe_name, started,
                status="fail",
                error_type=ErrorType.LOGIC,
                message=f"limit=1 returned {n}",
                evidence={"limit": 1, "got": n},
            )

        sample = data[:SAMPLE_MAX_ITEMS]

        if lim > 1 and n < lim:
            r = self.result(
                ctx, probe_name, started,
                status="warn",
                error_type=ErrorType.LOGIC,
                message=f"got {n}/{lim} trades (upstream returned fewer than declared cap)",
                evidence={"limit": lim, "got": n},
            )
            r.sample = sample
            return r

        r = self.result(
            ctx, probe_name, started,
            status="pass",
            error_type=ErrorType.OK,
            message=f"got {n}/{lim} trades, ascending order",
            evidence={"limit": lim, "got": n},
        )
        r.sample = sample
        return r
