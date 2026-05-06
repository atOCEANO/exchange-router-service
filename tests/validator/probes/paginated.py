import time
from typing import Any, Dict, List, Optional, Tuple, Type

from pydantic import BaseModel

from tests.validator.config import freshness_threshold_ms, interval_ms, theoretical_max
from tests.validator.results import ErrorType, ProbeResult

from tests.validator.probes.base import (
    Probe, ProbeContext, check_ascending_ts, fetch, validate_list,
)


class PaginatedProbe(Probe):
    kind = "rest"


    def __init__(self, route: str, model: Type[BaseModel], *, big_limit: int = 1000, allow_empty: bool = False, period: Optional[str] = None, period_param: str = "period"):
        self.route        = route
        self.model        = model
        self.big_limit    = big_limit
        self.allow_empty  = allow_empty
        self.period       = period
        self.period_param = period_param
        self.period_ms    = interval_ms(period) if period else 0
        self.name         = route if not period else f"{route}:{period}"


    def applies(self, ctx: ProbeContext) -> bool:
        if not ctx.caps_slice:
            return False
        rc = ctx.caps_slice.get(self.route, {})
        if not rc.get("rest"):
            return False
        if self.period:
            intervals = rc.get("intervals") or []
            return self.period in intervals
        return True


    async def _request(self, ctx: ProbeContext, params: Dict[str, Any]) -> Tuple[Optional[int], Any, Optional[str]]:
        endpoint = f"/{ctx.exchange}/{ctx.market_type}/{self.route}/{ctx.symbol}"
        merged: Dict[str, Any] = {}
        if self.period:
            merged[self.period_param] = self.period
        merged.update(params)
        return await fetch(ctx.client, endpoint, merged)


    async def _probe(self, ctx: ProbeContext, sub_name: str, params: Dict[str, Any], expect_empty_ok: bool) -> Tuple[Optional[List[dict]], ProbeResult]:
        probe_name = f"{self.name}:{sub_name}"
        started = time.time()
        status, data, err = await self._request(ctx, params)

        if status != 200:
            return None, self.http_failure(ctx, probe_name, started, status, data, err)
        if not isinstance(data, list):
            return None, self.result(
                ctx, probe_name, started,
                status="fail",
                error_type=ErrorType.SCHEMA,
                message="response is not a list",
                evidence={"got_type": type(data).__name__, "params": params},
            )

        if len(data) == 0:
            if expect_empty_ok or self.allow_empty:
                return None, self.result(
                    ctx, probe_name, started,
                    status="warn",
                    error_type=ErrorType.EMPTY,
                    message="empty (upstream retention or quiet market)",
                    evidence={"params": params},
                )
            return None, self.result(
                ctx, probe_name, started,
                status="fail",
                error_type=ErrorType.EMPTY,
                message="empty",
                evidence={"params": params},
            )

        shape_err = validate_list(data, self.model)
        if shape_err:
            msg, ev = shape_err
            return None, self.result(
                ctx, probe_name, started,
                status="fail",
                error_type=ErrorType.SCHEMA,
                message=msg,
                evidence={**ev, "params": params},
            )

        violation = check_ascending_ts(data)
        if violation:
            return None, self.result(
                ctx, probe_name, started,
                status="fail",
                error_type=ErrorType.LOGIC,
                message=f"non-ascending timestamps at idx {violation['idx']}",
                evidence={**violation, "params": params},
            )

        return data, self.result(
            ctx, probe_name, started,
            status="pass",
            error_type=ErrorType.OK,
            message=f"got {len(data)} records",
            evidence={"params": params, "count": len(data), "first_ts": data[0]["timestamp"], "last_ts": data[-1]["timestamp"]},
        )


    async def run(self, ctx: ProbeContext) -> List[ProbeResult]:
        rc = ctx.caps_slice[self.route]
        paginated    = rc.get("paginated", True)
        upstream_max = rc.get("max_limit")
        retention_ms = rc.get("retention_ms")
        big = self.big_limit if upstream_max is None else min(self.big_limit, upstream_max)

        out: List[ProbeResult] = []
        out.append(await self._probe_recent(ctx, big, retention_ms))
        out.append(await self._probe_exact_count(ctx, "limit=1", 1))
        out.append(await self._probe_at_most(ctx, "limit=5", 5))

        if not paginated:
            out.append(self._skip_anchors(ctx))
            return out

        out.append(await self._probe_anchor_past(ctx, "start=1h_ago", 60 * 60 * 1000))
        if self.period_ms == 0 or self.period_ms <= 12 * 3_600_000:
            out.append(await self._probe_anchor_past(ctx, "start=24h_ago", 24 * 60 * 60 * 1000))
        out.append(await self._probe_anchor_future(ctx))
        return out


    async def _probe_recent(self, ctx: ProbeContext, big: int, retention_ms: Optional[int]) -> ProbeResult:
        data, res = await self._probe(ctx, "recent", {"limit": big}, expect_empty_ok=False)
        if data is None:
            return res

        n = len(data)
        last_ts = data[-1]["timestamp"]
        age_s = (ctx.now_ms - last_ts) / 1000
        ceiling = theoretical_max(self.period_ms, big, retention_ms, ctx.now_ms)
        findings: List[str] = []

        if self.period_ms > 0:
            threshold = freshness_threshold_ms(ctx.exchange, ctx.market_type or "", self.period, self.period_ms)
            if last_ts < ctx.now_ms - threshold:
                findings.append(f"last record stale: {age_s:.0f}s old (threshold {threshold/1000:.0f}s)")

        if n < int(ceiling * 0.9):
            findings.append(f"got {n}/{big} (theoretical max {ceiling}, retention_ms={retention_ms})")

        if findings:
            res.status = "fail" if "stale" in findings[0] else "warn"
            res.error_type = ErrorType.LOGIC
            res.message = " | ".join(findings)
            res.evidence.update({"count": n, "ceiling": ceiling, "age_s": age_s, "retention_ms": retention_ms})
        else:
            res.message = f"got {n}/{big}, last age={age_s:.0f}s"
            res.evidence.update({"count": n, "age_s": age_s, "ceiling": ceiling})
        return res


    async def _probe_exact_count(self, ctx: ProbeContext, sub_name: str, expected: int) -> ProbeResult:
        data, res = await self._probe(ctx, sub_name, {"limit": expected}, expect_empty_ok=True)
        if data is not None and len(data) != expected:
            res.status = "fail"
            res.error_type = ErrorType.LOGIC
            res.message = f"{sub_name} returned {len(data)}"
        return res


    async def _probe_at_most(self, ctx: ProbeContext, sub_name: str, ceiling: int) -> ProbeResult:
        data, res = await self._probe(ctx, sub_name, {"limit": ceiling}, expect_empty_ok=True)
        if data is not None and len(data) > ceiling:
            res.status = "fail"
            res.error_type = ErrorType.LOGIC
            res.message = f"{sub_name} returned {len(data)}"
        return res


    def _skip_anchors(self, ctx: ProbeContext) -> ProbeResult:
        return ProbeResult(
            exchange=ctx.exchange,
            market_type=ctx.market_type or "",
            route=self.route,
            kind=self.kind,
            symbol=ctx.symbol,
            probe=f"{self.name}:start_anchor",
            status="skip",
            error_type=ErrorType.SKIPPED,
            message="upstream does not honour start parameter",
            evidence={"paginated": False},
            latency_ms=0.0,
            started_at=time.time(),
            ended_at=time.time(),
        )


    async def _probe_anchor_past(self, ctx: ProbeContext, sub_name: str, offset_ms: int) -> ProbeResult:
        anchor = ctx.now_ms - offset_ms
        data, res = await self._probe(ctx, sub_name, {"start": anchor, "limit": 500}, expect_empty_ok=True)
        if data is None:
            return res

        last_ts = data[-1]["timestamp"]
        if last_ts > anchor:
            res.status = "fail"
            res.error_type = ErrorType.LOGIC
            res.message = f"upstream ignored start: last record {(last_ts - anchor)/1000:.0f}s after anchor"
            res.evidence.update({"anchor_ms": anchor, "last_ts": last_ts, "drift_s": (last_ts - anchor) / 1000})
        elif self.period_ms > 0 and last_ts < anchor - 2 * self.period_ms:
            res.status = "warn"
            res.error_type = ErrorType.LOGIC
            res.message = f"last record {(anchor - last_ts)/1000:.0f}s before anchor (>2 periods)"
            res.evidence.update({"anchor_ms": anchor, "last_ts": last_ts})
        else:
            res.message = f"{len(data)} records ending {(anchor - last_ts)/1000:.0f}s before anchor"
        return res


    async def _probe_anchor_future(self, ctx: ProbeContext) -> ProbeResult:
        anchor = ctx.now_ms + 60 * 60 * 1000
        data, res = await self._probe(ctx, "start=future", {"start": anchor, "limit": 100}, expect_empty_ok=False)
        if data is None:
            return res

        last_ts = data[-1]["timestamp"]
        fresh_now = int(time.time() * 1000)
        tolerance = max(self.period_ms, 60_000)
        if last_ts > fresh_now + tolerance:
            res.status = "fail"
            res.error_type = ErrorType.LOGIC
            res.message = f"future-dated record returned: last_ts {(last_ts - fresh_now)/1000:.0f}s ahead of now (tolerance {tolerance/1000:.0f}s)"
            res.evidence.update({
                "future_anchor": anchor,
                "last_ts":       last_ts,
                "fresh_now_ms":  fresh_now,
                "tolerance_ms":  tolerance,
                "drift_s":       (last_ts - fresh_now) / 1000,
            })
        else:
            drift_s = (last_ts - fresh_now) / 1000
            rel = f"{drift_s:.0f}s ahead of now" if drift_s > 0 else f"{-drift_s:.0f}s before now"
            res.message = f"got {len(data)} records, last_ts {rel}"
            res.evidence.update({"future_anchor": anchor, "count": len(data), "last_ts": last_ts, "fresh_now_ms": fresh_now})
        return res
