import time
from typing import Any, Callable, Dict, List, Tuple, Type

from pydantic import BaseModel

from tests.validator.config import SNAPSHOT_FRESHNESS_MS
from tests.validator.results import ErrorType, ProbeResult

from tests.validator.probes.base import (
    Probe, ProbeContext, fetch, now_ms, validate_one,
)


def _ticker_checks(d: dict) -> Tuple[str, ErrorType, str, Dict[str, Any]]:
    price = d["price"]
    low, high = d["low_24h"], d["high_24h"]
    if price <= 0:
        return "fail", ErrorType.LOGIC, f"non-positive price {price}", {"price": price}
    if high > 0 and low > 0 and not (low <= price <= high):
        return "warn", ErrorType.LOGIC, f"price {price} outside 24h range [{low}, {high}]", {"price": price, "low": low, "high": high}
    if d.get("volume_24h", 0) < 0:
        return "fail", ErrorType.LOGIC, f"negative volume {d['volume_24h']}", {"volume": d["volume_24h"]}
    age_ms = now_ms() - d["timestamp"]
    if age_ms > SNAPSHOT_FRESHNESS_MS:
        return "warn", ErrorType.LOGIC, f"stale: {age_ms/1000:.0f}s old", {"age_ms": age_ms, "timestamp": d["timestamp"]}
    return "pass", ErrorType.OK, f"price={price}, vol24h={d['volume_24h']:.4g}", {"price": price, "age_ms": age_ms}


def _book_ticker_checks(d: dict) -> Tuple[str, ErrorType, str, Dict[str, Any]]:
    bid, ask = d["bid_price"], d["ask_price"]
    bid_qty, ask_qty = d["bid_qty"], d["ask_qty"]
    if bid <= 0 or ask <= 0:
        return "fail", ErrorType.LOGIC, f"non-positive price: bid={bid}, ask={ask}", {"bid": bid, "ask": ask}
    if bid > ask:
        return "fail", ErrorType.LOGIC, f"crossed book: bid {bid} > ask {ask}", {"bid": bid, "ask": ask}
    if bid_qty <= 0 or ask_qty <= 0:
        return "fail", ErrorType.LOGIC, f"non-positive qty: bid_qty={bid_qty}, ask_qty={ask_qty}", {"bid_qty": bid_qty, "ask_qty": ask_qty}
    age_ms = now_ms() - d["timestamp"]
    if age_ms > SNAPSHOT_FRESHNESS_MS:
        return "warn", ErrorType.LOGIC, f"stale: {age_ms/1000:.0f}s old", {"age_ms": age_ms}
    spread = ask - bid
    return "pass", ErrorType.OK, f"bid={bid}, ask={ask}, spread={spread:.6g}", {"bid": bid, "ask": ask, "spread": spread}


def _mark_price_checks(d: dict) -> Tuple[str, ErrorType, str, Dict[str, Any]]:
    mp = d["mark_price"]
    ip = d["index_price"]
    if mp <= 0 or ip <= 0:
        return "fail", ErrorType.LOGIC, f"non-positive: mark={mp}, index={ip}", {"mark": mp, "index": ip}
    age_ms = now_ms() - d["timestamp"]
    if age_ms > SNAPSHOT_FRESHNESS_MS:
        return "warn", ErrorType.LOGIC, f"stale: {age_ms/1000:.0f}s old", {"age_ms": age_ms}
    return "pass", ErrorType.OK, f"mark={mp}, index={ip}, fr={d['funding_rate']}", {"mark": mp, "index": ip, "funding_rate": d["funding_rate"]}


SNAPSHOT_CHECKS: Dict[str, Callable[[dict], Tuple[str, ErrorType, str, Dict[str, Any]]]] = {
    "ticker":      _ticker_checks,
    "book_ticker": _book_ticker_checks,
    "mark_price":  _mark_price_checks,
}


class SnapshotProbe(Probe):
    kind = "rest"


    def __init__(self, route: str, model: Type[BaseModel]):
        self.route = route
        self.name  = route
        self.model = model
        self.checks = SNAPSHOT_CHECKS.get(route)


    def applies(self, ctx: ProbeContext) -> bool:
        return bool(ctx.caps_slice and ctx.caps_slice.get(self.route, {}).get("rest"))


    async def run(self, ctx: ProbeContext) -> List[ProbeResult]:
        endpoint = f"/{ctx.exchange}/{ctx.market_type}/{self.route}/{ctx.symbol}"
        started = time.time()
        status, data, err = await fetch(ctx.client, endpoint)

        if status != 200:
            return [self.http_failure(ctx, self.name, started, status, data, err)]

        shape_err = validate_one(data, self.model)
        if shape_err:
            msg, ev = shape_err
            return [self.result(
                ctx, self.name, started,
                status="fail",
                error_type=ErrorType.SCHEMA,
                message=msg,
                evidence=ev,
            )]

        if self.checks is None:
            return [self.result(
                ctx, self.name, started,
                status="pass",
                error_type=ErrorType.OK,
                message="parsed OK",
            )]

        s, et, msg, ev = self.checks(data)
        return [self.result(
            ctx, self.name, started,
            status=s,
            error_type=et,
            message=msg,
            evidence=ev,
        )]
