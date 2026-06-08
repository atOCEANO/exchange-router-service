import time
from typing import Any, Callable, Dict, List, Tuple, Type

from pydantic import BaseModel

from tools.auditor.config import SNAPSHOT_FRESHNESS_MS
from tools.auditor.aggregator import ErrorType, ProbeResult

from tools.auditor.probes.base import (
    Probe, ProbeContext, fetch, now_ms, validate_one,
)


def _ticker_checks(d: dict) -> Tuple[str, ErrorType, str, Dict[str, Any]]:
    price     = d["price"]
    low, high = d["low_24h"], d["high_24h"]
    vol_obj   = d.get("volume_24h") or {}
    vol_native = vol_obj.get("native", 0) if isinstance(vol_obj, dict) else 0
    if price <= 0:
        return "fail", ErrorType.LOGIC, f"non-positive price {price}", {"price": price}
    if high > 0 and low > 0 and not (low <= price <= high):
        return "warn", ErrorType.LOGIC, f"price {price} outside 24h range [{low}, {high}]", {"price": price, "low": low, "high": high}
    if vol_native < 0:
        return "fail", ErrorType.LOGIC, f"negative volume {vol_native}", {"volume": vol_native}
    age_ms = now_ms() - d["timestamp"]
    if age_ms > SNAPSHOT_FRESHNESS_MS:
        return "warn", ErrorType.LOGIC, f"stale: {age_ms/1000:.0f}s old", {"age_ms": age_ms, "timestamp": d["timestamp"]}
    return "pass", ErrorType.OK, f"price={price}, vol24h={vol_native:.4g}", {"price": price, "age_ms": age_ms}


def _book_ticker_checks(d: dict) -> Tuple[str, ErrorType, str, Dict[str, Any]]:
    bid, ask = d["bid_price"], d["ask_price"]
    bid_qty_obj = d.get("bid_qty") or {}
    ask_qty_obj = d.get("ask_qty") or {}
    bid_qty = bid_qty_obj.get("native", 0) if isinstance(bid_qty_obj, dict) else 0
    ask_qty = ask_qty_obj.get("native", 0) if isinstance(ask_qty_obj, dict) else 0
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
    ip = d.get("index_price")
    funding_obj = d.get("funding")
    fr = funding_obj.get("per_cycle") if isinstance(funding_obj, dict) else None
    if mp <= 0:
        return "fail", ErrorType.LOGIC, f"non-positive mark={mp}", {"mark": mp}
    if ip is not None and ip <= 0:
        return "fail", ErrorType.LOGIC, f"non-positive index={ip}", {"index": ip}
    age_ms = now_ms() - d["timestamp"]
    if age_ms > SNAPSHOT_FRESHNESS_MS:
        return "warn", ErrorType.LOGIC, f"stale: {age_ms/1000:.0f}s old", {"age_ms": age_ms}
    return "pass", ErrorType.OK, f"mark={mp}, index={ip}, fr={fr}", {"mark": mp, "index": ip, "funding_rate": fr}


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
            r = self.result(
                ctx, self.name, started,
                status="pass",
                error_type=ErrorType.OK,
                message="parsed OK",
            )
            r.sample = data
            return [r]

        s, et, msg, ev = self.checks(data)
        r = self.result(
            ctx, self.name, started,
            status=s,
            error_type=et,
            message=msg,
            evidence=ev,
        )
        r.sample = data
        return [r]
