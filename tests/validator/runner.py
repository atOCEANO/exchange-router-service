import asyncio
import logging
import time
from typing import Any, Callable, Dict, List, Optional

import httpx

from tests.validator.config import (
    API_URL, MAX_CONCURRENT_EXCHANGES, MAX_CONCURRENT_REST_PER_EXCHANGE,
    MAX_CONCURRENT_WS_PER_EXCHANGE, TIMEOUT, pick_symbol,
)
from tests.validator.probes import (
    Probe, ProbeContext, exchange_probes, fetch, market_probes,
)
from tests.validator.results import ErrorType, ProbeResult


logger = logging.getLogger(__name__)


OnResult = Callable[[ProbeResult], None]


def _result(
    *,
    exchange: str,
    route: str,
    probe: str,
    status: str,
    error_type: ErrorType,
    message: str,
    evidence: Optional[Dict[str, Any]] = None,
    market_type: str = "ALL",
    symbol: Optional[str] = None,
    kind: str = "rest",
    started: Optional[float] = None,
    ended: Optional[float] = None,
) -> ProbeResult:
    end = ended if ended is not None else time.time()
    start = started if started is not None else end
    return ProbeResult(
        exchange=exchange,
        market_type=market_type,
        route=route,
        kind=kind,
        symbol=symbol,
        probe=probe,
        status=status,
        error_type=error_type,
        message=message,
        evidence=evidence or {},
        latency_ms=(end - start) * 1000.0,
        started_at=start,
        ended_at=end,
    )


def _global(probe: str, status: str, error_type: ErrorType, message: str, evidence: Optional[Dict[str, Any]] = None) -> ProbeResult:
    return _result(
        exchange="GLOBAL",
        route="global",
        probe=probe,
        status=status,
        error_type=error_type,
        message=message,
        evidence=evidence,
    )


async def _run_probe(probe: Probe, ctx: ProbeContext, on_result: OnResult, sem: asyncio.Semaphore) -> List[ProbeResult]:
    async with sem:
        try:
            results = await probe.run(ctx)
        except Exception as e:
            logger.exception(f"probe {probe.name} crashed on {ctx.exchange}/{ctx.market_type}")
            results = [probe.result(
                ctx, probe.name, time.time(),
                status="fail",
                error_type=ErrorType.NETWORK,
                message=f"probe crashed: {e}",
                evidence={"exception": type(e).__name__},
            )]
        for r in results:
            on_result(r)
        return results


async def _pick_symbol_for_market(client: httpx.AsyncClient, exchange: str, market_type: str, on_result: OnResult) -> Optional[str]:
    started = time.time()
    status, data, err = await fetch(client, f"/{exchange}/{market_type}/markets")
    ended = time.time()

    def emit(*, status_v: str, error_type: ErrorType, message: str, evidence: Optional[Dict[str, Any]] = None) -> None:
        on_result(_result(
            exchange=exchange,
            market_type=market_type,
            route="markets",
            probe="markets:list",
            status=status_v,
            error_type=error_type,
            message=message,
            evidence=evidence,
            started=started,
            ended=ended,
        ))

    if err is not None:
        emit(status_v="fail", error_type=ErrorType.NETWORK, message=err)
        return None
    if status != 200 or not isinstance(data, list):
        emit(
            status_v="fail",
            error_type=ErrorType.HTTP if status else ErrorType.NETWORK,
            message=f"markets fetch HTTP {status}",
        )
        return None

    sym = pick_symbol(data)
    if sym is None:
        emit(
            status_v="warn",
            error_type=ErrorType.LOGIC,
            message=f"no BTC/ETH-stable symbol found among {len(data)} markets",
            evidence={"market_count": len(data)},
        )
        return None

    emit(
        status_v="pass",
        error_type=ErrorType.OK,
        message=f"selected {sym} from {len(data)} markets",
        evidence={"selected": sym, "market_count": len(data)},
    )
    return sym


async def _run_for_exchange(client: httpx.AsyncClient, exchange: str, on_result: OnResult, exch_sem: asyncio.Semaphore) -> None:
    async with exch_sem:
        rest_sem = asyncio.Semaphore(MAX_CONCURRENT_REST_PER_EXCHANGE)
        ws_sem   = asyncio.Semaphore(MAX_CONCURRENT_WS_PER_EXCHANGE)

        started = time.time()
        status, _, status_err = await fetch(client, f"/{exchange}/status")
        ended = time.time()
        ok = status == 200
        on_result(_result(
            exchange=exchange,
            route="status",
            probe="status",
            status="pass" if ok else "fail",
            error_type=ErrorType.OK if ok else (ErrorType.NETWORK if status_err else ErrorType.HTTP),
            message="OK" if ok else (status_err or f"HTTP {status}"),
            evidence={"status": status},
            started=started,
            ended=ended,
        ))

        started = time.time()
        cap_status, caps, cap_err = await fetch(client, f"/{exchange}/capabilities")
        ended = time.time()
        if cap_status != 200 or not isinstance(caps, dict):
            on_result(_result(
                exchange=exchange,
                route="capabilities",
                probe="capabilities",
                status="fail",
                error_type=ErrorType.NETWORK if cap_err else ErrorType.HTTP,
                message=cap_err or f"HTTP {cap_status}",
                evidence={"endpoint": f"/{exchange}/capabilities"},
                started=started,
                ended=ended,
            ))
            return
        on_result(_result(
            exchange=exchange,
            route="capabilities",
            probe="capabilities",
            status="pass",
            error_type=ErrorType.OK,
            message="OK",
            evidence={"markets": list(caps.get("markets", {}).keys())},
            started=started,
            ended=ended,
        ))

        ex_ctx = ProbeContext(
            client=client,
            exchange=exchange,
            market_type=None,
            symbol=None,
            caps=caps,
            caps_slice=None,
            now_ms=int(time.time() * 1000),
        )
        ex_tasks = [_run_probe(p, ex_ctx, on_result, rest_sem) for p in exchange_probes() if p.applies(ex_ctx)]
        if ex_tasks:
            await asyncio.gather(*ex_tasks)

        markets_caps: Dict[str, Dict[str, Any]] = caps.get("markets", {}) or {}
        for market_type, slice_caps in markets_caps.items():
            symbol = await _pick_symbol_for_market(client, exchange, market_type, on_result)
            if symbol is None:
                continue

            ctx = ProbeContext(
                client=client,
                exchange=exchange,
                market_type=market_type,
                symbol=symbol,
                caps=caps,
                caps_slice=slice_caps,
                now_ms=int(time.time() * 1000),
            )
            probes = [p for p in market_probes(slice_caps) if p.applies(ctx)]

            rest_tasks = [_run_probe(p, ctx, on_result, rest_sem) for p in probes if p.kind == "rest"]
            ws_tasks   = [_run_probe(p, ctx, on_result, ws_sem)   for p in probes if p.kind == "ws"]

            await asyncio.gather(*rest_tasks, *ws_tasks)


async def run(exchanges: List[str], on_result: OnResult, max_concurrent_exchanges: Optional[int] = None) -> None:
    concurrency = max_concurrent_exchanges if max_concurrent_exchanges is not None else MAX_CONCURRENT_EXCHANGES
    logger.info(f"validator: target {API_URL}, exchanges={exchanges}, concurrency={concurrency}")

    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        s, data, err = await fetch(client, "/status")
        if s != 200:
            on_result(_global(
                "service_status", "fail",
                ErrorType.NETWORK if err else ErrorType.HTTP,
                err or f"HTTP {s}",
                {"endpoint": "/status"},
            ))
            return
        on_result(_global("service_status", "pass", ErrorType.OK, "OK"))

        s, ver, err = await fetch(client, "/version")
        if s == 200 and isinstance(ver, dict) and "version" in ver:
            on_result(_global("version", "pass", ErrorType.OK, f"v{ver['version']}", {"version": ver["version"]}))
        else:
            on_result(_global(
                "version", "fail",
                ErrorType.NETWORK if err else ErrorType.HTTP,
                err or f"HTTP {s}",
            ))

        s, listing, err = await fetch(client, "/exchanges")
        if s != 200 or not isinstance(listing, dict):
            on_result(_global(
                "list_exchanges", "fail",
                ErrorType.NETWORK if err else ErrorType.HTTP,
                err or f"HTTP {s}",
            ))
            return

        available = listing.get("exchanges", []) or []
        if not available:
            on_result(_global("list_exchanges", "fail", ErrorType.LOGIC, "no exchanges returned"))
            return
        on_result(_global("list_exchanges", "pass", ErrorType.OK, f"found {len(available)}", {"exchanges": available}))

        if exchanges:
            unknown = [e for e in exchanges if e not in available]
            if unknown:
                on_result(_global(
                    "filter_exchanges", "warn", ErrorType.LOGIC,
                    f"unknown exchange(s) {unknown}; available {available}",
                    {"unknown": unknown, "available": available},
                ))
            target = [e for e in available if e in set(exchanges)]
        else:
            target = list(available)

        if not target:
            on_result(_global(
                "filter_exchanges", "fail", ErrorType.LOGIC,
                "no matching exchanges to test",
                {"requested": exchanges, "available": available},
            ))
            return

        exch_sem = asyncio.Semaphore(concurrency)
        await asyncio.gather(*[_run_for_exchange(client, ex, on_result, exch_sem) for ex in target])
