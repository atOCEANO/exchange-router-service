import asyncio
import logging
import time
from typing import Any, Callable, Dict, List, Optional

import httpx

from src.version import SCHEMA_VERSION
from tools.auditor.config import (
    API_URL, MAX_CONCURRENT_EXCHANGES, MAX_CONCURRENT_REST_PER_EXCHANGE,
    MAX_CONCURRENT_WS_PER_EXCHANGE, TIMEOUT, pick_symbol,
)
from tools.auditor.probes import (
    Probe, ProbeContext, exchange_probes, fetch, market_probes,
)
from tools.auditor.aggregator import ErrorType, ProbeResult


logger = logging.getLogger(__name__)


OnResult           = Callable[[ProbeResult], None]
OnRegisterExchange = Callable[[str, int], None]
OnProbeDone        = Callable[[str], None]
OnSkipMarket       = Callable[[str, int], None]


def _noop_register(_e: str, _n: int) -> None: ...


def _noop_done(_e: str) -> None: ...


def _noop_skip(_e: str, _n: int) -> None: ...


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


async def _run_probe(probe: Probe, ctx: ProbeContext, on_result: OnResult, on_probe_done: OnProbeDone, sem: asyncio.Semaphore, exchange: str) -> List[ProbeResult]:
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

        on_probe_done(exchange)
        return results


def _count_expected_probes(exchange: str, caps: Dict[str, Any]) -> int:
    now_ms = int(time.time() * 1000)

    ex_ctx = ProbeContext(
        client=None, exchange=exchange, market_type=None, symbol=None,
        caps=caps, caps_slice=None, now_ms=now_ms,
    )
    count = sum(1 for p in exchange_probes() if p.applies(ex_ctx))

    markets_caps = caps.get("markets", {}) or {}
    for market_type, slice_caps in markets_caps.items():
        count += _count_market_probes(exchange, market_type, caps, slice_caps)
    return count


def _count_market_probes(exchange: str, market_type: str, caps: Dict[str, Any], slice_caps: Dict[str, Any]) -> int:
    ctx = ProbeContext(
        client=None, exchange=exchange, market_type=market_type,
        symbol="BTCUSDT", caps=caps, caps_slice=slice_caps,
        now_ms=int(time.time() * 1000),
    )
    return sum(1 for p in market_probes(slice_caps) if p.applies(ctx))


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
    if status != 200 or not isinstance(data, dict) or "markets" not in data:
        emit(
            status_v="fail",
            error_type=ErrorType.HTTP if status else ErrorType.NETWORK,
            message=f"markets fetch HTTP {status}",
        )
        return None

    entries = data["markets"]
    symbols = [entry.get("symbol") for entry in entries if isinstance(entry, dict) and entry.get("symbol")]

    sym = pick_symbol(symbols)
    if sym is None:
        emit(
            status_v="warn",
            error_type=ErrorType.LOGIC,
            message=f"no BTC/ETH-stable symbol found among {len(symbols)} markets",
            evidence={"market_count": len(symbols)},
        )
        return None

    emit(
        status_v="pass",
        error_type=ErrorType.OK,
        message=f"selected {sym} from {len(symbols)} markets",
        evidence={"selected": sym, "market_count": len(symbols)},
    )
    return sym


async def _run_for_exchange(client: httpx.AsyncClient, exchange: str, caps: Optional[Dict[str, Any]], on_result: OnResult, on_probe_done: OnProbeDone, on_skip_market: OnSkipMarket, exch_sem: asyncio.Semaphore) -> None:
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

        if caps is None:
            return

        ex_ctx = ProbeContext(
            client=client,
            exchange=exchange,
            market_type=None,
            symbol=None,
            caps=caps,
            caps_slice=None,
            now_ms=int(time.time() * 1000),
        )
        ex_probes = [p for p in exchange_probes() if p.applies(ex_ctx)]
        if ex_probes:
            await asyncio.gather(*[_run_probe(p, ex_ctx, on_result, on_probe_done, rest_sem, exchange) for p in ex_probes])

        markets_caps: Dict[str, Dict[str, Any]] = caps.get("markets", {}) or {}
        for market_type, slice_caps in markets_caps.items():
            symbol = await _pick_symbol_for_market(client, exchange, market_type, on_result)
            if symbol is None:
                skipped = _count_market_probes(exchange, market_type, caps, slice_caps)
                if skipped > 0:
                    on_skip_market(exchange, skipped)
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
            if not probes:
                continue

            rest_tasks = [_run_probe(p, ctx, on_result, on_probe_done, rest_sem, exchange) for p in probes if p.kind == "rest"]
            ws_tasks   = [_run_probe(p, ctx, on_result, on_probe_done, ws_sem,   exchange) for p in probes if p.kind == "ws"]

            await asyncio.gather(*rest_tasks, *ws_tasks)


async def run(
    exchanges               : List[str],
    on_result               : OnResult,
    *,
    on_register_exchange    : OnRegisterExchange = _noop_register,
    on_probe_done           : OnProbeDone        = _noop_done,
    on_skip_market          : OnSkipMarket       = _noop_skip,
    max_concurrent_exchanges: Optional[int]      = None,
) -> None:
    concurrency = max_concurrent_exchanges if max_concurrent_exchanges is not None else MAX_CONCURRENT_EXCHANGES
    logger.debug(f"auditor: target {API_URL}, exchanges={exchanges}, concurrency={concurrency}")

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

        s, root, err = await fetch(client, "/")
        if s == 200 and isinstance(root, dict) and "schema_version" in root:
            served = root["schema_version"]
            if served == SCHEMA_VERSION:
                on_result(_global(
                    "schema_version", "pass", ErrorType.OK,
                    f"v{served} matches auditor",
                    {"served": served, "expected": SCHEMA_VERSION},
                ))
            else:
                on_result(_global(
                    "schema_version", "fail", ErrorType.LOGIC,
                    f"drift: service v{served}, auditor v{SCHEMA_VERSION}",
                    {"served": served, "expected": SCHEMA_VERSION},
                ))
        else:
            on_result(_global(
                "schema_version", "warn", ErrorType.LOGIC,
                err or f"GET / returned HTTP {s} or missing schema_version",
                {"served": None, "expected": SCHEMA_VERSION},
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

        async def _fetch_caps(ex: str):
            started_at = time.time()
            cap_s, cap_data, cap_err = await fetch(client, f"/{ex}/capabilities")
            ended_at = time.time()
            return ex, cap_s, cap_data, cap_err, started_at, ended_at

        cap_results = await asyncio.gather(*[_fetch_caps(ex) for ex in target])

        caps_map: Dict[str, Dict[str, Any]] = {}
        for ex, cap_s, cap_data, cap_err, started_at, ended_at in cap_results:
            if cap_s == 200 and isinstance(cap_data, dict):
                caps_map[ex] = cap_data
                expected = _count_expected_probes(ex, cap_data)
                on_register_exchange(ex, expected)

                caps_result = _result(
                    exchange=ex,
                    route="capabilities",
                    probe="capabilities",
                    status="pass",
                    error_type=ErrorType.OK,
                    message="OK",
                    evidence={"markets": list(cap_data.get("markets", {}).keys())},
                    started=started_at,
                    ended=ended_at,
                )
                caps_result.sample = cap_data
                on_result(caps_result)
            else:
                on_result(_result(
                    exchange=ex,
                    route="capabilities",
                    probe="capabilities",
                    status="fail",
                    error_type=ErrorType.NETWORK if cap_err else ErrorType.HTTP,
                    message=cap_err or f"HTTP {cap_s}",
                    evidence={"endpoint": f"/{ex}/capabilities"},
                    started=started_at,
                    ended=ended_at,
                ))

        exch_sem = asyncio.Semaphore(concurrency)
        await asyncio.gather(*[
            _run_for_exchange(client, ex, caps_map.get(ex), on_result, on_probe_done, on_skip_market, exch_sem)
            for ex in target
        ])
