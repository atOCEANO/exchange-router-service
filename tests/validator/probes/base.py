import asyncio
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Type

import httpx
from pydantic import BaseModel, ValidationError

from tests.validator.config import API_URL, MIN_REST_INTERVAL_MS
from tests.validator.results import ErrorType, ProbeResult


@dataclass
class ProbeContext:
    client      : httpx.AsyncClient
    exchange    : str
    market_type : Optional[str]
    symbol      : Optional[str]
    caps        : Dict[str, Any]
    caps_slice  : Optional[Dict[str, Any]]
    now_ms      : int


def now_ms() -> int:
    return int(time.time() * 1000)


_rest_next_slot_at: Dict[str, float] = {}
_rest_throttle_lock = asyncio.Lock()


def _exchange_from_endpoint(endpoint: str) -> Optional[str]:
    parts = endpoint.lstrip("/").split("/", 1)
    if len(parts) < 2 or not parts[0] or parts[0] == "ws":
        return None
    return parts[0]


async def _await_rest_spacing(endpoint: str) -> None:
    if MIN_REST_INTERVAL_MS <= 0:
        return
    exchange = _exchange_from_endpoint(endpoint)
    if not exchange:
        return
    interval_s = MIN_REST_INTERVAL_MS / 1000.0
    async with _rest_throttle_lock:
        now = time.monotonic()
        next_slot = max(now, _rest_next_slot_at.get(exchange, 0.0) + interval_s)
        _rest_next_slot_at[exchange] = next_slot
        wait = next_slot - now
    if wait > 0:
        await asyncio.sleep(wait)


async def fetch(client: httpx.AsyncClient, endpoint: str, params: Optional[Dict] = None) -> Tuple[Optional[int], Any, Optional[str]]:
    await _await_rest_spacing(endpoint)
    full_url = f"{API_URL}{endpoint}"
    try:
        response = await client.get(full_url, params=params)
        try:
            data = response.json()
        except Exception:
            data = response.text
        return response.status_code, data, None
    except httpx.TimeoutException as e:
        return None, None, f"timeout: {e}"
    except httpx.RequestError as e:
        return None, None, f"network: {e}"
    except Exception as e:
        return None, None, f"unexpected: {e}"


def validate_one(data: Any, model: Type[BaseModel]) -> Optional[Tuple[str, Dict[str, Any]]]:
    if not isinstance(data, dict):
        return f"{model.__name__}: expected dict, got {type(data).__name__}", {"got_type": type(data).__name__}
    try:
        model.model_validate(data)
        return None
    except ValidationError as e:
        first = e.errors()[0]
        loc = ".".join(str(x) for x in first["loc"]) or "<root>"
        return f"{model.__name__}.{loc}: {first['msg']}", {"field": loc, "error": first["msg"], "input": first.get("input")}


def validate_list(data: Any, model: Type[BaseModel], sample: int = 5) -> Optional[Tuple[str, Dict[str, Any]]]:
    if not isinstance(data, list):
        return f"expected list, got {type(data).__name__}", {"got_type": type(data).__name__}
    if not data:
        return None
    n = len(data)
    idxs = sorted({0, n - 1, n // 2, n // 4, 3 * n // 4})[:sample]
    for i in idxs:
        err = validate_one(data[i], model)
        if err:
            msg, ev = err
            return f"item[{i}/{n}] {msg}", {"index": i, "total": n, **ev}
    return None


def check_ascending_ts(items: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    for i in range(len(items) - 1):
        a = items[i].get("timestamp")
        b = items[i + 1].get("timestamp")
        if a is None or b is None:
            continue
        if a > b:
            return {"idx": i, "prev_ts": a, "this_ts": b, "diff_ms": a - b}
    return None


class Probe(ABC):
    name  : str
    kind  : str
    route : str


    @abstractmethod
    def applies(self, ctx: ProbeContext) -> bool: ...


    @abstractmethod
    async def run(self, ctx: ProbeContext) -> List[ProbeResult]: ...


    def result(
        self,
        ctx: ProbeContext,
        probe_name: str,
        started: float,
        *,
        status: str,
        error_type: ErrorType,
        message: str,
        evidence: Optional[Dict[str, Any]] = None,
        ended: Optional[float] = None,
    ) -> ProbeResult:
        end = ended if ended is not None else time.time()
        return ProbeResult(
            exchange=ctx.exchange,
            market_type=ctx.market_type or "ALL",
            route=self.route,
            kind=self.kind,
            symbol=ctx.symbol,
            probe=probe_name,
            status=status,
            error_type=error_type,
            message=message,
            evidence=evidence or {},
            latency_ms=(end - started) * 1000.0,
            started_at=started,
            ended_at=end,
        )


    def http_failure(
        self,
        ctx: ProbeContext,
        probe_name: str,
        started: float,
        status_code: Optional[int],
        data: Any,
        net_err: Optional[str],
        expected: str = "200",
    ) -> ProbeResult:
        if net_err is not None:
            return self.result(
                ctx, probe_name, started,
                status="fail",
                error_type=ErrorType.NETWORK,
                message=net_err,
                evidence={"endpoint": self.route, "expected": expected},
            )
        if status_code == 501:
            return self.result(
                ctx, probe_name, started,
                status="fail",
                error_type=ErrorType.UNIMPLEMENTED,
                message="501 Not Implemented (unexpected)",
                evidence={"endpoint": self.route, "status": 501},
            )
        snippet = str(data)[:200] if data is not None else ""
        return self.result(
            ctx, probe_name, started,
            status="fail",
            error_type=ErrorType.HTTP,
            message=f"HTTP {status_code}: {snippet}",
            evidence={"endpoint": self.route, "status": status_code, "body_preview": snippet, "expected": expected},
        )
