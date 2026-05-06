from collections import defaultdict
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any, Dict, List, Literal, Optional


class ErrorType(str, Enum):
    OK            = "ok"
    NETWORK       = "network"
    HTTP          = "http"
    SCHEMA        = "schema"
    LOGIC         = "logic"
    EMPTY         = "empty"
    TIMEOUT       = "timeout"
    UNIMPLEMENTED = "unimplemented"
    SKIPPED       = "skipped"


Status = Literal["pass", "warn", "fail", "skip"]


@dataclass
class ProbeResult:
    exchange    : str
    market_type : str
    route       : str
    kind        : str
    symbol      : Optional[str]
    probe       : str
    status      : Status
    error_type  : ErrorType
    message     : str
    evidence    : Dict[str, Any] = field(default_factory=dict)
    latency_ms  : float          = 0.0
    started_at  : float          = 0.0
    ended_at    : float          = 0.0


    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["error_type"] = self.error_type.value
        return d


def _percentile(values: List[float], pct: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    s = sorted(values)
    k = (len(s) - 1) * pct
    lo = int(k)
    hi = min(lo + 1, len(s) - 1)
    frac = k - lo
    return s[lo] + (s[hi] - s[lo]) * frac


class Aggregator:
    def __init__(self) -> None:
        self.results: List[ProbeResult] = []


    def add(self, result: ProbeResult) -> None:
        self.results.append(result)


    def extend(self, results: List[ProbeResult]) -> None:
        self.results.extend(results)


    def totals(self) -> Dict[str, int]:
        out = {"pass": 0, "warn": 0, "fail": 0, "skip": 0}
        for r in self.results:
            out[r.status] = out.get(r.status, 0) + 1
        return out


    def by_error_type(self) -> Dict[str, int]:
        out: Dict[str, int] = defaultdict(int)
        for r in self.results:
            out[r.error_type.value] += 1
        return dict(out)


    def by_exchange(self) -> Dict[str, Dict[str, int]]:
        out: Dict[str, Dict[str, int]] = defaultdict(lambda: {"pass": 0, "warn": 0, "fail": 0, "skip": 0})
        for r in self.results:
            out[r.exchange][r.status] += 1
        return {k: dict(v) for k, v in out.items()}


    def rest_latency_by_route(self) -> Dict[str, Dict[str, float]]:
        buckets: Dict[str, List[float]] = defaultdict(list)
        for r in self.results:
            if r.kind != "rest" or r.status == "skip" or r.latency_ms <= 0:
                continue
            buckets[r.route].append(r.latency_ms)
        out: Dict[str, Dict[str, float]] = {}
        for route, vals in buckets.items():
            out[route] = {
                "count": len(vals),
                "p50":   _percentile(vals, 0.50),
                "p95":   _percentile(vals, 0.95),
                "p99":   _percentile(vals, 0.99),
                "max":   max(vals),
            }
        return out


    def ws_by_channel(self) -> Dict[str, Dict[str, Any]]:
        buckets: Dict[str, Dict[str, List[float]]] = defaultdict(lambda: {"frames": [], "first_frame_ms": []})
        for r in self.results:
            if r.kind != "ws" or r.status == "skip":
                continue
            ev = r.evidence or {}
            channel = ev.get("channel") or r.route
            frames = ev.get("frames_received")
            ffm    = ev.get("first_frame_ms")
            if frames is not None:
                buckets[channel]["frames"].append(float(frames))
            if ffm is not None:
                buckets[channel]["first_frame_ms"].append(float(ffm))

        out: Dict[str, Dict[str, Any]] = {}
        for channel, data in buckets.items():
            frames = data["frames"]
            ffm    = data["first_frame_ms"]
            out[channel] = {
                "count":              len(frames),
                "frames_p50":         _percentile(frames, 0.50) if frames else 0.0,
                "frames_p95":         _percentile(frames, 0.95) if frames else 0.0,
                "frames_max":         max(frames) if frames else 0.0,
                "first_frame_ms_p50": _percentile(ffm, 0.50) if ffm else None,
                "first_frame_ms_p95": _percentile(ffm, 0.95) if ffm else None,
            }
        return out


    def summary(self) -> Dict[str, Any]:
        return {
            "totals":                self.totals(),
            "by_error_type":         self.by_error_type(),
            "by_exchange":           self.by_exchange(),
            "rest_latency_by_route": self.rest_latency_by_route(),
            "ws_by_channel":         self.ws_by_channel(),
        }
