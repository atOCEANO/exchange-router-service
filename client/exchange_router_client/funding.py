from typing import Iterable, Mapping


def per_hour_view(funding_row: Mapping) -> float:
    r = funding_row["rate"]
    return r["per_cycle"] / (r["cycle_ms"] / 3_600_000)


def funding_paid(rows: Iterable[Mapping], t_open: int, t_close: int, notional: float) -> float:
    paid = 0.0
    for r in rows:
        f = r["rate"]
        if f["kind"] == "discrete":
            if t_open <= r["timestamp"] <= t_close:
                paid += notional * f["per_cycle"]
        else:
            window_start = r["timestamp"]
            window_end   = r["timestamp"] + f["cycle_ms"]
            overlap_ms   = max(0, min(window_end, t_close) - max(window_start, t_open))
            paid += notional * f["per_cycle"] * (overlap_ms / f["cycle_ms"])
    return paid
