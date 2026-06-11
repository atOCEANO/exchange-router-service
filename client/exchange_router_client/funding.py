from typing import Any, List, Mapping, Tuple

import pandas as pd


def _rate_fields(row: Any) -> Tuple[str, float, float]:
    if isinstance(row, Mapping):
        block = row.get("rate") or row.get("funding")
        if isinstance(block, Mapping):
            return block["kind"], float(block["per_cycle"]), float(block["cycle_ms"])
        if "rate_per_cycle" in row:
            return row["rate_kind"], float(row["rate_per_cycle"]), float(row["rate_cycle_ms"])

    if isinstance(row, pd.Series):
        if "rate_per_cycle" in row.index:
            return row["rate_kind"], float(row["rate_per_cycle"]), float(row["rate_cycle_ms"])
        if "funding_per_cycle" in row.index:
            return row["funding_kind"], float(row["funding_per_cycle"]), float(row["funding_cycle_ms"])

    raise ValueError("Unrecognized funding row shape; expected a wire row, a flattened row, or a funding DataFrame row")


def _normalized_rows(rows: Any) -> List[Tuple[int, str, float, float]]:
    if isinstance(rows, pd.DataFrame):
        out = []
        for idx, row in rows.iterrows():
            kind, per_cycle, cycle_ms = _rate_fields(row)
            out.append((int(pd.Timestamp(idx).value // 1_000_000), kind, per_cycle, cycle_ms))
        return out

    out = []
    for r in rows:
        kind, per_cycle, cycle_ms = _rate_fields(r)
        out.append((int(r["timestamp"]), kind, per_cycle, cycle_ms))
    return out


def per_hour_view(funding_row: Any) -> float:
    _, per_cycle, cycle_ms = _rate_fields(funding_row)
    return per_cycle / (cycle_ms / 3_600_000)


def funding_paid(rows: Any, t_open: int, t_close: int, notional: float) -> float:
    paid = 0.0

    for ts, kind, per_cycle, cycle_ms in _normalized_rows(rows):
        if kind == "discrete":
            if t_open <= ts <= t_close:
                paid += notional * per_cycle
        else:
            window_start = ts
            window_end   = ts + cycle_ms
            overlap_ms   = max(0, min(window_end, t_close) - max(window_start, t_open))
            paid += notional * per_cycle * (overlap_ms / cycle_ms)

    return paid
