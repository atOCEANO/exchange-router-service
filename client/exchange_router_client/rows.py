from typing import Any, Dict, Optional


class Row(dict):

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)


    @property
    def raw(self) -> Optional[dict]:
        return getattr(self, "_raw", None)


def _qty(obj: Any):
    if not isinstance(obj, dict):
        return None, None, None
    return obj.get("native"), obj.get("usd"), obj.get("unit")


def _wrap(flat: Dict[str, Any], raw: Dict[str, Any]) -> Row:
    row = Row(flat)
    row._raw = raw
    return row


def ticker_row(raw: Dict[str, Any]) -> Row:
    native, usd, unit = _qty(raw.get("volume_24h"))

    flat = {
        "symbol":               raw.get("symbol"),
        "market_type":          raw.get("market_type"),
        "quote":                raw.get("quote"),
        "price":                raw.get("price"),
        "open_24h":             raw.get("open_24h"),
        "high_24h":             raw.get("high_24h"),
        "low_24h":              raw.get("low_24h"),
        "volume_24h":           native,
        "volume_24h_usd":       usd,
        "volume_24h_unit":      unit,
        "price_change_percent": raw.get("price_change_percent"),
        "timestamp":            raw.get("timestamp"),
    }

    return _wrap(flat, raw)


def book_ticker_row(raw: Dict[str, Any]) -> Row:
    bid_native, bid_usd, unit = _qty(raw.get("bid_qty"))
    ask_native, ask_usd, _    = _qty(raw.get("ask_qty"))

    flat = {
        "symbol":      raw.get("symbol"),
        "market_type": raw.get("market_type"),
        "quote":       raw.get("quote"),
        "bid_price":   raw.get("bid_price"),
        "bid_qty":     bid_native,
        "bid_qty_usd": bid_usd,
        "ask_price":   raw.get("ask_price"),
        "ask_qty":     ask_native,
        "ask_qty_usd": ask_usd,
        "qty_unit":    unit,
        "timestamp":   raw.get("timestamp"),
    }

    return _wrap(flat, raw)


def mark_price_row(raw: Dict[str, Any]) -> Row:
    funding   = raw.get("funding") or {}
    per_cycle = funding.get("per_cycle")
    cycle_ms  = funding.get("cycle_ms")

    if per_cycle is not None and cycle_ms:
        per_hour = per_cycle / (cycle_ms / 3_600_000)
    else:
        per_hour = None

    flat = {
        "symbol":                 raw.get("symbol"),
        "market_type":            raw.get("market_type"),
        "quote":                  raw.get("quote"),
        "mark_price":             raw.get("mark_price"),
        "index_price":            raw.get("index_price"),
        "funding_kind":           funding.get("kind"),
        "funding_per_cycle":      per_cycle,
        "funding_cycle_ms":       cycle_ms,
        "funding_per_hour":       per_hour,
        "funding_valid_until_ts": funding.get("valid_until_ts") if funding else None,
        "timestamp":              raw.get("timestamp"),
    }

    return _wrap(flat, raw)


def symbol_info_row(raw: Dict[str, Any]) -> Row:
    funding = raw.get("funding")

    flat = {k: v for k, v in raw.items() if k != "funding"}
    flat["funding_kind"] = funding.get("kind") if isinstance(funding, dict) else None

    return _wrap(flat, raw)
