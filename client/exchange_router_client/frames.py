from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


SCHEMA_VERSION = 3

USD_FAMILY = {"USD", "USDT", "USDC", "USDD", "TUSD", "FDUSD", "BUSD", "DAI", "PYUSD", "USDE", "USDP"}

TEXT_COLUMNS = {"side", "id", "agg_id", "first_trade_id", "last_trade_id", "exchange", "market_type", "symbol", "quote", "unit"}

COLUMNS: Dict[str, List[str]] = {
    "candles":          ["open", "high", "low", "close", "volume", "volume_usd"],
    "trades":           ["price", "qty", "qty_usd", "side", "id"],
    "agg_trades":       ["price", "qty", "qty_usd", "side", "agg_id", "first_trade_id", "last_trade_id"],
    "funding_rate":     ["rate", "funding_per_hour", "cycle_ms"],
    "open_interest":    ["open_interest", "open_interest_usd"],
    "liquidations":     ["price", "qty", "qty_usd", "side"],
    "long_short_ratio": ["ratio", "long_account", "short_account"],
}


def _base_attrs(ctx: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "exchange":       ctx["exchange"],
        "market_type":    ctx["market_type"],
        "symbol":         ctx["symbol"],
        "schema_version": SCHEMA_VERSION,
        "warnings":       [],
    }


def _indexed(rows: List[Dict]) -> pd.DataFrame:
    df = pd.json_normalize(rows, sep="_")
    df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df = df.set_index("datetime").sort_index()

    return df


def _const(df: pd.DataFrame, column: str) -> Any:
    if column not in df.columns:
        return None

    series = df[column].dropna()
    if series.empty:
        return None

    return series.iloc[0]


def _coerce(df: pd.DataFrame) -> pd.DataFrame:
    for column in df.columns:
        if column in TEXT_COLUMNS:
            continue
        df[column] = pd.to_numeric(df[column], errors="coerce")

    return df


def _finalize(df: pd.DataFrame, attrs: Dict[str, Any], columns: List[str], warnings: List[str]) -> Tuple[pd.DataFrame, List[str]]:
    out = df.reindex(columns=columns).copy()
    out = _coerce(out)

    attrs["warnings"] = warnings
    out.attrs.update(attrs)

    return out, warnings


def _empty(ctx: Dict[str, Any], route: str) -> Tuple[pd.DataFrame, List[str]]:
    message = f"0 rows returned for {ctx['exchange']}/{ctx['market_type']}/{ctx['symbol']} {route}"

    df    = pd.DataFrame(columns=COLUMNS[route])
    attrs = _base_attrs(ctx)
    attrs["warnings"] = [message]
    df.attrs.update(attrs)

    return df, [message]


def _quote_warning(quote: Optional[str], ctx: Dict[str, Any], route: str) -> Optional[str]:
    if quote and quote not in USD_FAMILY:
        return f"{route} {ctx['exchange']}/{ctx['market_type']}/{ctx['symbol']}: quote='{quote}'; *_usd fields are quote-denominated ({quote}), not US dollars"

    return None


def _short_result_warning(df: pd.DataFrame, ctx: Dict[str, Any], route: str) -> Optional[str]:
    requested = ctx.get("requested")
    if not ctx.get("paginated") or requested is None:
        return None

    if 0 < len(df) < requested:
        return f"{route} {ctx['exchange']}/{ctx['market_type']}/{ctx['symbol']}: got {len(df)} of {requested} requested; pass start= to fetch older history"

    return None


def _candles(rows: List[Dict], ctx: Dict[str, Any]) -> Tuple[pd.DataFrame, List[str]]:
    df = _indexed(rows)
    df = df.rename(columns={"volume_native": "volume"})

    attrs = _base_attrs(ctx)
    attrs["quote"]         = _const(df, "quote")
    attrs["interval"]      = _const(df, "interval")
    attrs["volume_unit"]   = _const(df, "volume_unit")
    attrs["contract_size"] = _const(df, "volume_contract_size")
    attrs["usd_basis"]     = _const(df, "volume_usd_basis_method")

    warnings: List[str] = []
    short = _short_result_warning(df, ctx, "candles")
    if short:
        warnings.append(short)
    quote = _quote_warning(attrs["quote"], ctx, "candles")
    if quote:
        warnings.append(quote)

    return _finalize(df, attrs, COLUMNS["candles"], warnings)


def _trades(rows: List[Dict], ctx: Dict[str, Any]) -> Tuple[pd.DataFrame, List[str]]:
    df = _indexed(rows)
    df = df.rename(columns={"qty_native": "qty"})

    attrs = _base_attrs(ctx)
    attrs["quote"]         = _const(df, "quote")
    attrs["qty_unit"]      = _const(df, "qty_unit")
    attrs["contract_size"] = _const(df, "qty_contract_size")

    warnings: List[str] = []
    if "id" in df.columns:
        missing = int(df["id"].isna().sum())
        if missing:
            warnings.append(f"trades {ctx['exchange']}/{ctx['market_type']}/{ctx['symbol']}: {missing} of {len(df)} trades have id=null; sort by timestamp, not id")
    quote = _quote_warning(attrs["quote"], ctx, "trades")
    if quote:
        warnings.append(quote)

    return _finalize(df, attrs, COLUMNS["trades"], warnings)


def _agg_trades(rows: List[Dict], ctx: Dict[str, Any]) -> Tuple[pd.DataFrame, List[str]]:
    df = _indexed(rows)
    df = df.rename(columns={"qty_native": "qty"})

    attrs = _base_attrs(ctx)
    attrs["quote"]         = _const(df, "quote")
    attrs["qty_unit"]      = _const(df, "qty_unit")
    attrs["contract_size"] = _const(df, "qty_contract_size")

    warnings: List[str] = []
    short = _short_result_warning(df, ctx, "agg_trades")
    if short:
        warnings.append(short)
    quote = _quote_warning(attrs["quote"], ctx, "agg_trades")
    if quote:
        warnings.append(quote)

    return _finalize(df, attrs, COLUMNS["agg_trades"], warnings)


def _funding_rate(rows: List[Dict], ctx: Dict[str, Any]) -> Tuple[pd.DataFrame, List[str]]:
    df = _indexed(rows)
    df = df.rename(columns={"rate_per_cycle": "rate", "rate_cycle_ms": "cycle_ms"})

    cycle_hours = df["cycle_ms"] / 3_600_000
    df["funding_per_hour"] = df["rate"].where(cycle_hours == 0, df["rate"] / cycle_hours)

    attrs = _base_attrs(ctx)
    attrs["quote"]        = _const(df, "quote")
    attrs["funding_kind"] = _const(df, "rate_kind")

    warnings: List[str] = []
    if df["cycle_ms"].nunique(dropna=True) > 1:
        warnings.append(f"funding_rate {ctx['exchange']}/{ctx['market_type']}/{ctx['symbol']}: funding cycle changed within the window; funding_per_hour is per-row correct")
    short = _short_result_warning(df, ctx, "funding_rate")
    if short:
        warnings.append(short)

    return _finalize(df, attrs, COLUMNS["funding_rate"], warnings)


def _open_interest(rows: List[Dict], ctx: Dict[str, Any]) -> Tuple[pd.DataFrame, List[str]]:
    df = _indexed(rows)
    df = df.rename(columns={"open_interest_native": "open_interest"})

    attrs = _base_attrs(ctx)
    attrs["quote"]         = _const(df, "quote")
    attrs["interval"]      = _const(df, "interval")
    attrs["oi_unit"]       = _const(df, "open_interest_unit")
    attrs["contract_size"] = _const(df, "open_interest_contract_size")
    attrs["usd_basis"]     = _const(df, "open_interest_usd_basis_method")

    warnings: List[str] = []
    if "open_interest_usd" in df.columns:
        missing = int(df["open_interest_usd"].isna().sum())
        if missing:
            warnings.append(f"open_interest {ctx['exchange']}/{ctx['market_type']}/{ctx['symbol']}: {missing} of {len(df)} rows have usd=NaN (join missed; native is populated)")
    short = _short_result_warning(df, ctx, "open_interest")
    if short:
        warnings.append(short)
    quote = _quote_warning(attrs["quote"], ctx, "open_interest")
    if quote:
        warnings.append(quote)

    return _finalize(df, attrs, COLUMNS["open_interest"], warnings)


def _liquidations(rows: List[Dict], ctx: Dict[str, Any]) -> Tuple[pd.DataFrame, List[str]]:
    df = _indexed(rows)
    df = df.rename(columns={"qty_native": "qty"})

    attrs = _base_attrs(ctx)
    attrs["quote"]         = _const(df, "quote")
    attrs["qty_unit"]      = _const(df, "qty_unit")
    attrs["contract_size"] = _const(df, "qty_contract_size")

    warnings: List[str] = []
    quote = _quote_warning(attrs["quote"], ctx, "liquidations")
    if quote:
        warnings.append(quote)

    return _finalize(df, attrs, COLUMNS["liquidations"], warnings)


def _long_short_ratio(rows: List[Dict], ctx: Dict[str, Any]) -> Tuple[pd.DataFrame, List[str]]:
    df = _indexed(rows)

    attrs = _base_attrs(ctx)
    attrs["interval"]      = _const(df, "interval")
    attrs["account_scope"] = _const(df, "account_scope")

    warnings: List[str] = []
    if attrs["account_scope"] == "opaque":
        warnings.append(f"long_short_ratio {ctx['exchange']}/{ctx['market_type']}/{ctx['symbol']}: account breakdown unavailable (account_scope='opaque'); long_account/short_account are NaN")
    short = _short_result_warning(df, ctx, "long_short_ratio")
    if short:
        warnings.append(short)

    return _finalize(df, attrs, COLUMNS["long_short_ratio"], warnings)


_BUILDERS = {
    "candles":          _candles,
    "trades":           _trades,
    "agg_trades":       _agg_trades,
    "funding_rate":     _funding_rate,
    "open_interest":    _open_interest,
    "liquidations":     _liquidations,
    "long_short_ratio": _long_short_ratio,
}


def build(route: str, rows: List[Dict], ctx: Dict[str, Any]) -> Tuple[pd.DataFrame, List[str]]:
    if not rows:
        return _empty(ctx, route)

    return _BUILDERS[route](rows, ctx)


def orderbook(data: Dict[str, Any], ctx: Dict[str, Any]) -> pd.DataFrame:
    bids = data.get("bids") or []
    asks = data.get("asks") or []

    records = [{"side": "bid", "price": p, "qty": q} for p, q in bids]
    records += [{"side": "ask", "price": p, "qty": q} for p, q in asks]

    df = pd.DataFrame(records, columns=["side", "price", "qty"])
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["qty"]   = pd.to_numeric(df["qty"], errors="coerce")

    df.attrs.update({
        "exchange":       ctx["exchange"],
        "market_type":    ctx["market_type"],
        "symbol":         ctx["symbol"],
        "quote":          data.get("quote"),
        "qty_unit":       data.get("qty_unit"),
        "timestamp":      data.get("timestamp"),
        "schema_version": SCHEMA_VERSION,
        "warnings":       [],
    })

    return df


def with_provenance(df: pd.DataFrame) -> pd.DataFrame:
    attrs = dict(df.attrs)
    out   = df.copy()

    out["exchange"] = attrs.get("exchange")
    out["symbol"]   = attrs.get("symbol")
    out["quote"]    = attrs.get("quote")
    out["unit"]     = attrs.get("volume_unit") or attrs.get("qty_unit") or attrs.get("oi_unit")

    out.attrs.update(attrs)

    return out
