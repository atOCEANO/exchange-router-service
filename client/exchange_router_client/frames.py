from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


SCHEMA_VERSION = 3

USD_FAMILY = {"USD", "USDT", "USDC", "USDD", "TUSD", "FDUSD", "BUSD", "DAI", "PYUSD", "USDE", "USDP"}

TEXT_COLUMNS = {"side", "id", "agg_id", "first_trade_id", "last_trade_id"}


def _base_attrs(ctx: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "exchange":       ctx["exchange"],
        "market_type":    ctx["market_type"],
        "symbol":         ctx["symbol"],
        "schema_version": SCHEMA_VERSION,
        "warnings":       [],
    }


def _empty(ctx: Dict[str, Any], route: str) -> Tuple[pd.DataFrame, List[str]]:
    df = pd.DataFrame()
    attrs = _base_attrs(ctx)
    message = f"0 rows returned for {ctx['exchange']}/{ctx['market_type']}/{ctx['symbol']} {route}"
    attrs["warnings"] = [message]
    df.attrs.update(attrs)

    return df, [message]


def _indexed(rows: List[Dict]) -> pd.DataFrame:
    df = pd.json_normalize(rows, sep="_")
    df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms")
    df = df.set_index("datetime").sort_index()
    df = df.drop(columns=["timestamp"])

    return df


def _demote(df: pd.DataFrame, attrs: Dict[str, Any], mapping: Dict[str, str]) -> pd.DataFrame:
    for column, attr_name in mapping.items():
        if column not in df.columns:
            continue

        series = df[column]
        if series.isna().all():
            df = df.drop(columns=[column])
            continue

        if series.nunique(dropna=True) == 1:
            attrs[attr_name] = series.dropna().iloc[0]
            df = df.drop(columns=[column])

    return df


def _coerce(df: pd.DataFrame) -> pd.DataFrame:
    for column in df.columns:
        if column in TEXT_COLUMNS:
            continue
        df[column] = pd.to_numeric(df[column], errors="coerce")

    return df


def _quote_warning(attrs: Dict[str, Any], ctx: Dict[str, Any], route: str) -> Optional[str]:
    quote = attrs.get("quote")
    if quote and quote not in USD_FAMILY:
        return f"{route} {ctx['exchange']}/{ctx['market_type']}/{ctx['symbol']}: quote='{quote}'; *_usd fields are quote-denominated ({quote}), not US dollars"

    return None


def _short_result_warning(df: pd.DataFrame, ctx: Dict[str, Any], route: str) -> Optional[str]:
    requested = ctx.get("requested")
    if not ctx.get("paginated") or requested is None:
        return None

    if 0 < len(df) < requested:
        return f"{route} {ctx['exchange']}/{ctx['market_type']}/{ctx['symbol']}: got {len(df)} of {requested} requested; the server paginates to ~100k per call, fetch the rest with start="

    return None


def _finalize(df: pd.DataFrame, attrs: Dict[str, Any], keep: List[str], warnings: List[str]) -> Tuple[pd.DataFrame, List[str]]:
    df = df[[c for c in keep if c in df.columns]].copy()
    df = _coerce(df)
    attrs["warnings"] = warnings
    df.attrs.update(attrs)

    return df, warnings


def _candles(rows: List[Dict], ctx: Dict[str, Any]) -> Tuple[pd.DataFrame, List[str]]:
    df = _indexed(rows)
    df = df.rename(columns={"volume_native": "volume"})

    attrs = _base_attrs(ctx)
    df = _demote(df, attrs, {
        "quote":                   "quote",
        "interval":                "interval",
        "volume_unit":             "volume_unit",
        "volume_contract_size":    "contract_size",
        "volume_usd_basis_method": "usd_basis",
    })

    warnings = []
    short = _short_result_warning(df, ctx, "candles")
    if short:
        warnings.append(short)
    quote = _quote_warning(attrs, ctx, "candles")
    if quote:
        warnings.append(quote)

    return _finalize(df, attrs, ["open", "high", "low", "close", "volume", "volume_usd"], warnings)


def _trades(rows: List[Dict], ctx: Dict[str, Any]) -> Tuple[pd.DataFrame, List[str]]:
    df = _indexed(rows)
    df = df.rename(columns={"qty_native": "qty"})

    attrs = _base_attrs(ctx)
    df = _demote(df, attrs, {
        "quote":              "quote",
        "qty_unit":           "qty_unit",
        "qty_contract_size":  "contract_size",
    })

    warnings = []
    if "id" in df.columns:
        missing = int(df["id"].isna().sum())
        if missing:
            warnings.append(f"trades {ctx['exchange']}/{ctx['market_type']}/{ctx['symbol']}: {missing} of {len(df)} trades have id=null; sort by timestamp, not id")
    quote = _quote_warning(attrs, ctx, "trades")
    if quote:
        warnings.append(quote)

    return _finalize(df, attrs, ["price", "qty", "qty_usd", "side", "id"], warnings)


def _agg_trades(rows: List[Dict], ctx: Dict[str, Any]) -> Tuple[pd.DataFrame, List[str]]:
    df = _indexed(rows)
    df = df.rename(columns={"qty_native": "qty"})

    attrs = _base_attrs(ctx)
    df = _demote(df, attrs, {
        "quote":              "quote",
        "qty_unit":           "qty_unit",
        "qty_contract_size":  "contract_size",
    })

    warnings = []
    short = _short_result_warning(df, ctx, "agg_trades")
    if short:
        warnings.append(short)
    quote = _quote_warning(attrs, ctx, "agg_trades")
    if quote:
        warnings.append(quote)

    return _finalize(df, attrs, ["price", "qty", "qty_usd", "side", "agg_id", "first_trade_id", "last_trade_id"], warnings)


def _funding_rate(rows: List[Dict], ctx: Dict[str, Any]) -> Tuple[pd.DataFrame, List[str]]:
    df = _indexed(rows)
    df = df.rename(columns={"rate_per_cycle": "rate", "rate_cycle_ms": "cycle_ms"})

    cycle_hours = df["cycle_ms"] / 3_600_000
    df["funding_per_hour"] = df["rate"].where(cycle_hours == 0, df["rate"] / cycle_hours)

    attrs = _base_attrs(ctx)
    df = _demote(df, attrs, {
        "quote":     "quote",
        "rate_kind": "funding_kind",
        "cycle_ms":  "cycle_ms",
    })

    warnings = []
    if "cycle_ms" in df.columns:
        warnings.append(f"funding_rate {ctx['exchange']}/{ctx['market_type']}/{ctx['symbol']}: funding cycle changed within the window; per-row funding_per_hour is correct, cycle_ms is a column not an attr")
    short = _short_result_warning(df, ctx, "funding_rate")
    if short:
        warnings.append(short)

    return _finalize(df, attrs, ["rate", "funding_per_hour", "cycle_ms"], warnings)


def _open_interest(rows: List[Dict], ctx: Dict[str, Any]) -> Tuple[pd.DataFrame, List[str]]:
    df = _indexed(rows)
    df = df.rename(columns={"open_interest_native": "open_interest"})

    attrs = _base_attrs(ctx)
    df = _demote(df, attrs, {
        "quote":                          "quote",
        "interval":                       "interval",
        "open_interest_unit":             "oi_unit",
        "open_interest_contract_size":    "contract_size",
        "open_interest_usd_basis_method": "usd_basis",
    })

    warnings = []
    if "open_interest_usd" in df.columns:
        missing = int(df["open_interest_usd"].isna().sum())
        if missing:
            warnings.append(f"open_interest {ctx['exchange']}/{ctx['market_type']}/{ctx['symbol']}: {missing} of {len(df)} rows have usd=NaN (join missed; native is populated)")
    short = _short_result_warning(df, ctx, "open_interest")
    if short:
        warnings.append(short)
    quote = _quote_warning(attrs, ctx, "open_interest")
    if quote:
        warnings.append(quote)

    return _finalize(df, attrs, ["open_interest", "open_interest_usd"], warnings)


def _liquidations(rows: List[Dict], ctx: Dict[str, Any]) -> Tuple[pd.DataFrame, List[str]]:
    df = _indexed(rows)
    df = df.rename(columns={"qty_native": "qty"})

    attrs = _base_attrs(ctx)
    df = _demote(df, attrs, {
        "quote":             "quote",
        "qty_unit":          "qty_unit",
        "qty_contract_size": "contract_size",
    })

    warnings = []
    quote = _quote_warning(attrs, ctx, "liquidations")
    if quote:
        warnings.append(quote)

    return _finalize(df, attrs, ["price", "qty", "qty_usd", "side"], warnings)


def _long_short_ratio(rows: List[Dict], ctx: Dict[str, Any]) -> Tuple[pd.DataFrame, List[str]]:
    df = _indexed(rows)

    attrs = _base_attrs(ctx)
    df = _demote(df, attrs, {
        "interval":      "interval",
        "account_scope": "account_scope",
    })

    warnings = []
    if attrs.get("account_scope") == "opaque":
        for column in ("long_account", "short_account"):
            if column in df.columns:
                df = df.drop(columns=[column])
        warnings.append(f"long_short_ratio {ctx['exchange']}/{ctx['market_type']}/{ctx['symbol']}: account columns unavailable (account_scope='opaque'); only ratio is returned")
    short = _short_result_warning(df, ctx, "long_short_ratio")
    if short:
        warnings.append(short)

    return _finalize(df, attrs, ["ratio", "long_account", "short_account"], warnings)


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
