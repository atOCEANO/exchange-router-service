from typing import Any, Dict, Optional
from urllib.parse import urlencode

from tools.auditor.aggregator import ProbeResult
from tools.auditor.output.ordering import PERIOD_ORDER, ROUTES_NO_SYMBOL


def extract_params(r: ProbeResult, variant_id: Optional[str]) -> Dict[str, Any]:
    if r.kind == "ws":
        return {"channel": r.route, "symbol": r.symbol}

    ev_params = (r.evidence or {}).get("params")
    params    = dict(ev_params) if isinstance(ev_params, dict) else {}

    if variant_id and ":" in variant_id:
        head = variant_id.split(":", 1)[0]
        if head in PERIOD_ORDER and "period" not in params and "interval" not in params:
            params["period"] = head

    if variant_id and not params:
        if "=" in variant_id:
            key, _, val = variant_id.partition("=")
            try:
                params[key] = int(val)
            except ValueError:
                params[key] = val

    return params


def synth_url(r: ProbeResult, params: Dict[str, Any], api_url: str) -> str:
    if r.kind == "ws":
        base = api_url.replace("https://", "wss://").replace("http://", "ws://")
        return f"{base}/ws/{r.exchange}/{r.market_type}"

    if r.route in ROUTES_NO_SYMBOL or not r.symbol:
        path = f"/{r.exchange}/{r.market_type}/{r.route}"
    else:
        path = f"/{r.exchange}/{r.market_type}/{r.route}/{r.symbol}"

    url = f"{api_url}{path}"
    if params:
        querystring = urlencode({k: v for k, v in params.items() if v is not None}, doseq=False)
        if querystring:
            url += "?" + querystring
    return url


def params_text(params: Dict[str, Any]) -> str:
    if not params:
        return ""
    return ", ".join(f"{k}={v}" for k, v in params.items())


_EVIDENCE_PRIORITY = ("count", "ceiling", "age_s", "depth", "top_bid_qty", "top_ask_qty", "drift_pct", "drift_bps", "last_ts", "limit", "status", "endpoint")


def compact_evidence(evidence: Dict[str, Any], max_chars: int = 60) -> str:
    if not evidence:
        return ""

    pairs = []
    for key in _EVIDENCE_PRIORITY:
        if key in evidence:
            pairs.append(f"{key}={_format_val(evidence[key])}")
    for key in evidence:
        if key in _EVIDENCE_PRIORITY or key == "params":
            continue
        pairs.append(f"{key}={_format_val(evidence[key])}")

    out = " ".join(pairs)
    if len(out) > max_chars:
        out = out[:max_chars - 1] + "…"
    return out


def _format_val(v: Any) -> str:
    if isinstance(v, float):
        return f"{v:.1f}" if abs(v) >= 0.1 or v == 0 else f"{v:.2g}"
    if isinstance(v, (list, dict)):
        return f"<{type(v).__name__}>"
    return str(v)
