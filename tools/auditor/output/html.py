import json
from collections import defaultdict
from datetime import datetime, timezone
from html import escape as _h
from pathlib import Path
from typing import Any, Dict, List, Optional

from tools.auditor.aggregator import Aggregator, ProbeResult
from tools.auditor.config import API_URL, HTML_PRE_MAX_CHARS
from tools.auditor.output._urlfmt import extract_params, params_text, synth_url
from tools.auditor.output.ordering import (
    CAPS_KEY_ORDER, MARKET_ORDER, META_ROUTES, PERIOD_ORDER,
    ROUTE_DISPLAY_ORDER, SUB_ORDER,
)


_ASSETS_DIR = Path(__file__).parent / "assets"
_HTML_CSS   = (_ASSETS_DIR / "style.css").read_text(encoding="utf-8")
_HTML_JS    = (_ASSETS_DIR / "script.js").read_text(encoding="utf-8")


_DASH = "-"


def _safe_id(s: str) -> str:
    out = "".join(c if c.isalnum() else "-" for c in s).strip("-").lower()
    return out or "x"


def _badge(status: str) -> str:
    return f'<span class="badge {_h(status)}">{_h(status)}</span>'


def _market_sort_key(mt: str) -> int:
    return MARKET_ORDER.get(mt, 99)


def _route_sort_key(route: str) -> tuple:
    if route in ROUTE_DISPLAY_ORDER:
        return (0, ROUTE_DISPLAY_ORDER.index(route))
    return (1, 0)


def _is_meta_probe(r: ProbeResult) -> bool:
    return r.route in META_ROUTES or r.market_type == "ALL"


def _variant_id(probe_name: str, route: str) -> Optional[str]:
    if probe_name == route:
        return None
    prefix = route + ":"
    if probe_name.startswith(prefix):
        return probe_name[len(prefix):]
    if probe_name.startswith("ws:"):
        return probe_name[3:]
    return probe_name


def _variant_sort_key(variant_id: Optional[str]) -> tuple:
    if variant_id is None:
        return (0, "", 0, "")
    parts = variant_id.split(":")
    if len(parts) >= 2 and parts[0] in PERIOD_ORDER:
        period_idx = PERIOD_ORDER.index(parts[0])
        sub        = ":".join(parts[1:])
        sub_idx    = SUB_ORDER.index(sub) if sub in SUB_ORDER else 99
        return (1, period_idx, sub_idx, variant_id)
    if "=" in variant_id:
        key, _, val = variant_id.partition("=")
        try:
            return (2, key, int(val), variant_id)
        except ValueError:
            return (2, key, 0, variant_id)
    if variant_id in SUB_ORDER:
        return (3, SUB_ORDER.index(variant_id), 0, variant_id)
    return (4, variant_id, 0, variant_id)


def _truncate_for_display(value: Any, max_items: int = 10) -> Any:
    if isinstance(value, list):
        if len(value) <= max_items:
            return [_truncate_for_display(v, max_items) for v in value]
        head = [_truncate_for_display(v, max_items) for v in value[:max_items]]
        return head + [f"... ({len(value) - max_items} more items truncated for display)"]
    if isinstance(value, dict):
        return {k: _truncate_for_display(v, max_items) for k, v in value.items()}
    return value


def _pretty_json(value: Any) -> str:
    try:
        text = json.dumps(value, indent=2, default=str, ensure_ascii=False)
    except Exception:
        text = str(value)
    if len(text) > HTML_PRE_MAX_CHARS:
        text = text[:HTML_PRE_MAX_CHARS] + "\n... [truncated]"
    return text


def _route_caps_key(route: str, variant_id: Optional[str]) -> str:
    if route == "ws" and variant_id:
        return variant_id
    return route


def _route_caps(caps_by_exchange: Dict[str, Dict[str, Any]], market_type: str, caps_key: str, exchanges: List[str]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for ex in exchanges:
        block = ((caps_by_exchange.get(ex) or {}).get("markets") or {}).get(market_type, {}).get(caps_key)
        if isinstance(block, dict):
            out[ex] = block
    return out


def _cap_cell(v: Any) -> str:
    if v is True:
        return '<span class="cap-yes">✓</span>'
    if v is False:
        return '<span class="cap-no">✗</span>'
    if v is None:
        return '<span class="cap-none">-</span>'
    if isinstance(v, list):
        if not v:
            return '<span class="cap-none">[]</span>'
        if len(v) > 6:
            return _h(", ".join(str(x) for x in v[:6])) + f' <span class="cap-none">… (+{len(v) - 6})</span>'
        return _h(", ".join(str(x) for x in v))
    return _h(str(v))


def _render_caps_matrix(caps_by_exchange: Dict[str, Dict[str, Any]], market_type: str, caps_key: str, exchanges: List[str]) -> str:
    per_exchange = _route_caps(caps_by_exchange, market_type, caps_key, exchanges)
    if not per_exchange:
        return ""

    rows: Dict[str, Dict[str, Any]] = {}
    for ex, block in per_exchange.items():
        for k, v in block.items():
            rows.setdefault(k, {})[ex] = v
    if not rows:
        return ""

    ordered_keys = [k for k in CAPS_KEY_ORDER if k in rows]
    ordered_keys += sorted(k for k in rows if k not in CAPS_KEY_ORDER)

    parts: List[str] = ['<table class="caps-matrix"><thead><tr><th>capability</th>']
    for ex in exchanges:
        parts.append(f'<th>{_h(ex)}</th>')
    parts.append('</tr></thead><tbody>')
    for key in ordered_keys:
        parts.append(f'<tr><td class="cap-key">{_h(key)}</td>')
        for ex in exchanges:
            parts.append(f'<td>{_cap_cell(rows[key].get(ex))}</td>')
        parts.append('</tr>')
    parts.append('</tbody></table>')
    return "".join(parts)


def _compact_cap_val(v: Any) -> str:
    if v is True:                return "yes"
    if v is False:               return "no"
    if v is None:                return "-"
    if isinstance(v, list):      return f"[{len(v)} items]"
    return str(v)


def _panel_caps_line(caps_block: Optional[Dict[str, Any]]) -> str:
    if not isinstance(caps_block, dict) or not caps_block:
        return ""
    pairs = []
    seen  = set()
    for k in CAPS_KEY_ORDER:
        if k in caps_block:
            pairs.append(f"{k}={_compact_cap_val(caps_block[k])}")
            seen.add(k)
    for k, v in caps_block.items():
        if k not in seen:
            pairs.append(f"{k}={_compact_cap_val(v)}")
    summary = ", ".join(pairs)
    return f'<div class="panel-row"><span class="panel-label">capabilities</span><code class="panel-code">{_h(summary)}</code></div>'


def _render_panel(r: ProbeResult, params: Dict[str, Any], api_url: str, caps_block: Optional[Dict[str, Any]] = None) -> str:
    url    = synth_url(r, params, api_url)
    method = "WS" if r.kind == "ws" else "GET"

    parts: List[str] = []

    if r.kind == "ws":
        sub = {"channel": r.route, "symbol": r.symbol}
        parts.append(f'<div class="panel-row"><span class="panel-label">subscription</span><code class="panel-code">{_h(json.dumps(sub, ensure_ascii=False))}</code></div>')
    elif params:
        parts.append(f'<div class="panel-row"><span class="panel-label">params</span><code class="panel-code">{_h(params_text(params))}</code></div>')

    parts.append(f'<div class="panel-row"><span class="panel-label">url</span><code class="panel-url" title="click to select">{_h(method)} {_h(url)}</code></div>')
    parts.append(_panel_caps_line(caps_block))

    meta_bits = [_badge(r.status), f'<span class="error-type">{_h(r.error_type.value)}</span>', f'<span class="latency">{r.latency_ms:.0f}ms</span>']
    if r.symbol:
        meta_bits.append(f'<span class="symbol">{_h(r.symbol)}</span>')
    parts.append(f'<div class="panel-row panel-meta">{"".join(meta_bits)}</div>')

    if r.message:
        parts.append(f'<div class="panel-row panel-msg">{_h(r.message)}</div>')

    has_analytics = bool(r.evidence)
    has_sample    = r.sample is not None

    if not (has_analytics or has_sample):
        parts.append('<div class="panel-row panel-empty">(no payload captured for this probe)</div>')
        return "".join(parts)

    default_view      = "analytics" if has_analytics else "sample"
    analytics_label   = "analytics" if has_sample else "analytics only"
    sample_label      = "sample"    if has_analytics else "sample only"

    if has_analytics and has_sample:
        parts.append(
            '<div class="panel-row panel-view-toggle">'
            f'<button class="view-btn{" active" if default_view == "analytics" else ""}" data-view="analytics" onclick="selectView(this)">analytics</button>'
            f'<button class="view-btn{" active" if default_view == "sample" else ""}" data-view="sample" onclick="selectView(this)">sample</button>'
            '</div>'
        )

    if has_analytics:
        body   = _pretty_json(_truncate_for_display(r.evidence))
        active = " active" if default_view == "analytics" else ""
        parts.append(f'<div class="panel-row panel-response panel-view{active}" data-view="analytics"><div class="panel-label">{_h(analytics_label)}</div><pre>{_h(body)}</pre></div>')

    if has_sample:
        if r.sample == [] or r.sample == {}:
            shape   = "list" if isinstance(r.sample, list) else "object"
            content = f'<div class="panel-empty">(empty {shape})</div>'
        else:
            body    = _pretty_json(_truncate_for_display(r.sample))
            content = f'<pre>{_h(body)}</pre>'
        active = " active" if default_view == "sample" else ""
        parts.append(f'<div class="panel-row panel-response panel-view{active}" data-view="sample"><div class="panel-label">{_h(sample_label)}</div>{content}</div>')

    return "".join(parts)


def _render_route_section(
    market_type      : str,
    route            : str,
    kind             : str,
    results          : List[ProbeResult],
    exchanges        : List[str],
    api_url          : str,
    caps_by_exchange : Dict[str, Dict[str, Any]],
) -> str:
    grouped: Dict[Optional[str], Dict[str, ProbeResult]] = defaultdict(dict)
    for r in results:
        vid = _variant_id(r.probe, route) if kind == "rest" else _variant_id(r.probe, "ws")
        grouped[vid][r.exchange] = r

    variants = sorted(grouped.keys(), key=_variant_sort_key)
    if not variants:
        return ""

    has_real_variants = not (len(variants) == 1 and variants[0] is None)
    section_id        = _safe_id(f"r-{market_type}-{kind}-{route}")
    has_issue         = any(r.status in ("fail", "warn") for vmap in grouped.values() for r in vmap.values())

    default_variant  = variants[0] if variants[0] is not None else "default"
    default_exchange = None
    for ex in exchanges:
        if ex in grouped[variants[0]]:
            default_exchange = ex
            break
    if default_exchange is None and exchanges:
        default_exchange = exchanges[0]

    parts: List[str] = []
    parts.append(
        f'<section class="route" id="{section_id}" data-route="{_h(route)}" data-kind="{_h(kind)}" '
        f'data-active-variant="{_h(default_variant)}" data-active-exchange="{_h(default_exchange or "")}" '
        f'data-has-issue="{1 if has_issue else 0}">'
    )

    title = _h(route) if kind == "rest" else f"ws · {_h(route)}"
    parts.append(f'<h4><code>{title}</code></h4>')

    if has_real_variants:
        parts.append('<div class="variant-buttons">')
        for v in variants:
            vid    = v if v is not None else "default"
            active = " active" if vid == default_variant else ""
            label  = _h(v) if v is not None else "(default)"
            parts.append(f'<button class="variant-btn{active}" data-variant="{_h(vid)}" onclick="selectVariant(this)">{label}</button>')
        parts.append('</div>')

    for v in variants:
        vid      = v if v is not None else "default"
        vmap     = grouped[v]
        caps_key = _route_caps_key(route, v)
        parts.append(f'<div class="variant-block" data-variant="{_h(vid)}">')

        matrix = _render_caps_matrix(caps_by_exchange, market_type, caps_key, exchanges)
        if matrix:
            parts.append(matrix)

        parts.append('<div class="exchange-buttons">')
        for ex in exchanges:
            r = vmap.get(ex)
            if r is None:
                parts.append(
                    f'<button class="exchange-btn" data-exchange="{_h(ex)}" data-status="missing" disabled>'
                    f'<span class="dot"></span>{_h(ex)}<span class="meta">-</span></button>'
                )
                continue
            active = " active" if ex == default_exchange else ""
            parts.append(
                f'<button class="exchange-btn{active}" data-exchange="{_h(ex)}" data-status="{_h(r.status)}" onclick="selectExchange(this)">'
                f'<span class="dot"></span>{_h(ex)}<span class="meta">{r.latency_ms:.0f}ms</span></button>'
            )
        parts.append('</div>')

        for ex in exchanges:
            r = vmap.get(ex)
            if r is None:
                parts.append(
                    f'<div class="panel" data-variant="{_h(vid)}" data-exchange="{_h(ex)}">'
                    f'<div class="panel-row panel-empty">{_h(ex)} did not run this probe.</div>'
                    '</div>'
                )
                continue
            params     = extract_params(r, v)
            caps_block = ((caps_by_exchange.get(ex) or {}).get("markets") or {}).get(market_type, {}).get(caps_key)
            parts.append(
                f'<div class="panel" data-variant="{_h(vid)}" data-exchange="{_h(ex)}">'
                f'{_render_panel(r, params, api_url, caps_block)}'
                '</div>'
            )
        parts.append('</div>')

    parts.append('</section>')
    return "".join(parts)


def _render_checks_section(meta_results: List[ProbeResult]) -> str:
    if not meta_results:
        return ""
    parts = ['<section id="sec-checks"><details><summary class="checks-summary"><h2>Validation Checks</h2><span class="muted">(meta probes, click to expand)</span></summary>']
    parts.append('<table class="checks-table">')
    parts.append('<thead><tr><th>status</th><th>exchange</th><th>market</th><th>route</th><th>probe</th><th>latency</th><th>message</th></tr></thead><tbody>')
    for r in sorted(meta_results, key=lambda x: (x.status != "fail", x.status != "warn", x.exchange, x.market_type, x.route, x.probe)):
        parts.append(
            '<tr>'
            f'<td>{_badge(r.status)}</td>'
            f'<td>{_h(r.exchange)}</td>'
            f'<td>{_h(r.market_type)}</td>'
            f'<td>{_h(r.route)}</td>'
            f'<td><code>{_h(r.probe)}</code></td>'
            f'<td>{r.latency_ms:.0f}ms</td>'
            f'<td>{_h(r.message or "")}</td>'
            '</tr>'
        )
    parts.append('</tbody></table></details></section>')
    return "".join(parts)


def write_html(folder: Path, aggregator: Aggregator, wall_time_s: float, requested: List[str]) -> None:
    exchanges = sorted({r.exchange for r in aggregator.results if r.exchange != "GLOBAL"})

    meta_results    : List[ProbeResult]                  = []
    rest_by_mt_route: Dict[tuple, List[ProbeResult]]     = defaultdict(list)
    ws_by_mt        : Dict[str, List[ProbeResult]]       = defaultdict(list)
    caps_by_exchange: Dict[str, Dict[str, Any]]          = {}

    for r in aggregator.results:
        if r.route == "capabilities" and isinstance(r.sample, dict):
            caps_by_exchange[r.exchange] = r.sample
        if _is_meta_probe(r):
            meta_results.append(r)
            continue
        if r.kind == "ws":
            ws_by_mt[r.market_type].append(r)
        else:
            rest_by_mt_route[(r.market_type, r.route)].append(r)

    market_types = sorted({mt for (mt, _) in rest_by_mt_route.keys()} | set(ws_by_mt.keys()), key=_market_sort_key)

    parts: List[str] = []
    parts.append('<!doctype html><html lang="en"><head><meta charset="utf-8">')
    parts.append('<meta name="viewport" content="width=device-width, initial-scale=1">')
    parts.append("<title>Audit Run</title>")
    parts.append(f"<style>{_HTML_CSS}</style>")
    parts.append("</head><body>")

    totals = aggregator.totals()
    parts.append('<header class="topbar">')
    parts.append("<h1>Audit Run</h1>")
    parts.append('<div class="meta">')
    parts.append(f'<span>generated: {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")}</span>')
    parts.append(f'<span>api: <code>{_h(API_URL)}</code></span>')
    parts.append(f'<span>wall time: {wall_time_s:.1f}s</span>')
    parts.append(f'<span>requested: <code>{_h(str(requested or "all"))}</code></span>')
    parts.append(
        f'<span>{_badge("pass")}{totals.get("pass", 0)} '
        f'{_badge("warn")}{totals.get("warn", 0)} '
        f'{_badge("fail")}{totals.get("fail", 0)} '
        f'{_badge("skip")}{totals.get("skip", 0)}</span>'
    )
    parts.append("</div>")
    parts.append('<div class="toolbar">')
    parts.append('<input id="search" type="text" placeholder="filter by route name...">')
    parts.append('<label><input type="checkbox" id="only-issues"> only fails/warns</label>')
    parts.append("</div>")
    parts.append("</header>")

    parts.append('<div class="layout">')

    parts.append('<nav class="toc">')
    parts.append('<a href="#sec-summary">Summary</a>')
    if meta_results:
        parts.append('<a href="#sec-checks">Validation Checks</a>')
    for mt in market_types:
        parts.append(f'<details open><summary>{_h(mt)}</summary><div class="sub">')
        rest_routes = sorted({rt for (m, rt) in rest_by_mt_route.keys() if m == mt}, key=_route_sort_key)
        for rt in rest_routes:
            parts.append(f'<a href="#{_safe_id(f"r-{mt}-rest-{rt}")}">{_h(rt)}</a>')
        if mt in ws_by_mt:
            parts.append(f'<a href="#{_safe_id(f"r-{mt}-ws-ws")}">ws</a>')
        parts.append("</div></details>")
    parts.append("</nav>")

    parts.append("<main>")

    by_ex = aggregator.by_exchange()
    parts.append('<section id="sec-summary"><h2>Summary</h2>')
    parts.append('<table class="summary-table"><tr><th>status</th>')
    for ex in exchanges:
        parts.append(f"<th>{_h(ex)}</th>")
    parts.append("</tr>")
    for status in ("pass", "warn", "fail", "skip"):
        parts.append(f"<tr><td>{_badge(status)}</td>")
        for ex in exchanges:
            parts.append(f'<td>{by_ex.get(ex, {}).get(status, 0)}</td>')
        parts.append("</tr>")
    parts.append("</table></section>")

    parts.append(_render_checks_section(meta_results))

    for mt in market_types:
        mt_id = _safe_id(mt)
        parts.append(f'<section id="sec-{mt_id}"><h2>{_h(mt)}</h2>')

        rest_routes = sorted({rt for (m, rt) in rest_by_mt_route.keys() if m == mt}, key=_route_sort_key)
        if rest_routes:
            parts.append('<h3>REST</h3>')
            for rt in rest_routes:
                parts.append(_render_route_section(mt, rt, "rest", rest_by_mt_route[(mt, rt)], exchanges, API_URL, caps_by_exchange))

        if mt in ws_by_mt:
            parts.append('<h3>WebSocket</h3>')
            parts.append(_render_route_section(mt, "ws", "ws", ws_by_mt[mt], exchanges, API_URL, caps_by_exchange))

        parts.append("</section>")

    parts.append("</main></div>")
    parts.append(f"<script>{_HTML_JS}</script>")
    parts.append("</body></html>")

    (folder / "report.html").write_text("".join(parts), encoding="utf-8")
