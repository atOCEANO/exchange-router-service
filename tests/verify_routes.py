import os
import re
import asyncio
import colorsys
import httpx
import logging
import websockets
import json
from typing import List, Dict, Optional, Any, Iterable
from dataclasses import dataclass
from collections import defaultdict

from rich.console import Console
from rich.table import Table
from rich.text import Text
from rich.box import SIMPLE_HEAD

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

API_URL = os.getenv("API_URL", "http://localhost:8040")
TIMEOUT = float(os.getenv("TIMEOUT", "60.0"))

console = Console()

@dataclass
class TestResult:
    exchange: str
    market: str
    symbol: str
    test_type: str
    success: bool
    details: str = ""

_MONTHS = "JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC"
_LEVERAGED = re.compile(r"(?:^|[^A-Z])(UP|DOWN|BULL|BEAR|\d+[LS])(?:[^A-Z]|$)")
_DATED_NUM = re.compile(r"[-_]?\d{6,8}(?:[-_]|$)")
_DATED_MON = re.compile(rf"\d{{1,2}}(?:{_MONTHS})\d{{0,4}}")

def _score_symbol(symbol: str) -> float:
    s = symbol.upper()

    if _LEVERAGED.search(s) or _DATED_NUM.search(s) or _DATED_MON.search(s):
        return -1.0

    if "BTC" in s or "XBT" in s:
        base_score = 100.0
    elif "ETH" in s:
        base_score = 50.0
    else:
        return -1.0

    quote_score = 0.0
    stripped = re.sub(r"[-_/]?(PERP|PERPETUAL|SWAP)$", "", s)
    for quote, bonus in (("USDT", 30), ("USDC", 25), ("USD", 20)):
        if stripped.endswith(quote) or f"/{quote}" in stripped or f"-{quote}" in stripped or f"_{quote}" in stripped:
            quote_score = bonus
            break
    if quote_score == 0.0:
        return -1.0

    return base_score + quote_score - len(s) * 0.1

def pick_symbol(symbols: Iterable[str]) -> Optional[str]:
    best: Optional[str] = None
    best_score = 0.0
    for sym in symbols:
        if not isinstance(sym, str):
            continue
        score = _score_symbol(sym)
        if score > best_score:
            best_score = score
            best = sym
    return best

def _hsl_to_hex(h: float, s: float, l: float) -> str:
    r, g, b = colorsys.hls_to_rgb(h / 360.0, l, s)
    return f"#{int(r * 255):02x}{int(g * 255):02x}{int(b * 255):02x}"

def exchange_colors(exchanges: List[str]) -> Dict[str, str]:
    ordered = sorted(exchanges)
    n = max(len(ordered), 1)
    return {ex: _hsl_to_hex(((i / n) * 360 + 25) % 360, 0.55, 0.65) for i, ex in enumerate(ordered)}

async def fetch(client: httpx.AsyncClient, endpoint: str, params: Optional[Dict] = None) -> Any:
    full_url = f"{API_URL}{endpoint}"
    try:
        response = await client.get(full_url, params=params)
        if response.status_code == 501:
            return "NOT_IMPLEMENTED"
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP {e.response.status_code} while fetching {full_url}: {e.response.text}")
        return None
    except Exception as e:
        logger.error(f"Unexpected request failure for {full_url}: {str(e)}")
        return None

async def verify_pagination_interval(client: httpx.AsyncClient, exchange: str, market_type: str, symbol: str, interval: str) -> TestResult:
    endpoint = f"/{exchange}/{market_type}/candles/{symbol}"
    params = {"interval": interval, "limit": 1500}
    test_name = f"Candles {interval}"
    data = await fetch(client, endpoint, params)

    if data == "NOT_IMPLEMENTED":
        return TestResult(exchange, market_type, symbol, test_name, False, "501 Not Implemented (Unexpected)")
    if not data or not isinstance(data, list):
        return TestResult(exchange, market_type, symbol, test_name, False, "Invalid/Empty response format")
    if len(data) == 0:
        return TestResult(exchange, market_type, symbol, test_name, False, "Received 0 candles")

    timestamps = [c.get("timestamp") for c in data if isinstance(c, dict) and c.get("timestamp")]
    for i in range(len(timestamps) - 1):
        if timestamps[i] >= timestamps[i+1]:
            return TestResult(exchange, market_type, symbol, test_name, False,
                              f"Time violation at idx {i}: {timestamps[i]} >= {timestamps[i+1]}")

    return TestResult(exchange, market_type, symbol, test_name, True, f"Verified {len(data)} candles")

async def verify_websocket(exchange: str, market_type: str, symbol: str, channel: str) -> TestResult:
    if API_URL.startswith("https://"):
        base_ws_url = API_URL.replace("https://", "wss://")
    else:
        base_ws_url = API_URL.replace("http://", "ws://")

    ws_url = f"{base_ws_url}/ws/{exchange}/{market_type}"
    start_ts = asyncio.get_event_loop().time()

    try:
        async with websockets.connect(ws_url) as ws:
            await ws.send(json.dumps({"channel": channel, "symbol": symbol}))
            await asyncio.wait_for(ws.recv(), timeout=60.0)
            latency = (asyncio.get_event_loop().time() - start_ts) * 1000
            return TestResult(exchange, market_type, symbol, f"WS:{channel}", True, f"Connected, latency: {latency:.1f}ms")
    except asyncio.TimeoutError:
        return TestResult(exchange, market_type, symbol, f"WS:{channel}", False, "Connection/Response Timeout")
    except Exception as e:
        return TestResult(exchange, market_type, symbol, f"WS:{channel}", False, f"Error: {str(e)}")

async def verify_exchange(client: httpx.AsyncClient, exchange: str) -> List[TestResult]:
    results: List[TestResult] = []
    status_resp = await fetch(client, f"/{exchange}/status")
    results.append(TestResult(exchange, "ALL", "N/A", "Status", bool(status_resp), "OK" if status_resp else "Failed"))

    types_resp = await fetch(client, f"/{exchange}/market_types")
    results.append(TestResult(exchange, "ALL", "N/A", "Market Types", bool(types_resp), "OK" if types_resp else "Failed"))

    cap_resp = await fetch(client, f"/{exchange}/capabilities")
    if not cap_resp:
        results.append(TestResult(exchange, "ALL", "N/A", "Capabilities", False, "Failed to fetch map"))
        return results
    results.append(TestResult(exchange, "ALL", "N/A", "Capabilities", True, "OK"))

    markets_caps = cap_resp.get("markets", {})

    for market_type, caps in markets_caps.items():
        logger.info(f"Testing {exchange.upper()} [{market_type}]")

        info_resp = await fetch(client, f"/{exchange}/{market_type}/info")
        results.append(TestResult(exchange, market_type, "N/A", "Info", bool(info_resp), "OK" if info_resp else "Failed"))

        sym_resp = await fetch(client, f"/{exchange}/{market_type}/markets")
        if not sym_resp or not isinstance(sym_resp, list):
            results.append(TestResult(exchange, market_type, "N/A", "Markets", False, "Failed to fetch symbols"))
            logger.warning(f"No symbols returned for {exchange}/{market_type}")
            continue
        results.append(TestResult(exchange, market_type, "N/A", "Markets", True, f"Found {len(sym_resp)} symbols"))

        symbol = pick_symbol(sym_resp)
        if symbol is None:
            results.append(TestResult(exchange, market_type, "N/A", "Symbol Pick", False, "No BTC/ETH-stable pair found"))
            continue
        logger.info(f"  -> selected symbol: {symbol}")

        rest_routes = [
            ("Ticker",        "ticker",          f"/{exchange}/{market_type}/ticker/{symbol}"),
            ("Book Ticker",   "book_ticker",      f"/{exchange}/{market_type}/book_ticker/{symbol}"),
            ("Trades",        "trades",           f"/{exchange}/{market_type}/trades/{symbol}"),
            ("Agg Trades",    "agg_trades",       f"/{exchange}/{market_type}/agg_trades/{symbol}"),
            ("Orderbook",     "orderbook",        f"/{exchange}/{market_type}/orderbook/{symbol}"),
            ("Mark Price",    "mark_price",       f"/{exchange}/{market_type}/mark_price/{symbol}"),
            ("Funding Rate",  "funding_rate",     f"/{exchange}/{market_type}/funding_rate/{symbol}"),
            ("Open Interest", "open_interest",    f"/{exchange}/{market_type}/open_interest/{symbol}"),
            ("Liquidations",  "liquidations",     f"/{exchange}/{market_type}/liquidations/{symbol}"),
            ("L/S Ratio",     "long_short_ratio", f"/{exchange}/{market_type}/long_short_ratio/{symbol}"),
        ]

        for display_name, cap_key, route in rest_routes:
            if not caps.get(cap_key, {}).get("rest", False):
                continue
            res = await fetch(client, route)
            if res == "NOT_IMPLEMENTED":
                results.append(TestResult(exchange, market_type, symbol, display_name, False, "501 Not Implemented (Unexpected)"))
            elif res is not None:
                results.append(TestResult(exchange, market_type, symbol, display_name, True, "OK"))
            else:
                results.append(TestResult(exchange, market_type, symbol, display_name, False, "Request Failed"))

        candle_caps = caps.get("candles", {})
        if candle_caps.get("rest"):
            intervals = candle_caps.get("intervals", [])
            test_intervals = list(intervals[:2])
            if "1d" in intervals and "1d" not in test_intervals:
                test_intervals.append("1d")
            for interval in test_intervals:
                results.append(await verify_pagination_interval(client, exchange, market_type, symbol, interval))

        ws_channels = ["ticker", "book_ticker", "trades", "agg_trades", "orderbook", "mark_price", "liquidations"]
        for channel in ws_channels:
            if not caps.get(channel, {}).get("ws", False):
                continue
            results.append(await verify_websocket(exchange, market_type, symbol, channel))

    return results

def render_results(results: List[TestResult], colors: Dict[str, str]) -> None:
    table = Table(
        title="[bold]Exchange Router — Route Verification[/bold]",
        box=SIMPLE_HEAD,
        show_lines=False,
        header_style="bold",
        title_justify="left",
        pad_edge=False,
    )
    table.add_column("Exchange", no_wrap=True)
    table.add_column("Market", no_wrap=True)
    table.add_column("Symbol", no_wrap=True)
    table.add_column("Route", no_wrap=True)
    table.add_column("Status", justify="center", no_wrap=True)
    table.add_column("Details", overflow="fold")

    prev_ex: Optional[str] = None
    for i, r in enumerate(results):
        is_last_in_group = (i == len(results) - 1) or (results[i + 1].exchange != r.exchange)

        if r.exchange != prev_ex:
            color = colors.get(r.exchange, "white")
            ex_cell: Any = Text(r.exchange, style=f"bold {color}")
        else:
            ex_cell = ""

        status = Text("PASS", style="bold green") if r.success else Text("FAIL", style="bold red")
        detail_style = "dim" if r.success else "red"
        details = Text(r.details, style=detail_style)

        table.add_row(ex_cell, r.market, r.symbol, r.test_type, status, details, end_section=is_last_in_group)
        prev_ex = r.exchange

    console.print()
    console.print(table)

def render_summary(results: List[TestResult], colors: Dict[str, str]) -> int:
    stats: Dict[str, Dict[str, int]] = defaultdict(lambda: {"pass": 0, "fail": 0})
    for r in results:
        stats[r.exchange]["pass" if r.success else "fail"] += 1

    summary = Table(title="[bold]Summary[/bold]", box=SIMPLE_HEAD, header_style="bold", title_justify="left", pad_edge=False)
    summary.add_column("Exchange", no_wrap=True)
    summary.add_column("Passed", justify="right")
    summary.add_column("Failed", justify="right")
    summary.add_column("Total", justify="right")
    summary.add_column("Pass Rate", justify="right")

    total_pass = total_fail = 0
    for ex in sorted(stats.keys()):
        p, f = stats[ex]["pass"], stats[ex]["fail"]
        total_pass += p
        total_fail += f
        total = p + f
        rate = (p / total * 100) if total else 0.0
        color = colors.get(ex, "white")
        rate_style = "green" if f == 0 else ("yellow" if rate >= 80 else "red")
        summary.add_row(
            Text(ex, style=f"bold {color}"),
            Text(str(p), style="green"),
            Text(str(f), style="red" if f else "dim"),
            str(total),
            Text(f"{rate:.1f}%", style=rate_style),
        )

    total = total_pass + total_fail
    overall_rate = (total_pass / total * 100) if total else 0.0
    summary.add_row(
        Text("TOTAL", style="bold"),
        Text(str(total_pass), style="bold green"),
        Text(str(total_fail), style="bold red" if total_fail else "dim"),
        Text(str(total), style="bold"),
        Text(f"{overall_rate:.1f}%", style="bold green" if total_fail == 0 else "bold yellow"),
        end_section=True,
    )

    console.print()
    console.print(summary)
    console.print()
    return total_fail

async def main():
    results: List[TestResult] = []
    logger.info(f"Starting verification against {API_URL}")

    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        resp = await fetch(client, "/status")
        if not resp:
            logger.critical("Aborting: Service appears to be down!")
            return
        results.append(TestResult("GLOBAL", "ALL", "N/A", "Service Status", True, "OK"))

        ver_resp = await fetch(client, "/version")
        if ver_resp and "version" in ver_resp:
            results.append(TestResult("GLOBAL", "ALL", "N/A", "Version", True, f"v{ver_resp['version']}"))
        else:
            results.append(TestResult("GLOBAL", "ALL", "N/A", "Version", False, "Failed to fetch version"))

        resp = await fetch(client, "/exchanges")
        if not resp:
            results.append(TestResult("GLOBAL", "ALL", "N/A", "List Exchanges", False, "Failed to fetch"))
            logger.critical("Aborting: Could not fetch exchanges list!")
            return
        exchanges = resp.get("exchanges", [])
        if exchanges:
            results.append(TestResult("GLOBAL", "ALL", "N/A", "List Exchanges", True, f"Found {len(exchanges)}"))
        else:
            results.append(TestResult("GLOBAL", "ALL", "N/A", "List Exchanges", False, "No exchanges returned"))

        per_exchange = await asyncio.gather(*[verify_exchange(client, ex) for ex in exchanges])
        for chunk in per_exchange:
            results.extend(chunk)

    colors = exchange_colors(list({r.exchange for r in results}))
    colors["GLOBAL"] = "#8b949e"

    render_results(results, colors)
    failures = render_summary(results, colors)

    if failures:
        console.print(f"[bold red]CRITICAL:[/bold red] {failures} failures detected during verification.")
        exit(1)
    else:
        console.print("[bold green]SUCCESS:[/bold green] All systems operational.")

if __name__ == "__main__":
    asyncio.run(main())
