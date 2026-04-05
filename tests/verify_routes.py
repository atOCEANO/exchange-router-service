# Integration tests for the Exchange Router Service.
# Run against a live service: API_URL=http://localhost:8040 python tests/verify_routes.py

import os
import asyncio
import httpx
import logging
import websockets
import json
from typing import List, Dict, Optional, Any
from dataclasses import dataclass

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

API_URL = os.getenv("API_URL", "http://localhost:8040")
TIMEOUT = float(os.getenv("TIMEOUT", "60.0"))

@dataclass
class TestResult:
    exchange: str
    market: str
    symbol: str
    test_type: str
    success: bool
    details: str = ""

async def fetch(client: httpx.AsyncClient, endpoint: str, params: Optional[Dict] = None) -> Any:
    """GET helper. Returns 'NOT_IMPLEMENTED' on 501, None on any other error."""
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
    """Fetches 1500 candles and checks timestamps are strictly ascending."""
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
    """Connects, subscribes to a channel, and waits for the first message."""
    if API_URL.startswith("https://"):
        base_ws_url = API_URL.replace("https://", "wss://")
    else:
        base_ws_url = API_URL.replace("http://", "ws://")

    ws_url = f"{base_ws_url}/ws/{exchange}/{market_type}"
    start_ts = asyncio.get_event_loop().time()

    try:
        async with websockets.connect(ws_url) as ws:
            await ws.send(json.dumps({"channel": channel, "symbol": symbol}))
            await asyncio.wait_for(ws.recv(), timeout=5.0)
            latency = (asyncio.get_event_loop().time() - start_ts) * 1000
            return TestResult(exchange, market_type, symbol, f"WS:{channel}", True, f"Connected, latency: {latency:.1f}ms")
    except asyncio.TimeoutError:
        return TestResult(exchange, market_type, symbol, f"WS:{channel}", False, "Connection/Response Timeout")
    except Exception as e:
        return TestResult(exchange, market_type, symbol, f"WS:{channel}", False, f"Error: {str(e)}")

async def verify_exchange(client: httpx.AsyncClient, exchange: str, results: List[TestResult]) -> None:
    """Runs all REST and WS tests for one exchange, driven by its capabilities map."""
    status_resp = await fetch(client, f"/{exchange}/status")
    results.append(TestResult(exchange, "ALL", "N/A", "Status", bool(status_resp), "OK" if status_resp else "Failed"))

    types_resp = await fetch(client, f"/{exchange}/market_types")
    results.append(TestResult(exchange, "ALL", "N/A", "Market Types", bool(types_resp), "OK" if types_resp else "Failed"))

    cap_resp = await fetch(client, f"/{exchange}/capabilities")
    if not cap_resp:
        results.append(TestResult(exchange, "ALL", "N/A", "Capabilities", False, "Failed to fetch map"))
        return
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

        priority = ["BTCUSDT", "ETHUSDT", "BTCUSD", "ETHUSD", "BTCUSD_PERP", "ETHUSD_PERP"]
        selected_symbols = [s for s in priority if s in sym_resp] or sym_resp[:1]

        for symbol in selected_symbols:

            # REST endpoint tests — driven by cap["feature"]["rest"]
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

            # Candle pagination tests — driven by cap["candles"]["rest"]
            candle_caps = caps.get("candles", {})
            if candle_caps.get("rest"):
                test_intervals = candle_caps.get("intervals", [])[:2]
                if "1d" in candle_caps.get("intervals", []) and "1d" not in test_intervals:
                    test_intervals.append("1d")
                for interval in test_intervals:
                    results.append(await verify_pagination_interval(client, exchange, market_type, symbol, interval))

            # WebSocket channel tests — driven by cap["feature"]["ws"]
            ws_channels = [
                "ticker", "book_ticker", "trades", "agg_trades",
                "orderbook", "mark_price", "liquidations",
            ]
            for channel in ws_channels:
                if not caps.get(channel, {}).get("ws", False):
                    continue
                results.append(await verify_websocket(exchange, market_type, symbol, channel))

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

        await asyncio.gather(*[verify_exchange(client, ex, results) for ex in exchanges])

    print("\n" + "="*95)
    print(f"{'EXCHANGE':<10} | {'MARKET':<8} | {'SYMBOL':<15} | {'TEST':<15} | {'STATUS':<6} | {'DETAILS'}")
    print("-" * 95)

    failures = []
    for r in results:
        status = "PASS" if r.success else "FAIL"
        if not r.success:
            failures.append(r)
        print(f"{r.exchange:<10} | {r.market:<8} | {r.symbol:<15} | {r.test_type:<15} | {status:<6} | {r.details}")

    print("="*95)

    if failures:
        print(f"\nCRITICAL: {len(failures)} failures detected during verification.")
        exit(1)
    else:
        print("\nSUCCESS: All systems operational.")

if __name__ == "__main__":
    asyncio.run(main())
