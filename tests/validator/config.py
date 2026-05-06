import os
import re
from typing import Dict, Iterable, Optional, Tuple


API_URL                            = os.getenv("API_URL", "http://localhost:8040")
TIMEOUT                            = float(os.getenv("TIMEOUT", "60.0"))
WS_TEST_DURATION                   = float(os.getenv("WS_TEST_DURATION", "180.0"))
MAX_CONCURRENT_EXCHANGES           = int(os.getenv("MAX_CONCURRENT_EXCHANGES", "5"))
MAX_CONCURRENT_REST_PER_EXCHANGE   = int(os.getenv("MAX_CONCURRENT_REST_PER_EXCHANGE", "4"))
MAX_CONCURRENT_WS_PER_EXCHANGE     = int(os.getenv("MAX_CONCURRENT_WS_PER_EXCHANGE", "32"))
MIN_REST_INTERVAL_MS               = float(os.getenv("MIN_REST_INTERVAL_MS", "250"))
LATENCY_WARN_MS                    = float(os.getenv("LATENCY_WARN_MS", "5000"))
SNAPSHOT_FRESHNESS_MS              = int(os.getenv("SNAPSHOT_FRESHNESS_MS", "300000"))


CRYPTO_EPOCH_MS = 1_262_304_000_000


INTERVAL_MS: Dict[str, int] = {
    "1m":  60_000,
    "3m":  180_000,
    "5m":  300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h":  3_600_000,
    "2h":  7_200_000,
    "4h":  14_400_000,
    "6h":  21_600_000,
    "8h":  28_800_000,
    "12h": 43_200_000,
    "1d":  86_400_000,
    "3d":  259_200_000,
    "1w":  604_800_000,
    "1M":  2_592_000_000,
}


WS_MIN_FRAMES: Dict[str, int] = {
    "orderbook":    10,
    "ticker":       5,
    "book_ticker":  5,
    "trades":       1,
    "agg_trades":   1,
    "mark_price":   1,
    "liquidations": 0,
}


FRESHNESS_OVERRIDE_MS: Dict[Tuple[str, str, str], int] = {
    ("kucoin", "inverse", "1m"): 600_000,
}


_MONTHS    = "JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC"
_LEVERAGED = re.compile(r"(?:^|[^A-Z])(UP|DOWN|BULL|BEAR|\d+[LS])(?:[^A-Z]|$)")
_DATED_NUM = re.compile(r"[-_]?\d{6,8}(?:[-_]|$)")
_DATED_MON = re.compile(rf"\d{{1,2}}(?:{_MONTHS})\d{{0,4}}")


def interval_ms(interval: str) -> int:
    return INTERVAL_MS.get(interval, 0)


def freshness_threshold_ms(exchange: str, market_type: str, period: Optional[str], period_ms: int) -> int:
    if period and (exchange, market_type, period) in FRESHNESS_OVERRIDE_MS:
        return FRESHNESS_OVERRIDE_MS[(exchange, market_type, period)]
    return 3 * period_ms + 30_000


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


def theoretical_max(period_ms: int, big_limit: int, retention_ms: Optional[int], now_ms: int) -> int:
    if period_ms <= 0:
        return big_limit
    age_cap = (now_ms - CRYPTO_EPOCH_MS) // period_ms
    ceiling = min(big_limit, age_cap)
    if retention_ms is not None:
        retention_cap = retention_ms // period_ms
        ceiling = min(ceiling, retention_cap)
    return max(ceiling, 1)
