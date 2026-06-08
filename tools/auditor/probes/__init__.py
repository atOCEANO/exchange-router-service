from typing import Any, Dict, List

from src.models import (
    AggTrade, BookTicker, Candle, FundingRate, Liquidation, LongShortRatio,
    MarkPrice, OpenInterest, Ticker,
)

from tools.auditor.config import BIG_LIMITS
from tools.auditor.probes.base import (
    Probe, ProbeContext, fetch,
)
from tools.auditor.probes.capabilities import CapabilitiesConsistencyProbe
from tools.auditor.probes.cross_route import CrossRouteConsistencyProbe
from tools.auditor.probes.errors import ErrorPathProbe
from tools.auditor.probes.info import InfoProbe
from tools.auditor.probes.markets import MarketsProbe
from tools.auditor.probes.orderbook import OrderBookProbe
from tools.auditor.probes.paginated import PaginatedProbe
from tools.auditor.probes.snapshot import SnapshotProbe
from tools.auditor.probes.symbol import SymbolResolutionProbe
from tools.auditor.probes.trades import TradesProbe
from tools.auditor.probes.websocket import WS_CHANNEL_MODEL, WebSocketProbe


__all__ = [
    "Probe",
    "ProbeContext",
    "fetch",
    "WS_CHANNEL_MODEL",
    "market_probes",
    "exchange_probes",
]


def market_probes(caps_slice: Dict[str, Any]) -> List[Probe]:
    probes: List[Probe] = []

    probes.append(InfoProbe())
    probes.append(MarketsProbe())
    probes.append(SymbolResolutionProbe())
    probes.append(CapabilitiesConsistencyProbe())

    probes.append(SnapshotProbe("ticker",      Ticker))
    probes.append(SnapshotProbe("book_ticker", BookTicker))
    probes.append(SnapshotProbe("mark_price",  MarkPrice))

    probes.append(OrderBookProbe())
    probes.append(TradesProbe())

    probes.append(PaginatedProbe("agg_trades",   AggTrade,    big_limit=BIG_LIMITS["agg_trades"]))
    probes.append(PaginatedProbe("funding_rate", FundingRate, big_limit=BIG_LIMITS["funding_rate"]))
    probes.append(PaginatedProbe("liquidations", Liquidation, big_limit=BIG_LIMITS["liquidations"], allow_empty=True))

    candle_caps = caps_slice.get("candles", {}) or {}
    for interval in candle_caps.get("intervals", []) or []:
        probes.append(PaginatedProbe("candles", Candle, big_limit=BIG_LIMITS["candles"], period=interval, period_param="interval"))

    oi_caps = caps_slice.get("open_interest", {}) or {}
    for interval in oi_caps.get("intervals", ["1h"]) or ["1h"]:
        probes.append(PaginatedProbe("open_interest", OpenInterest, big_limit=BIG_LIMITS["open_interest"], period=interval, period_param="period"))

    ls_caps = caps_slice.get("long_short_ratio", {}) or {}
    for interval in ls_caps.get("intervals", ["1h"]) or ["1h"]:
        probes.append(PaginatedProbe("long_short_ratio", LongShortRatio, big_limit=BIG_LIMITS["long_short_ratio"], period=interval, period_param="period"))

    probes.append(CrossRouteConsistencyProbe())

    for channel in WS_CHANNEL_MODEL:
        probes.append(WebSocketProbe(channel))

    return probes


def exchange_probes() -> List[Probe]:
    return [ErrorPathProbe()]
