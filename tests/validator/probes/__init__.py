from typing import Any, Dict, List

from src.models import (
    AggTrade, BookTicker, Candle, FundingRate, Liquidation, LongShortRatio,
    MarkPrice, OpenInterest, Ticker,
)

from tests.validator.probes.base import (
    Probe, ProbeContext, fetch,
)
from tests.validator.probes.capabilities import CapabilitiesConsistencyProbe
from tests.validator.probes.cross_route import CrossRouteConsistencyProbe
from tests.validator.probes.errors import ErrorPathProbe
from tests.validator.probes.info import InfoProbe
from tests.validator.probes.markets import MarketsProbe
from tests.validator.probes.orderbook import OrderBookProbe
from tests.validator.probes.paginated import PaginatedProbe
from tests.validator.probes.snapshot import SnapshotProbe
from tests.validator.probes.symbol import SymbolResolutionProbe
from tests.validator.probes.trades import TradesProbe
from tests.validator.probes.websocket import WS_CHANNEL_MODEL, WebSocketProbe


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

    probes.append(PaginatedProbe("agg_trades",   AggTrade,    big_limit=1000))
    probes.append(PaginatedProbe("funding_rate", FundingRate, big_limit=500))
    probes.append(PaginatedProbe("liquidations", Liquidation, big_limit=500, allow_empty=True))

    candle_caps = caps_slice.get("candles", {}) or {}
    for interval in candle_caps.get("intervals", []) or []:
        probes.append(PaginatedProbe("candles", Candle, big_limit=3000, period=interval, period_param="interval"))

    oi_caps = caps_slice.get("open_interest", {}) or {}
    for interval in oi_caps.get("intervals", ["1h"]) or ["1h"]:
        probes.append(PaginatedProbe("open_interest", OpenInterest, big_limit=500, period=interval, period_param="period"))

    ls_caps = caps_slice.get("long_short_ratio", {}) or {}
    for interval in ls_caps.get("intervals", ["1h"]) or ["1h"]:
        probes.append(PaginatedProbe("long_short_ratio", LongShortRatio, big_limit=500, period=interval, period_param="period"))

    probes.append(CrossRouteConsistencyProbe())

    for channel in WS_CHANNEL_MODEL:
        probes.append(WebSocketProbe(channel))

    return probes


def exchange_probes() -> List[Probe]:
    return [ErrorPathProbe()]
