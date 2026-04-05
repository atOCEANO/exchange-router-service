from enum import Enum
from typing import List, Literal, Optional
from pydantic import BaseModel, Field


class MarketType(str, Enum):
    SPOT = "spot"
    LINEAR = "linear"
    INVERSE = "inverse"


class SymbolInfo(BaseModel):
    symbol: str
    base_asset: str
    quote_asset: str
    price_precision: int
    quantity_precision: int
    min_qty: float
    max_qty: float
    min_notional: float
    status: str


class Ticker(BaseModel):
    symbol: str
    price: float = Field(..., description="Last traded price")
    open_24h: float
    high_24h: float
    low_24h: float
    volume_24h: float = Field(..., description="Base asset volume")
    quote_volume_24h: float = Field(..., description="Quote asset volume")
    price_change_percent: float
    timestamp: int = Field(..., description="Unix timestamp in milliseconds")


class BookTicker(BaseModel):
    symbol: str
    bid_price: float
    bid_qty: float
    ask_price: float
    ask_qty: float
    timestamp: int


class MarkPrice(BaseModel):
    symbol: str
    mark_price: float
    index_price: float
    funding_rate: float
    next_funding_time: int
    timestamp: int


class Trade(BaseModel):
    id: str
    price: float
    qty: float
    side: Literal["buy", "sell"]
    timestamp: int = Field(..., description="Unix timestamp in milliseconds")


class AggTrade(BaseModel):
    agg_id: str
    price: float
    qty: float
    first_trade_id: str
    last_trade_id: str
    side: Literal["buy", "sell"]
    timestamp: int


class Candle(BaseModel):
    timestamp: int = Field(..., description="Open time in unix milliseconds")
    open: float
    high: float
    low: float
    close: float
    volume: float


class OrderBook(BaseModel):
    symbol: str
    bids: List[List[float]]
    asks: List[List[float]]
    timestamp: int


class FundingRate(BaseModel):
    symbol: str
    rate: float
    timestamp: int


class OpenInterest(BaseModel):
    symbol: str
    open_interest: float
    value_usd: float = 0.0
    timestamp: int


class Liquidation(BaseModel):
    symbol: str
    side: Literal["buy", "sell"]
    price: float
    qty: float
    timestamp: int


class LongShortRatio(BaseModel):
    symbol: str
    ratio: float
    long_account: float
    short_account: float
    timestamp: int