from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any, Union, AsyncGenerator
from datetime import datetime, timezone
from src.models import (
    Ticker, BookTicker, MarkPrice, OrderBook, Candle, Trade, AggTrade,
    MarketType, SymbolInfo, FundingRate, OpenInterest, Liquidation, LongShortRatio
)



class BaseExchange(ABC):
    def __init__(self):
        pass


    @abstractmethod
    async def shutdown(self):
        pass


    def normalize_symbol(self, symbol: str) -> str:
        return symbol.replace("/", "").replace("-", "").upper()


    def normalize_timestamp(self, ts: Union[int, float, str]) -> int:
        if isinstance(ts, str):
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                return int(dt.timestamp() * 1000)
            except ValueError:
                try:
                    ts = float(ts)
                except ValueError:
                    raise ValueError(f"Could not parse timestamp string: {ts}")
        if isinstance(ts, (int, float)):
            if ts < 100_000_000_000:
                return int(ts * 1000)
            return int(ts)
        raise ValueError(f"Unsupported timestamp format: {type(ts)}")


    def normalize_interval(self, interval: str) -> str:
        return interval


    @property
    @abstractmethod
    def name(self) -> str:
        pass


    @property
    @abstractmethod
    def supported_market_types(self) -> List[MarketType]:
        pass


    @abstractmethod
    def get_capabilities(self) -> Dict[str, Any]:
        pass


    async def get_status(self) -> Dict[str, Any]:
        return {"status": "online", "exchange": self.name}


    @abstractmethod
    async def get_exchange_info(self, market_type: MarketType) -> List[SymbolInfo]:
        pass


    @abstractmethod
    async def get_markets(self, market_type: MarketType) -> List[str]:
        pass


    @abstractmethod
    async def get_ticker(self, market_type: MarketType, symbol: str) -> Ticker:
        pass


    @abstractmethod
    async def get_book_ticker(self, market_type: MarketType, symbol: str) -> BookTicker:
        pass


    async def get_mark_price(self, market_type: MarketType, symbol: str) -> MarkPrice:
        raise NotImplementedError(f"Mark Price not supported on {self.name} {market_type}")


    @abstractmethod
    async def get_orderbook(self, market_type: MarketType, symbol: str, depth: int = 20) -> OrderBook:
        pass


    @abstractmethod
    async def get_trades(self, market_type: MarketType, symbol: str, limit: int = 100) -> List[Trade]:
        pass


    @abstractmethod
    async def get_agg_trades(self, market_type: MarketType, symbol: str, start_time: Optional[int] = None, limit: int = 500) -> List[AggTrade]:
        pass


    @abstractmethod
    async def get_candles(self, market_type: MarketType, symbol: str, interval: str, start_time: Optional[int] = None, limit: int = 100) -> List[Candle]:
        pass


    @abstractmethod
    async def stream_ticker(self, market_type: MarketType, symbol: str) -> AsyncGenerator[Ticker, None]:
        pass


    @abstractmethod
    async def stream_book_ticker(self, market_type: MarketType, symbol: str) -> AsyncGenerator[BookTicker, None]:
        pass


    async def stream_mark_price(self, market_type: MarketType, symbol: str) -> AsyncGenerator[MarkPrice, None]:
        if False: yield
        raise NotImplementedError(f"Mark Price stream not supported on {self.name} {market_type}")


    async def stream_agg_trades(self, market_type: MarketType, symbol: str) -> AsyncGenerator[AggTrade, None]:
        if False: yield
        raise NotImplementedError(f"AggTrade stream not supported on {self.name} {market_type}")


    @abstractmethod
    async def stream_trades(self, market_type: MarketType, symbol: str) -> AsyncGenerator[Trade, None]:
        pass


    @abstractmethod
    async def stream_orderbook(self, market_type: MarketType, symbol: str, depth: int = 20, update_speed: str = "100ms") -> AsyncGenerator[OrderBook, None]:
        pass


    async def stream_liquidations(self, market_type: MarketType, symbol: str) -> AsyncGenerator[Liquidation, None]:
        if False: yield
        raise NotImplementedError(f"Liquidation stream not supported on {self.name} {market_type}")


    async def get_open_interest(self, market_type: MarketType, symbol: str, period: str = "1h", start_time: Optional[int] = None, limit: int = 30) -> List[OpenInterest]:
        raise NotImplementedError(f"Open Interest not supported on {self.name} {market_type}")


    async def get_funding_rate(self, market_type: MarketType, symbol: str, start_time: Optional[int] = None, limit: int = 100) -> List[FundingRate]:
        raise NotImplementedError(f"Funding Rate not supported on {self.name} {market_type}")


    async def get_liquidations(self, market_type: MarketType, symbol: str, start_time: Optional[int] = None, limit: int = 100) -> List[Liquidation]:
        raise NotImplementedError(f"Liquidations not supported on {self.name} {market_type}")


    async def get_long_short_ratio(self, market_type: MarketType, symbol: str, period: str = "5m", start_time: Optional[int] = None, limit: int = 30) -> List[LongShortRatio]:
        raise NotImplementedError(f"L/S Ratio not supported on {self.name} {market_type}")