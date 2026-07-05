from enum import Enum
from typing import List, Literal, Optional
from pydantic import BaseModel, Field, model_validator


class MarketType(str, Enum):
    SPOT = "spot"
    LINEAR = "linear"
    INVERSE = "inverse"


class UsdBasis(BaseModel):
    method: Literal["close", "contract_size", "candle_close"] = Field(
        ...,
        description="How the parent record's usd field was computed. 'close' uses the bucket's own close price (approximate, used on linear/spot Candle.volume and Ticker.volume_24h). 'contract_size' multiplies by the fixed contract notional (exact, used on inverse). 'candle_close' joins to a same-period Candle.close at the row's timestamp (used on linear OpenInterest where the row carries no price).",
    )
    close: Optional[float] = Field(
        None,
        description="Close price used in the conversion. Populated when method == 'candle_close' AND the join succeeded. Null when method != 'candle_close', or when method == 'candle_close' but no matching candle was found (in which case the parent record's usd is also null). In quote currency.",
    )
    close_ts: Optional[int] = Field(
        None,
        description="Timestamp in unix milliseconds of the candle whose close was used. Same nullability rules as close.",
    )


    @model_validator(mode="after")
    def _close_pair_consistency(self) -> "UsdBasis":
        if (self.close is None) != (self.close_ts is None):
            raise ValueError("usd_basis: close and close_ts must both be set or both be null")
        return self


class QtyValue(BaseModel):
    native: float = Field(
        ...,
        description="Quantity in the unit indicated by `unit`. Raw upstream value.",
    )
    unit: Literal["base", "contract"] = Field(
        ...,
        description="What the `native` figure is measured in. 'base' on spot and linear markets (qty is in the base asset, e.g. BTC). 'contract' on inverse markets (qty is in contracts, each worth `contract_size` units of the quote asset).",
    )
    contract_size: Optional[float] = Field(
        None,
        description="Quote-asset value of one contract. Populated only when unit == 'contract'. E.g. 100 means each contract is 100 USD of notional on a USD-quoted inverse market; 1 on Kraken futures where contract_size == 1.",
    )
    usd: float = Field(
        ...,
        description="Quote-currency notional. For unit == 'base': native * price (uses the parent row's price field). For unit == 'contract': native * contract_size (price-independent). 'usd' here means quote-currency; on a non-USD-quoted pair it is the quote value (BTC, EUR, etc.).",
    )


    @model_validator(mode="after")
    def _contract_requires_size(self) -> "QtyValue":
        if self.unit == "contract" and self.contract_size is None:
            raise ValueError("QtyValue.contract_size must be set when unit == 'contract'")
        return self


class VolumeValue(BaseModel):
    native: float = Field(
        ...,
        description="Volume in the unit indicated by `unit`. Raw upstream value.",
    )
    unit: Literal["base", "contract"] = Field(
        ...,
        description="What the `native` figure is measured in. 'base' on spot and linear markets, 'contract' on inverse.",
    )
    contract_size: Optional[float] = Field(
        None,
        description="Quote-asset value of one contract. Populated only when unit == 'contract'.",
    )
    usd: float = Field(
        ...,
        description="Quote-currency turnover. For unit == 'base': native * close (close-as-VWAP-proxy; see usd_basis). For unit == 'contract': native * contract_size (exact).",
    )
    usd_basis: UsdBasis = Field(
        ...,
        description="How `usd` was computed. method == 'close' for linear/spot (approximate); 'contract_size' for inverse (exact).",
    )


    @model_validator(mode="after")
    def _contract_requires_size(self) -> "VolumeValue":
        if self.unit == "contract" and self.contract_size is None:
            raise ValueError("VolumeValue.contract_size must be set when unit == 'contract'")
        return self


class OiValue(BaseModel):
    native: float = Field(
        ...,
        description="Open interest in the unit indicated by `unit`. Raw upstream value.",
    )
    unit: Literal["base", "contract"] = Field(
        ...,
        description="What the `native` figure is measured in. 'base' on linear, 'contract' on inverse.",
    )
    contract_size: Optional[float] = Field(
        None,
        description="Quote-asset value of one contract. Populated only when unit == 'contract'.",
    )
    usd: Optional[float] = Field(
        None,
        description="Quote-currency notional outstanding. Null on linear when the candle-close join fails (raw native is always populated; usd_basis.method tells you why). Exact on inverse (native * contract_size).",
    )
    usd_basis: UsdBasis = Field(
        ...,
        description="How `usd` was computed. 'contract_size' on inverse (exact); 'candle_close' on linear (joined to same-period candle's close at the row's timestamp).",
    )


    @model_validator(mode="after")
    def _contract_requires_size(self) -> "OiValue":
        if self.unit == "contract" and self.contract_size is None:
            raise ValueError("OiValue.contract_size must be set when unit == 'contract'")
        return self


class FundingConvention(BaseModel):
    kind: Literal["discrete", "continuous"] = Field(
        ...,
        description="Categorical funding model for this symbol. 'discrete' venues settle at fixed cycles. 'continuous' venues accrue smoothly and are sampled at a fixed cadence. The cycle_ms parameter lives on MarkPrice.funding and FundingRate.rate, not on SymbolInfo, because it is a parameter the exchange could change without affecting this categorical fact.",
    )


class FundingCurrent(BaseModel):
    kind: Literal["discrete", "continuous"] = Field(
        ...,
        description="Funding model. 'discrete' means per_cycle is the rate to be charged at valid_until_ts. 'continuous' means per_cycle is the rate accrued over each cycle_ms-length window; partial holds pay proportionally.",
    )
    per_cycle: float = Field(
        ...,
        description="Funding rate per cycle of length cycle_ms, dimensionless decimal (e.g. 0.0001 = 0.01%). On discrete: the estimate of what will be charged at the next settlement. On continuous: the instantaneous per-cycle-equivalent accrual rate sampled at the parent row's timestamp.",
    )
    cycle_ms: int = Field(
        ...,
        description="Cycle length in milliseconds. On discrete: the settlement interval (e.g. 28_800_000 for 8h). On continuous: the sample-window length (3_600_000 for Kraken's hourly sampling).",
    )
    valid_until_ts: int = Field(
        ...,
        description="Unix milliseconds at which this rate stops being the current one. On discrete: hard commitment, the exact next-settlement moment. On continuous: expected next-sample moment, a prediction not a commitment.",
    )


class FundingHistorical(BaseModel):
    kind: Literal["discrete", "continuous"] = Field(
        ...,
        description="Funding model. 'discrete' rows are settlement events; 'continuous' rows are sample observations.",
    )
    per_cycle: float = Field(
        ...,
        description="Rate per cycle. On discrete: the rate actually charged at the parent row's timestamp. On continuous: the instantaneous per-cycle-equivalent rate observed at the parent row's timestamp.",
    )
    cycle_ms: int = Field(
        ...,
        description="Cycle length in milliseconds. Stable per symbol on discrete (8h or 4h cycles); always 3_600_000 on continuous (Kraken sample window).",
    )


class SymbolInfo(BaseModel):
    symbol: str = Field(
        ...,
        description="Routing-facing symbol; exchange-native form with contract-type prefixes stripped (e.g. 'BTCUSDT' on Binance, 'XBTUSD' on Kraken futures where the PI_ prefix is stripped). No base-currency translation (XBT stays XBT).",
    )
    native_symbol: str = Field(
        ...,
        description="Raw upstream symbol when it differs from the routing-facing one (e.g. 'BTCUSD_PERP' on Binance COIN-M, 'PI_XBTUSD' on Kraken futures).",
    )
    base_asset: str = Field(
        ...,
        description="Ticker of the base currency (left side of the pair, e.g. 'BTC' in BTC/USDT, 'XBT' on Kraken). Exchange-native casing.",
    )
    quote_asset: str = Field(
        ...,
        description="Ticker of the quote currency (right side of the pair, e.g. 'USDT' in BTC/USDT). All price fields on every record are denominated in this asset. Carried at the row level as `quote` on every data record so each row is interpretable in isolation.",
    )
    price_precision: int = Field(
        ...,
        description="Decimal places for price (not tick size). A symbol with a 0.5 tick is not expressible here.",
    )
    quantity_precision: int = Field(
        ...,
        description="Decimal places for quantity (not step size).",
    )
    min_qty: Optional[float] = Field(
        None,
        description="Minimum order quantity in qty_unit. Null when the exchange does not declare a minimum.",
    )
    max_qty: Optional[float] = Field(
        None,
        description="Maximum order quantity in qty_unit. Null when the exchange does not declare a maximum.",
    )
    min_notional: Optional[float] = Field(
        None,
        description="Minimum order notional in quote_asset. Null when the exchange does not declare a minimum.",
    )
    qty_unit: Literal["base", "contract"] = Field(
        ...,
        description="Unit that min_qty / max_qty are expressed in. 'base' on spot and linear, 'contract' on inverse.",
    )
    contract_size: Optional[float] = Field(
        None,
        description="Quote-asset notional per contract. Null for spot/linear; set on inverse perpetuals (e.g. 100 on Binance BTCUSD, 1 on Kraken XBTUSD).",
    )
    funding: Optional[FundingConvention] = Field(
        None,
        description="Funding convention categorical descriptor. Null on spot. {'kind': 'discrete'} on discrete-funding linear and inverse markets. {'kind': 'continuous'} on continuous-funding markets.",
    )


class Ticker(BaseModel):
    symbol: str = Field(
        ...,
        description="Routing-facing symbol matching SymbolInfo.symbol.",
    )
    market_type: MarketType = Field(
        ...,
        description="Market provenance of this row.",
    )
    quote: str = Field(
        ...,
        description="Quote currency for all price-denominated fields on this row, matching SymbolInfo.quote_asset.",
    )
    price: float = Field(
        ...,
        description="Last traded price in quote. Can lag on thinly-traded inverse markets; consult MarkPrice for current fair value.",
    )
    open_24h: float = Field(
        ...,
        description="Price in quote 24 hours before timestamp, used as the open of the rolling 24h window.",
    )
    high_24h: float = Field(
        ...,
        description="Highest price in quote over the rolling 24h window ending at timestamp.",
    )
    low_24h: float = Field(
        ...,
        description="Lowest price in quote over the rolling 24h window ending at timestamp.",
    )
    volume_24h: VolumeValue = Field(
        ...,
        description="24h volume nested as native + unit + usd + usd_basis. See VolumeValue.",
    )
    price_change_percent: float = Field(
        ...,
        description="24h price change as a percent. 5.0 means +5%, not +0.05.",
    )
    timestamp: int = Field(
        ...,
        description="Server snapshot time in unix milliseconds. Some adapters fall back to router-receive time when the upstream endpoint omits a server timestamp.",
    )


class BookTicker(BaseModel):
    symbol: str = Field(
        ...,
        description="Routing-facing symbol matching SymbolInfo.symbol.",
    )
    market_type: MarketType = Field(
        ...,
        description="Market provenance of this row.",
    )
    quote: str = Field(
        ...,
        description="Quote currency for price fields.",
    )
    bid_price: float = Field(
        ...,
        description="Best bid price in quote.",
    )
    bid_qty: QtyValue = Field(
        ...,
        description="Quantity resting at bid_price, nested as native + unit + usd.",
    )
    ask_price: float = Field(
        ...,
        description="Best ask price in quote.",
    )
    ask_qty: QtyValue = Field(
        ...,
        description="Quantity resting at ask_price, nested as native + unit + usd.",
    )
    timestamp: int = Field(
        ...,
        description="Server snapshot time in unix milliseconds.",
    )


class MarkPrice(BaseModel):
    symbol: str = Field(
        ...,
        description="Routing-facing symbol matching SymbolInfo.symbol.",
    )
    market_type: MarketType = Field(
        ...,
        description="Market provenance. Always 'linear' or 'inverse' (no mark price on spot).",
    )
    quote: str = Field(
        ...,
        description="Quote currency.",
    )
    mark_price: float = Field(
        ...,
        description="Fair-value-anchored price in quote. Funding settles on this; liquidations and margin reference this rather than last-traded.",
    )
    index_price: Optional[float] = Field(
        None,
        description="Spot index reference in quote used to compute the mark. Null when the upstream payload omits an index price.",
    )
    funding: Optional[FundingCurrent] = Field(
        None,
        description="Current funding view. Null when the upstream payload omits funding info. See FundingCurrent.",
    )
    timestamp: int = Field(
        ...,
        description="Snapshot time in unix milliseconds, as reported by the exchange.",
    )


    @model_validator(mode="after")
    def _funding_valid_until_after_timestamp(self) -> "MarkPrice":
        if self.funding is not None and self.funding.kind == "discrete":
            if self.funding.valid_until_ts < self.timestamp:
                raise ValueError(
                    f"discrete funding.valid_until_ts ({self.funding.valid_until_ts}) must be >= timestamp ({self.timestamp})"
                )
        return self


class Trade(BaseModel):
    id: Optional[str] = Field(
        None,
        description="Exchange-native trade id when available, with an adapter-synthesized event-time fallback on rows where the upstream omits it. Null when neither is available. Not all exchanges produce sortable values; do not rely on lexicographic ordering. Sort by timestamp for chronology.",
    )
    symbol: str = Field(
        ...,
        description="Routing-facing symbol matching SymbolInfo.symbol.",
    )
    market_type: MarketType = Field(
        ...,
        description="Market provenance of this row.",
    )
    quote: str = Field(
        ...,
        description="Quote currency.",
    )
    price: float = Field(
        ...,
        description="Execution price of this trade, in quote.",
    )
    qty: QtyValue = Field(
        ...,
        description="Trade size nested as native + unit + usd.",
    )
    side: Literal["buy", "sell"] = Field(
        ...,
        description="Taker side / aggressor side. A 'buy' means the taker hit the asks (a sell limit was filled).",
    )
    timestamp: int = Field(
        ...,
        description="Event time in unix milliseconds, as reported by the exchange.",
    )


class AggTrade(BaseModel):
    agg_id: str = Field(
        ...,
        description="Exchange-native aggregate-trade id, stringified.",
    )
    symbol: str = Field(
        ...,
        description="Routing-facing symbol matching SymbolInfo.symbol.",
    )
    market_type: MarketType = Field(
        ...,
        description="Market provenance of this row.",
    )
    quote: str = Field(
        ...,
        description="Quote currency.",
    )
    price: float = Field(
        ...,
        description="Price of the aggregated group, in quote. Same-price trades on the same side are what the exchange folds into one AggTrade.",
    )
    qty: QtyValue = Field(
        ...,
        description="Aggregated size nested as native + unit + usd.",
    )
    first_trade_id: str = Field(
        ...,
        description="Stringified id of the first constituent trade in this aggregation.",
    )
    last_trade_id: str = Field(
        ...,
        description="Stringified id of the last constituent trade in this aggregation.",
    )
    side: Literal["buy", "sell"] = Field(
        ...,
        description="Taker side / aggressor side.",
    )
    timestamp: int = Field(
        ...,
        description="Last-trade time of the aggregated group, in unix milliseconds.",
    )


class Candle(BaseModel):
    symbol: str = Field(
        ...,
        description="Routing-facing symbol matching SymbolInfo.symbol.",
    )
    market_type: MarketType = Field(
        ...,
        description="Market provenance of this row.",
    )
    quote: str = Field(
        ...,
        description="Quote currency.",
    )
    interval: str = Field(
        ...,
        description="Bucket size as a canonical string matching the consumer's `period` parameter, e.g. '1m', '5m', '1h', '1d'.",
    )
    timestamp: int = Field(
        ...,
        description="Bucket open time in unix milliseconds on every exchange.",
    )
    open: float = Field(
        ...,
        description="First traded price of the bucket, in quote.",
    )
    high: float = Field(
        ...,
        description="Highest traded price during the bucket, in quote.",
    )
    low: float = Field(
        ...,
        description="Lowest traded price during the bucket, in quote.",
    )
    close: float = Field(
        ...,
        description="Last traded price of the bucket, in quote.",
    )
    volume: VolumeValue = Field(
        ...,
        description="Bucket volume nested as native + unit + usd + usd_basis. usd_basis.method is 'close' on linear/spot and 'contract_size' on inverse.",
    )


class OrderBook(BaseModel):
    symbol: str = Field(
        ...,
        description="Routing-facing symbol matching SymbolInfo.symbol.",
    )
    market_type: MarketType = Field(
        ...,
        description="Market provenance of this row.",
    )
    quote: str = Field(
        ...,
        description="Quote currency for prices in bids/asks.",
    )
    bids: List[List[float]] = Field(
        ...,
        description="Array of [price, qty] pairs sorted best (highest) first. Price is in quote; qty is in qty_unit. Per-level USD is computed by the consumer as price * qty (intentionally not pre-computed to keep depth payloads small).",
    )
    asks: List[List[float]] = Field(
        ...,
        description="Array of [price, qty] pairs sorted best (lowest) first. Same units as bids.",
    )
    qty_unit: Literal["base", "contract"] = Field(
        ...,
        description="Unit covering qty on both bids and asks.",
    )
    timestamp: int = Field(
        ...,
        description="Depth snapshot time in unix milliseconds.",
    )


class FundingRate(BaseModel):
    symbol: str = Field(
        ...,
        description="Routing-facing symbol matching SymbolInfo.symbol.",
    )
    market_type: MarketType = Field(
        ...,
        description="Market provenance. Always 'linear' or 'inverse'.",
    )
    quote: str = Field(
        ...,
        description="Quote currency.",
    )
    rate: FundingHistorical = Field(
        ...,
        description="Historical funding view nested as kind + per_cycle + cycle_ms. See FundingHistorical.",
    )
    timestamp: int = Field(
        ...,
        description="Settlement time (discrete) or sample observation time (continuous), in unix milliseconds.",
    )


class OpenInterest(BaseModel):
    symbol: str = Field(
        ...,
        description="Routing-facing symbol matching SymbolInfo.symbol.",
    )
    market_type: MarketType = Field(
        ...,
        description="Market provenance. Always 'linear' or 'inverse'.",
    )
    quote: str = Field(
        ...,
        description="Quote currency.",
    )
    interval: str = Field(
        ...,
        description="Bucket size as a canonical string, e.g. '5m', '1h'. Matches the route's `period` parameter.",
    )
    open_interest: OiValue = Field(
        ...,
        description="Open interest nested as native + unit + usd + usd_basis. usd_basis.method is 'candle_close' on linear (joined server-side); 'contract_size' on inverse (exact).",
    )
    timestamp: int = Field(
        ...,
        description="Bucket close time in unix milliseconds (end of `interval`).",
    )


class Liquidation(BaseModel):
    symbol: str = Field(
        ...,
        description="Routing-facing symbol matching SymbolInfo.symbol.",
    )
    market_type: MarketType = Field(
        ...,
        description="Market provenance. Always 'linear' or 'inverse'.",
    )
    quote: str = Field(
        ...,
        description="Quote currency.",
    )
    side: Literal["buy", "sell"] = Field(
        ...,
        description="The liquidation order's book side, which is the opposite of the liquidated position's side. A long position being liquidated produces side='sell'. The feed is partial on every public exchange; see capabilities.liquidations.completeness.",
    )
    price: float = Field(
        ...,
        description="Price at which the liquidation order was executed (or bankruptcy reference, depending on what the upstream reports), in quote.",
    )
    qty: QtyValue = Field(
        ...,
        description="Liquidation size nested as native + unit + usd.",
    )
    timestamp: int = Field(
        ...,
        description="Event time in unix milliseconds, as reported by the exchange.",
    )


class LongShortRatio(BaseModel):
    symbol: str = Field(
        ...,
        description="Routing-facing symbol matching SymbolInfo.symbol.",
    )
    market_type: MarketType = Field(
        ...,
        description="Market provenance. Always 'linear' or 'inverse'.",
    )
    interval: str = Field(
        ...,
        description="Bucket size as a canonical string, e.g. '5m', '1h'.",
    )
    ratio: float = Field(
        ...,
        description="long_account / short_account. Dimensionless; > 1 means more accounts are long than short within the account_scope.",
    )
    long_account: Optional[float] = Field(
        None,
        description="Fraction of accounts (or top-trader accounts, per account_scope) on the long side, in [0, 1]. Null on exchanges that expose only the ratio (account_scope == 'opaque').",
    )
    short_account: Optional[float] = Field(
        None,
        description="Fraction of accounts on the short side, in [0, 1]. Null when account_scope == 'opaque'.",
    )
    account_scope: Literal["top_traders", "all_accounts", "opaque"] = Field(
        ...,
        description="Whose accounts are counted: 'top_traders', 'all_accounts', or 'opaque' when the upstream exposes no breakdown. Per-venue specifics live in Exchange Notes. Cross-exchange comparisons require matching scope.",
    )
    timestamp: int = Field(
        ...,
        description="Bucket close time in unix milliseconds (end of `interval`).",
    )
