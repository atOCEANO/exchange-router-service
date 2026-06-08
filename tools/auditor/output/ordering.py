MARKET_ORDER = {"spot": 0, "linear": 1, "inverse": 2, "ALL": 3}


ROUTE_DISPLAY_ORDER = ["markets", "info", "ticker", "book_ticker", "mark_price", "orderbook", "trades", "agg_trades", "candles", "funding_rate", "open_interest", "liquidations", "long_short_ratio"]


PERIOD_ORDER = ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"]


SUB_ORDER = ["recent", "limit=1", "limit=5", "before=1h_ago", "before=24h_ago", "start=future"]


META_ROUTES = {"capabilities_drift", "error_paths", "cross_route", "symbol_resolution", "status", "global", "capabilities"}


ROUTES_NO_SYMBOL = {"markets", "status", "version", "global", "capabilities"}


CAPS_KEY_ORDER = ["rest", "ws", "paginated", "max_limit", "max_depth", "depths", "intervals", "retention_ms"]
