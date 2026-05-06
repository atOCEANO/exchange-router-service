import time
from typing import Dict, List

from tests.validator.results import ErrorType, ProbeResult

from tests.validator.probes.base import Probe, ProbeContext, fetch


class SymbolResolutionProbe(Probe):
    kind  = "rest"
    route = "symbol_resolution"
    name  = "symbol_resolution"


    KNOWN_SPOT_SYMBOLS: Dict[str, str] = {
        "binance": "BTCUSDT",
        "bybit":   "BTCUSDT",
        "kucoin":  "BTCUSDT",
        "okx":     "BTCUSDT",
        "kraken":  "XBTUSDT",
    }


    def applies(self, ctx: ProbeContext) -> bool:
        return ctx.market_type == "spot" and ctx.exchange in self.KNOWN_SPOT_SYMBOLS


    async def run(self, ctx: ProbeContext) -> List[ProbeResult]:
        sym = self.KNOWN_SPOT_SYMBOLS[ctx.exchange]
        endpoint = f"/{ctx.exchange}/spot/ticker/{sym}"
        started = time.time()
        status, data, err = await fetch(ctx.client, endpoint)
        ended = time.time()

        if err is not None:
            return [self.result(
                ctx, self.name, started,
                status="fail",
                error_type=ErrorType.NETWORK,
                message=err,
                evidence={"endpoint": endpoint, "expected_symbol": sym},
                ended=ended,
            )]
        if status != 200:
            return [self.result(
                ctx, self.name, started,
                status="fail",
                error_type=ErrorType.HTTP,
                message=f"HTTP {status} on known spot symbol {sym}",
                evidence={"endpoint": endpoint, "status": status},
                ended=ended,
            )]
        if not isinstance(data, dict) or data.get("symbol") != sym:
            return [self.result(
                ctx, self.name, started,
                status="fail",
                error_type=ErrorType.LOGIC,
                message=f"response symbol mismatch: expected {sym}, got {data.get('symbol') if isinstance(data, dict) else 'n/a'}",
                evidence={"expected": sym, "got": data.get("symbol") if isinstance(data, dict) else None},
                ended=ended,
            )]

        return [self.result(
            ctx, self.name, started,
            status="pass",
            error_type=ErrorType.OK,
            message=f"{sym} resolves on spot",
            evidence={"symbol": sym},
            ended=ended,
        )]
