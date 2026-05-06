import time
from typing import List

from tests.validator.results import ErrorType, ProbeResult

from tests.validator.probes.base import Probe, ProbeContext, fetch


class CrossRouteConsistencyProbe(Probe):
    kind  = "rest"
    route = "cross_route"
    name  = "cross_route"


    def applies(self, ctx: ProbeContext) -> bool:
        if not ctx.caps_slice or not ctx.symbol:
            return False
        has_ticker = ctx.caps_slice.get("ticker", {}).get("rest")
        has_book   = ctx.caps_slice.get("book_ticker", {}).get("rest")
        return bool(has_ticker and has_book)


    async def run(self, ctx: ProbeContext) -> List[ProbeResult]:
        out: List[ProbeResult] = []
        sym = ctx.symbol
        ex, m = ctx.exchange, ctx.market_type

        started = time.time()
        s_t, d_t, _ = await fetch(ctx.client, f"/{ex}/{m}/ticker/{sym}")
        s_b, d_b, _ = await fetch(ctx.client, f"/{ex}/{m}/book_ticker/{sym}")
        ended = time.time()

        if s_t == 200 and s_b == 200 and isinstance(d_t, dict) and isinstance(d_b, dict):
            tp = d_t.get("price")
            bid = d_b.get("bid_price")
            ask = d_b.get("ask_price")
            if tp and bid and ask and tp > 0:
                mid = (bid + ask) / 2
                drift = abs(tp - mid) / tp
                if drift > 0.005:
                    out.append(self.result(
                        ctx, "cross:ticker_vs_book", started,
                        status="warn",
                        error_type=ErrorType.LOGIC,
                        message=f"ticker/book drift {drift*100:.2f}% > 0.5%",
                        evidence={"ticker_price": tp, "book_mid": mid, "drift_pct": drift * 100},
                        ended=ended,
                    ))
                else:
                    out.append(self.result(
                        ctx, "cross:ticker_vs_book", started,
                        status="pass",
                        error_type=ErrorType.OK,
                        message=f"ticker/book drift {drift*100:.3f}%",
                        evidence={"ticker_price": tp, "book_mid": mid, "drift_pct": drift * 100},
                        ended=ended,
                    ))

        if m == "linear" and ctx.caps_slice.get("mark_price", {}).get("rest"):
            started = time.time()
            s_m, d_m, _ = await fetch(ctx.client, f"/{ex}/{m}/mark_price/{sym}")
            ended = time.time()
            if s_m == 200 and isinstance(d_m, dict) and isinstance(d_t, dict):
                mp = d_m.get("mark_price")
                tp = d_t.get("price")
                if mp and tp and tp > 0:
                    drift = abs(mp - tp) / tp
                    if drift > 0.01:
                        out.append(self.result(
                            ctx, "cross:mark_vs_ticker", started,
                            status="warn",
                            error_type=ErrorType.LOGIC,
                            message=f"mark/ticker drift {drift*100:.2f}% > 1%",
                            evidence={"mark_price": mp, "ticker_price": tp, "drift_pct": drift * 100},
                            ended=ended,
                        ))
                    else:
                        out.append(self.result(
                            ctx, "cross:mark_vs_ticker", started,
                            status="pass",
                            error_type=ErrorType.OK,
                            message=f"mark/ticker drift {drift*100:.3f}%",
                            evidence={"mark_price": mp, "ticker_price": tp, "drift_pct": drift * 100},
                            ended=ended,
                        ))

        return out
