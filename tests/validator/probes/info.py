import time
from typing import List

from src.models import SymbolInfo
from tests.validator.results import ErrorType, ProbeResult

from tests.validator.probes.base import Probe, ProbeContext, fetch, validate_list


class InfoProbe(Probe):
    kind  = "rest"
    route = "info"
    name  = "info"


    def applies(self, ctx: ProbeContext) -> bool:
        return ctx.market_type is not None


    async def run(self, ctx: ProbeContext) -> List[ProbeResult]:
        endpoint = f"/{ctx.exchange}/{ctx.market_type}/info"
        started = time.time()
        status, data, err = await fetch(ctx.client, endpoint)

        if status != 200:
            return [self.http_failure(ctx, self.name, started, status, data, err)]

        if not isinstance(data, list):
            return [self.result(
                ctx, self.name, started,
                status="fail",
                error_type=ErrorType.SCHEMA,
                message="response is not a list",
                evidence={"got_type": type(data).__name__},
            )]
        if not data:
            return [self.result(
                ctx, self.name, started,
                status="warn",
                error_type=ErrorType.EMPTY,
                message="empty info list",
            )]

        shape_err = validate_list(data, SymbolInfo)
        if shape_err:
            msg, ev = shape_err
            return [self.result(
                ctx, self.name, started,
                status="fail",
                error_type=ErrorType.SCHEMA,
                message=msg,
                evidence=ev,
            )]

        for i in (0, len(data) - 1, len(data) // 2):
            d = data[i]
            if d["max_qty"] > 0 and d["min_qty"] > d["max_qty"]:
                return [self.result(
                    ctx, self.name, started,
                    status="fail",
                    error_type=ErrorType.LOGIC,
                    message=f"item[{i}] min_qty {d['min_qty']} > max_qty {d['max_qty']}",
                    evidence={"index": i, "min_qty": d["min_qty"], "max_qty": d["max_qty"]},
                )]
            if d["price_precision"] < 0 or d["quantity_precision"] < 0:
                return [self.result(
                    ctx, self.name, started,
                    status="fail",
                    error_type=ErrorType.LOGIC,
                    message=f"item[{i}] negative precision",
                    evidence={"index": i, "price_precision": d["price_precision"], "quantity_precision": d["quantity_precision"]},
                )]

        return [self.result(
            ctx, self.name, started,
            status="pass",
            error_type=ErrorType.OK,
            message=f"{len(data)} symbols, sample parsed OK",
            evidence={"count": len(data)},
        )]
