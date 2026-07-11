import time
from typing import List

from src.models import SymbolInfo
from tools.auditor.aggregator import ErrorType, ProbeResult

from tools.auditor.probes.base import Probe, ProbeContext, fetch, validate_one


class InfoProbe(Probe):
    kind  = "rest"
    route = "markets"
    name  = "markets:detail"


    def applies(self, ctx: ProbeContext) -> bool:
        return ctx.market_type is not None and ctx.symbol is not None


    async def run(self, ctx: ProbeContext) -> List[ProbeResult]:
        endpoint = f"/{ctx.exchange}/{ctx.market_type}/markets/{ctx.symbol}"
        started = time.time()
        status, data, err = await fetch(ctx.client, endpoint)

        if status != 200:
            return [self.http_failure(ctx, self.name, started, status, data, err)]

        if not isinstance(data, dict):
            return [self.result(
                ctx, self.name, started,
                status="fail",
                error_type=ErrorType.SCHEMA,
                message="response is not a dict",
                evidence={"got_type": type(data).__name__},
            )]

        shape_err = validate_one(data, SymbolInfo)
        if shape_err:
            msg, ev = shape_err
            return [self.result(
                ctx, self.name, started,
                status="fail",
                error_type=ErrorType.SCHEMA,
                message=msg,
                evidence=ev,
            )]

        min_qty = data.get("min_qty")
        max_qty = data.get("max_qty")
        if min_qty is not None and max_qty is not None and min_qty > max_qty:
            return [self.result(
                ctx, self.name, started,
                status="fail",
                error_type=ErrorType.LOGIC,
                message=f"min_qty {min_qty} > max_qty {max_qty}",
                evidence={"min_qty": min_qty, "max_qty": max_qty},
            )]
        if data["price_precision"] < 0 or data["quantity_precision"] < 0:
            return [self.result(
                ctx, self.name, started,
                status="fail",
                error_type=ErrorType.LOGIC,
                message="negative precision",
                evidence={"price_precision": data["price_precision"], "quantity_precision": data["quantity_precision"]},
            )]

        qty_unit = data.get("qty_unit")
        contract_size = data.get("contract_size")
        if ctx.market_type == "inverse":
            unit_ok = qty_unit == "contract"
            cs_ok = contract_size is not None and contract_size > 0
            rule = "inverse requires qty_unit='contract' and a positive contract_size"
        else:
            unit_ok = qty_unit == "base"
            cs_ok = contract_size is None
            rule = f"{ctx.market_type} requires qty_unit='base' and contract_size=null"
        if not unit_ok or not cs_ok:
            return [self.result(
                ctx, self.name, started,
                status="fail",
                error_type=ErrorType.SCHEMA,
                message=f"unit contract broken: qty_unit={qty_unit!r}, contract_size={contract_size}; {rule}",
                evidence={"qty_unit": qty_unit, "contract_size": contract_size, "market_type": ctx.market_type},
            )]

        r = self.result(
            ctx, self.name, started,
            status="pass",
            error_type=ErrorType.OK,
            message=f"symbol {data.get('symbol')} info parsed OK",
            evidence={"symbol": data.get("symbol")},
        )
        r.sample = [data]
        return [r]
