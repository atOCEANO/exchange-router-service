import argparse
import asyncio
import logging
import os
import sys
import time

from rich.console import Console

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from tools.auditor import runner
from tools.auditor.aggregator import Aggregator
from tools.auditor.output import LiveReporter, make_run_folder, render_exchange_summary, render_failure_summary, write_html, write_json


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("websockets").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m tools.auditor",
        description="Audit exchange-router-service routes against a running instance.",
        epilog=(
            "Examples:\n"
            "  python -m tools.auditor                       all registered exchanges, default parallelism\n"
            "  python -m tools.auditor okx                   only OKX\n"
            "  python -m tools.auditor binance bybit         Binance and Bybit, both in parallel\n"
            "  python -m tools.auditor -p 1                  serial mode (one adapter at a time)\n"
            "  python -m tools.auditor --parallel 2 binance bybit kraken\n"
            "\n"
            "Env knobs (CLI flags override these):\n"
            "  API_URL                              default http://localhost:8040\n"
            "  TIMEOUT                              REST timeout seconds, default 60\n"
            "  WS_TEST_DURATION                     WS sustained-window seconds, default 180\n"
            "  MAX_CONCURRENT_EXCHANGES             default 5\n"
            "  MAX_CONCURRENT_REST_PER_EXCHANGE     default 4\n"
            "  MAX_CONCURRENT_WS_PER_EXCHANGE       default 32\n"
            "  MIN_REST_INTERVAL_MS                 default 250 (per-exchange spacing for REST probes; set 1000 for Kraken-safe runs)\n"
            "  LATENCY_WARN_MS                      default 5000\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("exchanges", nargs="*", metavar="EXCHANGE",
                        help="Exchange(s) to audit (binance, bybit, kraken, kucoin, okx). Omit for all.")
    parser.add_argument("-p", "--parallel", type=int, default=None, metavar="N",
                        help="Number of adapters to audit in parallel (overrides MAX_CONCURRENT_EXCHANGES). Use 1 for serial.")
    return parser.parse_args()


async def _amain(requested: list, parallel: int) -> int:
    console = Console(record=False)
    aggregator = Aggregator()

    started = time.time()
    with LiveReporter(console) as live:
        def on_result(r):
            aggregator.add(r)
            live.on_result(r)

        await runner.run(
            requested, on_result,
            on_register_exchange    = live.register_exchange,
            on_probe_done           = live.on_probe_done,
            on_skip_market          = live.on_skip_market,
            max_concurrent_exchanges= parallel,
        )
    wall_time = time.time() - started

    folder = make_run_folder(requested)
    write_json(folder / "results.json", aggregator, wall_time, requested)
    write_html(folder, aggregator, wall_time, requested)

    render_exchange_summary(console, aggregator)
    render_failure_summary(console, aggregator.results)

    totals = aggregator.totals()
    fails  = totals.get("fail", 0)
    warns  = totals.get("warn", 0)
    passes = totals.get("pass", 0)
    skips  = totals.get("skip", 0)
    total = fails + warns + passes + skips

    if fails:
        console.print(f"\n[bold red]CRITICAL:[/bold red] {fails} failures, {warns} warnings, {passes} passing ({total} probes, {wall_time:.1f}s)")
    elif warns:
        console.print(f"\n[bold yellow]OK with warnings:[/bold yellow] {warns} warnings, {passes} passing ({total} probes, {wall_time:.1f}s)")
    else:
        console.print(f"\n[bold green]SUCCESS:[/bold green] {passes} passing, {skips} skipped ({total} probes, {wall_time:.1f}s)")

    console.print(f"[dim]Saved run to {folder}[/dim]")
    return 1 if fails else 0


def main() -> None:
    args = parse_args()
    requested = sorted({ex.lower() for ex in args.exchanges})
    code = asyncio.run(_amain(requested, args.parallel))
    sys.exit(code)


if __name__ == "__main__":
    main()
