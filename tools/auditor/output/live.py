from typing import Dict, List, Optional

from rich.box import SIMPLE_HEAD
from rich.console import Console, Group
from rich.live import Live
from rich.progress import (
    BarColumn, MofNCompleteColumn, Progress, SpinnerColumn,
    TextColumn, TimeElapsedColumn, TimeRemainingColumn,
)
from rich.table import Column, Table

from tools.auditor.aggregator import Aggregator, ProbeResult
from tools.auditor.config import API_URL
from tools.auditor.output._urlfmt import compact_evidence, extract_params, synth_url


EXCHANGE_COLOR: Dict[str, str] = {
    "binance": "yellow",
    "bybit":   "magenta",
    "kraken":  "purple",
    "kucoin":  "green",
    "okx":     "cyan",
}


STATUS_COLOR: Dict[str, str] = {
    "pass": "green",
    "warn": "yellow",
    "fail": "red",
    "skip": "dim",
}


LABEL_WIDTH = 22


def _sort_key(r: ProbeResult) -> tuple:
    return (r.exchange, r.market_type, r.route, r.kind, r.probe)


def _build_columns():
    return (
        TextColumn("  "),
        SpinnerColumn(),
        TextColumn(
            "{task.fields[label]}",
            table_column=Column(min_width=LABEL_WIDTH, max_width=LABEL_WIDTH, no_wrap=True),
        ),
        TextColumn("[green]✓ {task.fields[passed]:>3}[/green]"),
        TextColumn("[yellow]⚠ {task.fields[warned]:>3}[/yellow]"),
        TextColumn("[red]✗ {task.fields[failed]:>3}[/red]"),
        TextColumn("[dim]• {task.fields[skipped]:>3}[/dim]"),
        TextColumn("[dim]│[/dim]"),
        BarColumn(bar_width=None),
        MofNCompleteColumn(),
        TextColumn("[dim]│[/dim]"),
        TimeElapsedColumn(),
        TextColumn("[dim]eta[/dim]"),
        TimeRemainingColumn(),
        TextColumn("  "),
    )


class LiveReporter:
    def __init__(self, console: Console, clear_screen: bool = True):
        self.console               = console
        self.clear_screen          = clear_screen
        self.results               : List[ProbeResult] = []
        self.global_totals         = {"pass": 0, "warn": 0, "fail": 0, "skip": 0}
        self.per_exchange_totals   : Dict[str, Dict[str, int]] = {}
        self.exchange_progress     : Optional[Progress] = None
        self.global_progress       : Optional[Progress] = None
        self.live                  : Optional[Live] = None
        self.global_task           = None
        self.exchange_tasks        : Dict[str, int] = {}


    def __enter__(self) -> "LiveReporter":
        if self.clear_screen:
            self.console.clear()

        self.exchange_progress = Progress(
            *_build_columns(),
            console=self.console,
            transient=False,
            expand=True,
        )
        self.global_progress = Progress(
            *_build_columns(),
            console=self.console,
            transient=False,
            expand=True,
        )

        self.live = Live(
            Group(self.exchange_progress, self.global_progress),
            console=self.console,
            refresh_per_second=10,
            transient=False,
        )
        self.live.__enter__()

        self.global_task = self.global_progress.add_task(
            "global",
            total=0,
            label="[bold white]total[/]",
            passed=0, warned=0, failed=0, skipped=0,
        )
        return self


    def __exit__(self, exc_type, exc, tb) -> None:
        if self.live is not None:
            self.live.__exit__(exc_type, exc, tb)


    def register_exchange(self, exchange: str, expected: int) -> None:
        if self.exchange_progress is None or self.global_progress is None or self.global_task is None:
            return
        if exchange in self.exchange_tasks or expected <= 0:
            if expected <= 0:
                self.per_exchange_totals.setdefault(exchange, {"pass": 0, "warn": 0, "fail": 0, "skip": 0})
            return

        color = EXCHANGE_COLOR.get(exchange, "white")
        task_id = self.exchange_progress.add_task(
            f"audit-{exchange}",
            total=expected,
            label=f"[bold {color}]auditing {exchange}[/]",
            passed=0, warned=0, failed=0, skipped=0,
        )
        self.exchange_tasks[exchange] = task_id
        self.per_exchange_totals[exchange] = {"pass": 0, "warn": 0, "fail": 0, "skip": 0}

        current_total = self.global_progress.tasks[self.global_task].total or 0
        self.global_progress.update(self.global_task, total=current_total + expected)


    def on_probe_done(self, exchange: str) -> None:
        if self.exchange_progress is None or self.global_progress is None:
            return
        if exchange in self.exchange_tasks:
            self.exchange_progress.advance(self.exchange_tasks[exchange], 1)
        if self.global_task is not None:
            self.global_progress.advance(self.global_task, 1)


    def on_skip_market(self, exchange: str, count: int) -> None:
        if self.exchange_progress is None or self.global_progress is None or count <= 0:
            return
        if exchange in self.exchange_tasks:
            task = self.exchange_tasks[exchange]
            cur = self.exchange_progress.tasks[task].total or 0
            self.exchange_progress.update(task, total=max(0, cur - count))
        if self.global_task is not None:
            cur_g = self.global_progress.tasks[self.global_task].total or 0
            self.global_progress.update(self.global_task, total=max(0, cur_g - count))


    def on_result(self, r: ProbeResult) -> None:
        self.results.append(r)

        if r.exchange in self.per_exchange_totals:
            self.per_exchange_totals[r.exchange][r.status] = self.per_exchange_totals[r.exchange].get(r.status, 0) + 1

        self.global_totals[r.status] = self.global_totals.get(r.status, 0) + 1

        if self.exchange_progress is None or self.global_progress is None:
            return

        if r.exchange in self.exchange_tasks:
            tots = self.per_exchange_totals[r.exchange]
            self.exchange_progress.update(
                self.exchange_tasks[r.exchange],
                passed  = tots["pass"],
                warned  = tots["warn"],
                failed  = tots["fail"],
                skipped = tots["skip"],
            )

        if self.global_task is not None:
            self.global_progress.update(
                self.global_task,
                passed  = self.global_totals["pass"],
                warned  = self.global_totals["warn"],
                failed  = self.global_totals["fail"],
                skipped = self.global_totals["skip"],
            )


def render_failure_summary(console: Console, results: List[ProbeResult]) -> None:
    failures = [r for r in results if r.status == "fail"]
    if not failures:
        return
    table = Table(
        title        = f"[bold red]Failures[/bold red] ({len(failures)})",
        box          = SIMPLE_HEAD,
        show_lines   = False,
        header_style = "bold",
        title_justify= "left",
        pad_edge     = False,
    )
    table.add_column("Exchange", no_wrap=True)
    table.add_column("Market",   no_wrap=True)
    table.add_column("Probe",    no_wrap=True)
    table.add_column("Type",     no_wrap=True)
    table.add_column("URL",      overflow="fold")
    table.add_column("Evidence", overflow="fold")
    table.add_column("Message",  overflow="fold")
    for r in sorted(failures, key=_sort_key):
        color   = EXCHANGE_COLOR.get(r.exchange, "white")
        params  = extract_params(r, None)
        url     = synth_url(r, params, API_URL)
        ev_text = compact_evidence(r.evidence or {})
        table.add_row(
            f"[{color}]{r.exchange}[/]",
            r.market_type,
            f"{r.route} / {r.probe}",
            r.error_type.value,
            url,
            ev_text,
            r.message,
        )
    console.print()
    console.print(table)


def render_exchange_summary(console: Console, aggregator: Aggregator) -> None:
    by_ex = aggregator.by_exchange()
    rows  = [(ex, counts) for ex, counts in by_ex.items() if ex != "GLOBAL"]
    if not rows:
        return

    rows.sort(key=lambda x: x[0])

    table = Table(
        title        = "[bold]Per-exchange results[/bold]",
        box          = SIMPLE_HEAD,
        show_lines   = False,
        header_style = "bold",
        title_justify= "left",
        pad_edge     = False,
    )
    table.add_column("exchange", no_wrap=True)
    table.add_column("pass",     justify="right", style=STATUS_COLOR["pass"])
    table.add_column("warn",     justify="right", style=STATUS_COLOR["warn"])
    table.add_column("fail",     justify="right", style=STATUS_COLOR["fail"])
    table.add_column("skip",     justify="right", style=STATUS_COLOR["skip"])

    totals = {"pass": 0, "warn": 0, "fail": 0, "skip": 0}
    for ex, counts in rows:
        color = EXCHANGE_COLOR.get(ex, "white")
        table.add_row(
            f"[bold {color}]{ex}[/]",
            str(counts.get("pass", 0)),
            str(counts.get("warn", 0)),
            str(counts.get("fail", 0)),
            str(counts.get("skip", 0)),
        )
        for k in totals:
            totals[k] += counts.get(k, 0)

    table.add_section()
    table.add_row(
        "[bold]total[/]",
        f"[bold]{totals['pass']}[/]",
        f"[bold]{totals['warn']}[/]",
        f"[bold]{totals['fail']}[/]",
        f"[bold]{totals['skip']}[/]",
    )

    console.print()
    console.print(table)
