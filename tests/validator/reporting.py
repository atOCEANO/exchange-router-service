import json
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from rich.box import SIMPLE_HEAD
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.table import Table

from tests.validator.config import API_URL, MAX_CONCURRENT_EXCHANGES
from tests.validator.results import Aggregator, ProbeResult


def _sort_key(r: ProbeResult) -> tuple:
    return (r.exchange, r.market_type, r.route, r.kind, r.probe)


class LiveReporter:
    def __init__(self, console: Console):
        self.console = console
        self.results: List[ProbeResult] = []
        self.totals = {"pass": 0, "warn": 0, "fail": 0, "skip": 0}
        self.progress: Optional[Progress] = None
        self.task_id = None


    def __enter__(self) -> "LiveReporter":
        self.progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold cyan]validating[/bold cyan]"),
            TextColumn("[green]pass {task.fields[passed]}[/green]"),
            TextColumn("[yellow]warn {task.fields[warned]}[/yellow]"),
            TextColumn("[red]fail {task.fields[failed]}[/red]"),
            TextColumn("[dim]skip {task.fields[skipped]}[/dim]"),
            TextColumn("[dim]|[/dim]"),
            TimeElapsedColumn(),
            TextColumn("[dim]|[/dim]"),
            TextColumn("{task.fields[current]}", style="white"),
            console=self.console,
            transient=False,
        )
        self.progress.__enter__()
        self.task_id = self.progress.add_task(
            "validate", total=None,
            passed=0, warned=0, failed=0, skipped=0,
            current="starting",
        )
        return self


    def __exit__(self, exc_type, exc, tb) -> None:
        if self.progress is not None:
            self.progress.update(self.task_id, current="done")
            self.progress.__exit__(exc_type, exc, tb)


    def on_result(self, r: ProbeResult) -> None:
        self.results.append(r)
        self.totals[r.status] = self.totals.get(r.status, 0) + 1
        if self.progress is not None and self.task_id is not None:
            label = f"{r.exchange}/{r.market_type}/{r.probe}"
            if len(label) > 60:
                label = label[:57] + "..."
            self.progress.update(
                self.task_id,
                advance=1,
                passed  = self.totals["pass"],
                warned  = self.totals["warn"],
                failed  = self.totals["fail"],
                skipped = self.totals["skip"],
                current = label,
            )


_STATUS_STYLE = {
    "pass": "bold green",
    "warn": "bold yellow",
    "fail": "bold red",
    "skip": "dim",
}


def render_failure_summary(console: Console, results: List[ProbeResult]) -> None:
    failures = [r for r in results if r.status == "fail"]
    if not failures:
        return
    table = Table(
        title       = f"[bold red]Failures[/bold red] ({len(failures)})",
        box         = SIMPLE_HEAD,
        show_lines  = False,
        header_style= "bold",
        title_justify="left",
        pad_edge    = False,
    )
    table.add_column("Exchange", no_wrap=True)
    table.add_column("Market",   no_wrap=True)
    table.add_column("Probe",    no_wrap=True)
    table.add_column("Type",     no_wrap=True)
    table.add_column("Message",  overflow="fold")
    for r in sorted(failures, key=_sort_key):
        table.add_row(
            r.exchange,
            r.market_type,
            f"{r.route} / {r.probe}",
            r.error_type.value,
            r.message,
        )
    console.print()
    console.print(table)


def write_json(path: Path, aggregator: Aggregator, wall_time_s: float, requested_exchanges: List[str]) -> None:
    summary = aggregator.summary()
    payload = {
        "schema_version":           1,
        "generated_at":             datetime.now().isoformat(timespec="seconds"),
        "api_url":                  API_URL,
        "max_concurrent_exchanges": MAX_CONCURRENT_EXCHANGES,
        "wall_time_seconds":        round(wall_time_s, 3),
        "requested_exchanges":      requested_exchanges,
        "summary":                  summary,
        "results":                  [r.to_dict() for r in aggregator.results],
    }
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def make_run_folder(requested: List[str]) -> Path:
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    suffix = ("_" + "+".join(sorted(requested))) if requested else "_all"
    folder = Path(__file__).resolve().parents[1] / "runs" / f"{ts}{suffix}"
    folder.mkdir(parents=True, exist_ok=True)
    return folder
