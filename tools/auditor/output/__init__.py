from datetime import datetime
from pathlib import Path
from typing import List

from tools.auditor.output.html import write_html
from tools.auditor.output.json import write_json
from tools.auditor.output.live import LiveReporter, render_exchange_summary, render_failure_summary


__all__ = [
    "LiveReporter",
    "make_run_folder",
    "render_exchange_summary",
    "render_failure_summary",
    "write_html",
    "write_json",
]


def make_run_folder(requested: List[str]) -> Path:
    ts     = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    suffix = ("_" + "+".join(sorted(requested))) if requested else "_all"
    folder = Path(__file__).resolve().parents[1] / "runs" / f"{ts}{suffix}"
    folder.mkdir(parents=True, exist_ok=True)
    return folder
