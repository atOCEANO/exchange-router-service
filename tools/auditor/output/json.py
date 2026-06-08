import json
from datetime import datetime
from pathlib import Path
from typing import List

from src.version import SCHEMA_VERSION
from tools.auditor.aggregator import Aggregator
from tools.auditor.config import API_URL, MAX_CONCURRENT_EXCHANGES


def write_json(path: Path, aggregator: Aggregator, wall_time_s: float, requested_exchanges: List[str]) -> None:
    summary = aggregator.summary()
    payload = {
        "schema_version":           SCHEMA_VERSION,
        "generated_at":             datetime.now().isoformat(timespec="seconds"),
        "api_url":                  API_URL,
        "max_concurrent_exchanges": MAX_CONCURRENT_EXCHANGES,
        "wall_time_seconds":        round(wall_time_s, 3),
        "requested_exchanges":      requested_exchanges,
        "summary":                  summary,
        "results":                  [r.to_dict() for r in aggregator.results],
    }
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
