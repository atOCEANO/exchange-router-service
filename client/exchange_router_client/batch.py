from typing import Any, Dict, Iterator, List, Tuple


class BatchResult:

    def __init__(self,
                 ok: Dict[str, Any],
                 degraded: Dict[str, Tuple[Any, List[str]]],
                 failed: Dict[str, Exception],
                 requested: int):
        self.ok       = ok
        self.degraded = degraded
        self.failed   = failed
        self.requested = requested


    def data(self) -> Dict[str, Any]:
        merged = dict(self.ok)

        for symbol, (payload, _warnings) in self.degraded.items():
            merged[symbol] = payload

        return merged


    def items(self) -> Iterator[Tuple[str, Any]]:
        return iter(self.data().items())


    def __iter__(self) -> Iterator[str]:
        return iter(self.data())


    def __getitem__(self, symbol: str) -> Any:
        return self.data()[symbol]


    def __len__(self) -> int:
        return len(self.data())


    def __contains__(self, symbol: str) -> bool:
        return symbol in self.data()


    def summary(self) -> str:
        return f"{self.requested} requested: {len(self.ok)} ok, {len(self.degraded)} degraded, {len(self.failed)} failed"


    def report(self) -> str:
        lines = [self.summary()]

        if self.degraded:
            lines.append("  degraded")
            for symbol, (_payload, messages) in self.degraded.items():
                joined = "; ".join(messages) if messages else "degraded"
                lines.append(f"    {symbol}   {joined}")

        if self.failed:
            lines.append("  failed")
            for symbol, error in self.failed.items():
                lines.append(f"    {symbol}   {type(error).__name__}: {getattr(error, 'detail', str(error))}")

        return "\n".join(lines)


    def __repr__(self) -> str:
        return f"BatchResult({self.summary()})"
