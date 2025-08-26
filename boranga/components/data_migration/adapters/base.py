import os
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ExtractionWarning:
    message: str
    row_hint: Any | None = None


@dataclass
class ExtractionResult:
    rows: list[dict[str, Any]]
    warnings: list[ExtractionWarning] = field(default_factory=list)


class SourceAdapter:
    source_key: str  # must match sources.Source value

    # Optional: domain name for logging
    domain: str | None = None

    def extract(self, path: str, **options) -> ExtractionResult:
        raise NotImplementedError

    # Optionally a quick probe (if you want auto-detect)
    def can_auto_detect(self) -> bool:
        # Only true if subclass overrides detect
        return self.__class__.detect is not SourceAdapter.detect

    def detect(self, path: str, **options) -> bool:
        """Return True if this adapter applies (implement only if needed)."""
        raise NotImplementedError

    def read_table(
        self, path: str, *, encoding: str = "utf-8"
    ) -> tuple[list[dict], list["ExtractionWarning"]]:
        """
        Read a flat table file into a list[dict] using pandas (assumed available).
        Returns (rows, warnings).
        """
        rows: list[dict] = []
        warnings: list[ExtractionWarning] = []
        try:
            import pandas as pd

            ext = os.path.splitext(path)[1].lower()
            if ext in (".xls", ".xlsx"):
                df = pd.read_excel(path, dtype=str)
            else:
                df = pd.read_csv(path, dtype=str, encoding=encoding)
            df = df.fillna("")
            rows = df.to_dict(orient="records")
        except Exception as e:
            warnings.append(ExtractionWarning(f"Failed reading file with pandas: {e}"))
        return rows, warnings
