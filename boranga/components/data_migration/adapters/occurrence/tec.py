from ..base import ExtractionResult, ExtractionWarning, SourceAdapter
from ..sources import Source
from . import schema


class OccurrenceTecAdapter(SourceAdapter):
    source_key = Source.TEC.value
    domain = "occurrence"

    def extract(self, path: str, **options) -> ExtractionResult:
        rows = []
        warnings: list[ExtractionWarning] = []

        raw_rows, read_warnings = self.read_table(path)
        warnings.extend(read_warnings)

        for raw in raw_rows:
            canonical = schema.map_raw_row(raw)
            canonical["group_type"] = (
                "community"  # TODO: Add any other source dependent constants here
            )
            rows.append(canonical)
        return ExtractionResult(rows=rows, warnings=warnings)
