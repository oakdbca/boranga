from __future__ import annotations

from django.db import transaction

from boranga.components.data_migration.adapters.occurrence import (  # shared canonical schema
    schema,
)
from boranga.components.data_migration.adapters.occurrence.tpfl import (
    OccurrenceTPFLAdapter,
)
from boranga.components.data_migration.adapters.sources import Source
from boranga.components.data_migration.registry import (
    BaseSheetImporter,
    ImportContext,
    TransformContext,
    register,
    run_pipeline,
)
from boranga.components.occurrence.models import Occurrence

SOURCE_ADAPTERS = {
    Source.TPFL.value: OccurrenceTPFLAdapter(),
    # Add new adapters here as they are implemented:
    # Source.TEC.value: OccurrenceTECAdapter(),
    # Source.TFAUNA.value: OccurrenceTFAUNAAdapter(),
}


@register
class OccurrenceImporter(BaseSheetImporter):
    slug = "occurrence_legacy"
    description = "Import occurrence data from legacy TEC / TFAUNA / TPFL sources"

    def add_arguments(self, parser):
        parser.add_argument(
            "--sources",
            nargs="+",
            choices=list(SOURCE_ADAPTERS.keys()),
            help="Subset of sources (default: all implemented)",
        )
        parser.add_argument(
            "--path-map",
            nargs="+",
            metavar="SRC=PATH",
            help="Per-source path overrides (e.g. TPFL=/tmp/tpfl.xlsx). If omitted, --path is reused.",
        )

    def _parse_path_map(self, pairs):
        out = {}
        if not pairs:
            return out
        for p in pairs:
            if "=" not in p:
                raise ValueError(f"Invalid path-map entry: {p}")
            k, v = p.split("=", 1)
            out[k] = v
        return out

    def run(self, path: str, ctx: ImportContext, **options):
        sources = options.get("sources") or list(SOURCE_ADAPTERS.keys())
        path_map = self._parse_path_map(options.get("path_map"))

        stats = ctx.stats.setdefault(self.slug, self.new_stats())
        all_rows: list[dict] = []
        warnings = []

        # 1. Extract
        for src in sources:
            adapter = SOURCE_ADAPTERS[src]
            src_path = path_map.get(src, path)
            result = adapter.extract(src_path, **options)
            for w in result.warnings:
                warnings.append(f"{src}: {w.message}")
            for r in result.rows:
                r["_source"] = src
            all_rows.extend(result.rows)

        # 2. Header / schema already normalised by adapters; apply transforms per column
        pipelines = {}
        for col, names in schema.COLUMN_PIPELINES.items():
            from boranga.components.data_migration.registry import (
                registry as transform_registry,
            )

            pipelines[col] = transform_registry.build_pipeline(names)

        processed = 0
        errors = 0
        created = 0
        updated = 0
        skipped = 0
        warn_count = 0

        # 3. Deduplicate (example simple key)
        seen_keys = set()
        deduped = []
        for row in all_rows:
            key = (row.get("legacy_id"), row.get("_source"))
            if key in seen_keys:
                continue
            seen_keys.add(key)
            deduped.append(row)

        # 4. Transform + persist
        for row in deduped:
            processed += 1
            tcx = TransformContext(row=row, model=None, user_id=ctx.user_id)
            issues = []
            transformed = {}
            has_error = False
            for col, pipeline in pipelines.items():
                raw_val = row.get(col)
                res = run_pipeline(pipeline, raw_val, tcx)
                transformed[col] = res.value
                for issue in res.issues:
                    issues.append((col, issue))
                    if issue.level == "error":
                        has_error = True
                        errors += 1
                    else:
                        warn_count += 1
            if has_error:
                skipped += 1
                continue

            # 5. Persist (pseudo-code; replace with real model logic)
            if ctx.dry_run:
                continue
            with transaction.atomic():
                obj, created_flag = Occurrence.objects.get_or_create(
                    legacy_source=row["_source"],
                    legacy_id=transformed["legacy_id"],
                    defaults={
                        "species_code": transformed["species_code"],
                        "observed_date": transformed["observed_date"],
                        "count": transformed["count"],
                        "location_name": transformed["location_name"],
                        "observer_name": transformed["observer_name"],
                        "notes": transformed["notes"],
                    },
                )
                created_flag = True  # placeholder
                if created_flag:
                    created += 1
                else:
                    # update fields if needed
                    updated += 1

        stats.update(
            processed=processed,
            created=created,
            updated=updated,
            skipped=skipped,
            errors=errors,
            warnings=warn_count,
        )
        return stats
