import csv

from django.contrib.contenttypes.models import ContentType
from django.core.management.base import BaseCommand
from django.db import transaction

from boranga.components.main.models import LegacyValueMap

APP_LABEL = "boranga"  # all target models live in this app


class Command(BaseCommand):
    help = (
        "Import LegacyValueMap rows from CSV. Columns: legacy_value, "
        "target_model, target_object_id, canonical_name, active"
    )

    def add_arguments(self, parser):
        parser.add_argument("csvfile", type=str)
        parser.add_argument("--legacy-system", required=True)
        parser.add_argument("--list-name", required=True)
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument(
            "--update", action="store_true", help="update existing rows"
        )

    def _resolve_content_type(self, model_name: str | None) -> ContentType | None:
        if not model_name:
            return None
        model = model_name.strip().lower()
        try:
            return ContentType.objects.get(app_label=APP_LABEL, model=model)
        except ContentType.DoesNotExist:
            return None

    def handle(self, *args, **options):
        csvfile = options["csvfile"]
        legacy_system = options["legacy_system"]
        list_name = options["list_name"]
        dry_run = options["dry_run"]
        do_update = options["update"]

        rows = []
        with open(csvfile, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for r in reader:
                rows.append(r)

        created = 0
        updated = 0
        skipped = 0
        with transaction.atomic():
            for r in rows:
                legacy_value = (r.get("legacy_value") or r.get("legacy") or "").strip()
                if not legacy_value:
                    self.stderr.write("Skipping row with no legacy_value")
                    skipped += 1
                    continue

                canonical = (
                    r.get("canonical_name") or r.get("canonical") or ""
                ).strip()
                active = r.get("active", "true").strip().lower() not in (
                    "0",
                    "false",
                    "no",
                )
                target_id = (
                    r.get("target_object_id") or r.get("target_id") or ""
                ).strip() or None
                target_model = (
                    r.get("target_model") or r.get("model") or ""
                ).strip() or None

                ct = self._resolve_content_type(target_model)
                if target_model and ct is None:
                    self.stderr.write(
                        f"Unknown model '{target_model}' in app '{APP_LABEL}'; skipping row '{legacy_value}'"
                    )
                    skipped += 1
                    continue

                defaults = {
                    "canonical_name": canonical,
                    "active": active,
                    "target_object_id": int(target_id) if target_id else None,
                    "target_content_type": ct,
                }

                if dry_run:
                    self.stdout.write(
                        f"[DRY] would create/update: {legacy_value} -> {defaults}"
                    )
                    continue

                obj, created_flag = LegacyValueMap.objects.get_or_create(
                    legacy_system=legacy_system,
                    list_name=list_name,
                    legacy_value=legacy_value,
                    defaults=defaults,
                )
                if created_flag:
                    created += 1
                else:
                    if do_update:
                        for k, v in defaults.items():
                            setattr(obj, k, v)
                        obj.save()
                        updated += 1

        self.stdout.write(
            f"created={created} updated={updated} skipped={skipped} (dry_run={dry_run})"
        )
