# Report OCRs and OCCs that have duplicate geometry records at identical coordinates.
#
# A duplicate means two or more visible geometry rows for the same parent record
# whose geometry WKB is identical — a symptom of the missing reloadQueryLayer()
# bug where unsaved features had no OL ID and were re-created on every save.
#
# Run via:
#   ./manage.py shell < scripts/report_duplicate_geometries.py
#
# Optional env vars:
#   SHOW_DISCARDED=1   Also report duplicate pairs where one or both are discarded
#                      (visible=False).  Default: only visible duplicates.

import os
from collections import defaultdict

SHOW_DISCARDED = os.getenv("SHOW_DISCARDED", "0") == "1"

try:
    from boranga.components.occurrence.models import (
        OccurrenceGeometry,
        OccurrenceReportGeometry,
    )
except Exception as e:
    print(f"Import failed: {e}")
    raise

# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def find_duplicates(qs, parent_field, parent_model_number_attr):
    """
    Returns a list of dicts describing each parent record that has duplicate
    geometry coordinates.

    qs               – QuerySet of geometry objects (with 'geometry' GeoField)
    parent_field     – FK field name on the geometry model (e.g. 'occurrence_report')
    parent_model_number_attr – attribute on the parent to use as a human-readable
                               number (e.g. 'occurrence_report_number')
    """
    results = []

    # Group geometry records by parent id, then by WKB hex within that parent.
    # Two rows are duplicates if they share the same parent AND the same WKB.
    parent_to_geoms = defaultdict(list)
    for geom_obj in qs.select_related(parent_field):
        parent = getattr(geom_obj, parent_field)
        wkb = geom_obj.geometry.wkb.hex() if geom_obj.geometry else None
        parent_to_geoms[parent.id].append(
            {
                "parent": parent,
                "geom_id": geom_obj.id,
                "wkb": wkb,
                "visible": geom_obj.visible,
                "created_date": geom_obj.created_date,
            }
        )

    for parent_id, rows in parent_to_geoms.items():
        # Group rows within this parent by WKB
        wkb_groups = defaultdict(list)
        for row in rows:
            wkb_groups[row["wkb"]].append(row)

        # Keep only groups that have more than one row (duplicates)
        dup_groups = {wkb: grp for wkb, grp in wkb_groups.items() if len(grp) > 1}
        if not dup_groups:
            continue

        parent_obj = rows[0]["parent"]
        parent_number = getattr(parent_obj, parent_model_number_attr, str(parent_id))

        entry = {
            "parent_id": parent_id,
            "parent_number": parent_number,
            "duplicate_groups": [],
        }
        for wkb, grp in dup_groups.items():
            entry["duplicate_groups"].append(
                {
                    "coords": grp[0]["parent"].id,  # placeholder; replaced below
                    "geometry_ids": [r["geom_id"] for r in grp],
                    "visible_flags": [r["visible"] for r in grp],
                    "created_dates": [str(r["created_date"])[:19] for r in grp],
                    "geometry_wkb_prefix": (wkb[:20] + "...") if wkb else "None",
                }
            )
            # Replace coords placeholder with actual coords from the first geom object
            geom_row = next(r for r in rows if r["wkb"] == wkb)
            geom_instance = qs.get(id=geom_row["geom_id"])
            if geom_instance.geometry:
                coords = list(geom_instance.geometry.coords)
                entry["duplicate_groups"][-1]["coords"] = coords
            else:
                entry["duplicate_groups"][-1]["coords"] = None

        results.append(entry)

    return results


# ---------------------------------------------------------------------------
# OCR (OccurrenceReport) geometries
# ---------------------------------------------------------------------------

print("=" * 72)
print("DUPLICATE GEOMETRIES REPORT")
print("=" * 72)

visibility_filter = {} if SHOW_DISCARDED else {"visible": True}

ocr_qs = OccurrenceReportGeometry.objects.filter(**visibility_filter)
ocr_dups = find_duplicates(ocr_qs, "occurrence_report", "occurrence_report_number")

print("\n--- Occurrence Report (ORF) duplicate geometries ---")
print(f"    Scope: {'visible=True only' if not SHOW_DISCARDED else 'all (including discarded)'}")
print(f"    ORFs with duplicate geometry coordinates: {len(ocr_dups)}\n")

total_ocr_dup_pairs = 0
for entry in sorted(ocr_dups, key=lambda e: e["parent_number"]):
    print(f"  ORF {entry['parent_number']}  (id={entry['parent_id']})")
    for grp in entry["duplicate_groups"]:
        count = len(grp["geometry_ids"])
        total_ocr_dup_pairs += count - 1  # extras beyond the first
        visible_summary = ", ".join("✓" if v else "✗" for v in grp["visible_flags"])
        ids = ", ".join(str(i) for i in grp["geometry_ids"])
        dates = ", ".join(grp["created_dates"])
        print(f"    {count}× duplicate at {grp['coords']}")
        print(f"      geometry IDs   : {ids}")
        print(f"      visible flags  : {visible_summary}")
        print(f"      created dates  : {dates}")
    print()

print(f"  Total extra (redundant) ORF geometry rows: {total_ocr_dup_pairs}")

# ---------------------------------------------------------------------------
# OCC (Occurrence) geometries
# ---------------------------------------------------------------------------

occ_qs = OccurrenceGeometry.objects.filter(**visibility_filter)
occ_dups = find_duplicates(occ_qs, "occurrence", "occurrence_number")

print("\n--- Occurrence (OCC) duplicate geometries ---")
print(f"    Scope: {'visible=True only' if not SHOW_DISCARDED else 'all (including discarded)'}")
print(f"    OCCs with duplicate geometry coordinates: {len(occ_dups)}\n")

total_occ_dup_pairs = 0
for entry in sorted(occ_dups, key=lambda e: e["parent_number"]):
    print(f"  OCC {entry['parent_number']}  (id={entry['parent_id']})")
    for grp in entry["duplicate_groups"]:
        count = len(grp["geometry_ids"])
        total_occ_dup_pairs += count - 1
        visible_summary = ", ".join("✓" if v else "✗" for v in grp["visible_flags"])
        ids = ", ".join(str(i) for i in grp["geometry_ids"])
        dates = ", ".join(grp["created_dates"])
        print(f"    {count}× duplicate at {grp['coords']}")
        print(f"      geometry IDs   : {ids}")
        print(f"      visible flags  : {visible_summary}")
        print(f"      created dates  : {dates}")
    print()

print(f"  Total extra (redundant) OCC geometry rows: {total_occ_dup_pairs}")

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

print()
print("=" * 72)
print("SUMMARY")
print("=" * 72)
print(f"  ORFs affected : {len(ocr_dups)}")
print(f"  OCCs affected : {len(occ_dups)}")
print(f"  Extra ORF geometry rows (duplicates beyond first): {total_ocr_dup_pairs}")
print(f"  Extra OCC geometry rows (duplicates beyond first): {total_occ_dup_pairs}")
if len(ocr_dups) == 0 and len(occ_dups) == 0:
    print("\n  No duplicate geometries found.")
else:
    print()
    print("  To also check discarded geometries, run:")
    print("    SHOW_DISCARDED=1 ./manage.py shell < scripts/report_duplicate_geometries.py")
