import json
import os
from datetime import datetime

from django.db import transaction

from boranga.components.species_and_communities.models import District, Species

# Define configuration paths
BACKUP_DIR = "private-media/legacy_data/TFAUNA/"
GROUP_FILTER = "fauna"
TARGET_DISTRICTS = ["Moora", "Central Wheatbelt", "Geraldton"]


def backup_and_remove_districts(dry_run=True):
    """Backs up and removes specified districts from fauna Species objects."""
    print(f"--- Starting Cleanup (Dry Run: {dry_run}) ---")

    districts = District.objects.filter(name__in=TARGET_DISTRICTS)
    district_map = {d.id: d.name for d in districts}

    if not districts.exists():
        print("Error: None of the target districts found in the database.")
        return

    species_qs = Species.objects.filter(group_type__name=GROUP_FILTER, districts__in=districts).distinct()

    if not species_qs.exists():
        print(f"No '{GROUP_FILTER}' species found matching target districts.")
        return

    # Structure the backup data
    backup_data = {
        "timestamp": datetime.now().isoformat(),
        "action": "remove_districts_from_fauna",
        "data": [],
    }

    total_removals = 0
    records_to_process = []

    # Map out what will be changed
    for species in species_qs:
        attached_districts = species.districts.filter(id__in=district_map.keys())
        district_ids = list(attached_districts.values_list("id", flat=True))
        district_names = [district_map[d_id] for d_id in district_ids]

        print(f"Species: {species.taxonomy.scientific_name} (ID: {species.id})")
        print(f"  -> Target districts found: {', '.join(district_names)}")

        total_removals += len(district_ids)

        backup_data["data"].append({"species_id": species.id, "district_ids": district_ids})
        records_to_process.append((species, attached_districts))

    print("\n--- Summary ---")
    print(f"Total Species affected: {len(records_to_process)}")
    print(f"Total relationships to remove: {total_removals}")

    if dry_run:
        print("\n[DRY RUN] No backup saved. No changes written to database.")
        return

    # Execute Live Run Changes
    try:
        os.makedirs(BACKUP_DIR, exist_ok=True)
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_filename = f"fauna_districts_rollback_{timestamp_str}.json"
        backup_path = os.path.join(BACKUP_DIR, backup_filename)

        with open(backup_path, "w") as f:
            json.dump(backup_data, f, indent=4)
        print(f"\n[SUCCESS] Backup state saved to AKS mount: {backup_path}")
    except Exception as e:
        print(f"\n[ERROR] Failed to write backup to {BACKUP_DIR}. Aborting run to protect data.")
        print(f"Details: {e}")
        return

    with transaction.atomic():
        for species, attached_districts in records_to_process:
            species.districts.remove(*attached_districts)

    print("[LIVE RUN] Changes successfully applied to the database.")


def revert_from_backup(backup_file_path):
    """Reads a JSON backup file and restores the District-to-Species relationships."""
    print(f"--- Starting Revert Process using: {backup_file_path} ---")

    if not os.path.exists(backup_file_path):
        print(f"Error: Backup file not found at {backup_file_path}")
        return

    with open(backup_file_path) as f:
        backup_content = json.load(f)

    backup_entries = backup_content.get("data", [])
    total_restored = 0

    with transaction.atomic():
        for entry in backup_entries:
            species_id = entry["species_id"]
            district_ids = entry["district_ids"]

            try:
                species = Species.objects.get(id=species_id)
                districts = District.objects.filter(id__in=district_ids)

                # Add relationships back (Django handles duplicates automatically)
                species.districts.add(*districts)
                print(f"Restored {districts.count()} districts to Species: {species.name} (ID: {species_id})")
                total_restored += districts.count()
            except Species.DoesNotExist:
                print(f"Warning: Species ID {species_id} no longer exists. Skipping.")

    print(f"\n[REVERT COMPLETE] Restored {total_restored} associations safely.")


# ==========================================
# EXECUTION CONTROLLER
# ==========================================
# Mode Options:
# 1. Clean Dry Run:              EXECUTION_MODE = 'dry_run'
# 2. Live Run + Create Backup:    EXECUTION_MODE = 'live_run'
# 3. Rollback from AKS File:     EXECUTION_MODE = 'revert'

EXECUTION_MODE = "live_run"
REVERT_FILE_TARGET = "private-media/legacy_data/TFAUNA/fauna_districts_rollback_XXXXXXXX_XXXXXX.json"

if EXECUTION_MODE == "dry_run":
    backup_and_remove_districts(dry_run=True)
elif EXECUTION_MODE == "live_run":
    backup_and_remove_districts(dry_run=False)
elif EXECUTION_MODE == "revert":
    revert_from_backup(REVERT_FILE_TARGET)
