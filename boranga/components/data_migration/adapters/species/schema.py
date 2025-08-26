from __future__ import annotations

from dataclasses import dataclass

from boranga.components.data_migration.adapters.schema_base import Schema
from boranga.components.data_migration.mappings import build_legacy_map_transform
from boranga.components.data_migration.registry import choices_transform

# Example species schema for migrating legacy Species data.
# Adjust LEGACY_SOURCE_KEY, column headers and transforms to match your CSVs / registry.

LEGACY_SOURCE_KEY = "LEGACY_SPP"

# Legacy → target FK / lookup transforms (require LegacyValueMap data)
TAXONOMY_TRANSFORM = build_legacy_map_transform(
    LEGACY_SOURCE_KEY, "taxonomy", return_type="id"
)
FAUNA_GROUP_TRANSFORM = build_legacy_map_transform(
    LEGACY_SOURCE_KEY, "fauna_group", return_type="id"
)
FAUNA_SUBGROUP_TRANSFORM = build_legacy_map_transform(
    LEGACY_SOURCE_KEY, "fauna_sub_group", return_type="id"
)
GROUP_TYPE_TRANSFORM = build_legacy_map_transform(
    LEGACY_SOURCE_KEY, "group_type", return_type="id"
)

# Processing status choices mirror Species.PROCESSING_STATUS_CHOICES
PROCESSING_STATUS_CHOICES = ["draft", "discarded", "active", "historical"]
PROCESSING_STATUS_TRANSFORM = choices_transform(PROCESSING_STATUS_CHOICES)

COLUMN_MAP = {
    "Legacy Species ID": "migrated_from_id",
    "Species Number": "species_number",
    "Group Type": "group_type",
    "Taxonomy (scientific name)": "taxonomy",
    "Taxonomy ID": "taxonomy_id",
    "Submitter": "submitter",
    "Processing Status": "processing_status",
    "Lodgement Date": "lodgement_date",
    "Last Data Curation Date": "last_data_curation_date",
    "Conservation Plan Exists": "conservation_plan_exists",
    "Conservation Plan Reference": "conservation_plan_reference",
    "Fauna Group": "fauna_group",
    "Fauna Sub Group": "fauna_sub_group",
    "Comment": "comment",
    "Department File Numbers": "department_file_numbers",
}

# Minimal required canonical fields for migration
REQUIRED_COLUMNS = [
    "migrated_from_id",  # legacy identifier used to relate back to source
    "group_type",
    "processing_status",
]

PIPELINES = {
    # Identifiers / basics
    "migrated_from_id": ["strip", "required"],
    "species_number": ["strip", "blank_to_none"],  # often generated server-side
    "department_file_numbers": ["strip", "blank_to_none"],
    "comment": ["strip", "blank_to_none"],
    # Group / taxonomy / FK mappings
    # group_type_by_name is kept for compatibility with other schemas (implement in registry)
    "group_type": ["strip", "blank_to_none", "group_type_by_name", "required"],
    "taxonomy": ["strip", "blank_to_none", TAXONOMY_TRANSFORM],
    "taxonomy_id": ["strip", "blank_to_none", TAXONOMY_TRANSFORM],
    "fauna_group": ["strip", "blank_to_none", FAUNA_GROUP_TRANSFORM],
    "fauna_sub_group": ["strip", "blank_to_none", FAUNA_SUBGROUP_TRANSFORM],
    # User refs / submitter (ledger ids may be resolved later)
    "submitter": ["strip", "blank_to_none"],
    # Status / choices
    "processing_status": ["strip", "required", PROCESSING_STATUS_TRANSFORM],
    # Dates
    "lodgement_date": ["strip", "blank_to_none", "date_iso"],
    "last_data_curation_date": ["strip", "blank_to_none", "date_iso"],
    # Booleans
    "conservation_plan_exists": ["strip", "blank_to_none", "bool"],
}

SCHEMA = Schema(
    column_map=COLUMN_MAP,
    required=REQUIRED_COLUMNS,
    pipelines=PIPELINES,
    source_choices=None,
)

# Re-export convenience functions
normalise_header = SCHEMA.normalise_header
canonical_key = SCHEMA.canonical_key
required_missing = SCHEMA.required_missing
validate_headers = SCHEMA.validate_headers
map_raw_row = SCHEMA.map_raw_row
COLUMN_PIPELINES = SCHEMA.effective_pipelines()


@dataclass
class SpeciesRow:
    """
    Canonical (post-transform) species row used for persistence.
    Types are examples; adjust to match pipeline outputs (ids resolved, booleans/dates normalized).
    """

    migrated_from_id: str
    group_type: int  # FK id (GroupType) or canonical string depending on transform
    processing_status: str
    species_number: str | None = None
    taxonomy: int | None = None  # FK id (Taxonomy) after transform
    taxonomy_id: int | None = None
    submitter: int | None = None  # ledger EmailUser id (migrated / resolved later)
    lodgement_date: object | None = None
    last_data_curation_date: object | None = None
    conservation_plan_exists: bool | None = None
    conservation_plan_reference: str | None = None
    fauna_group: int | None = None
    fauna_sub_group: int | None = None
    comment: str | None = None
    department_file_numbers: str | None = None
