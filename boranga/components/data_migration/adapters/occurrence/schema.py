from __future__ import annotations

from dataclasses import dataclass

from boranga.components.data_migration.adapters.schema_base import Schema
from boranga.components.data_migration.mappings import build_legacy_map_transform
from boranga.components.data_migration.registry import choices_transform

# NOTE: All values in this file are examples only at this point
# TODO: Replace with real data

# Legacy → target FK / lookup transforms (require LegacyValueMap data)
WILD_STATUS_TRANSFORM = build_legacy_map_transform(
    "TPFL", "wild_status", return_type="id"
)
SPECIES_CODE_TRANSFORM = build_legacy_map_transform("TPFL", "species", return_type="id")
COMMUNITY_CODE_TRANSFORM = build_legacy_map_transform(
    "TPFL", "community", return_type="id"
)

# Choice enums (Occurrence.REVIEW_STATUS_CHOICES / PROCESSING_STATUS_CHOICES):
REVIEW_STATUS_CHOICES = ["not_reviewed", "awaiting_amendments", "amended", "accepted"]
PROCESSING_STATUS_CHOICES = [
    "draft",
    "active",
    "split",
    "combine",
    "historical",
    "discarded",
]

REVIEW_STATUS_CHOICE_TRANSFORM = choices_transform(REVIEW_STATUS_CHOICES)
PROCESSING_STATUS_CHOICE_TRANSFORM = choices_transform(PROCESSING_STATUS_CHOICES)

# MultiSelect Occurrence Source (codes from OCCURRENCE_SOURCE_CHOICES)
OCCURRENCE_SOURCE_CODES = ["ocr", "non-ocr"]  # internal codes
OCCURRENCE_SOURCE_CHOICE_TRANSFORM = choices_transform(OCCURRENCE_SOURCE_CODES)
# You still need a multi-select splitter + per-item validator in the registry:
# - split_multiselect_occ_source (splits on ';' or ',' into list)
# - validate_occurrence_source_choices (iterates items, applying OCCURRENCE_SOURCE_CHOICE_TRANSFORM or equivalent)
# (Placeholders referenced below.)

COLUMN_MAP = {
    "Legacy Occurrence ID": "legacy_id",
    "Occurrence Name": "occurrence_name",
    "Group Type": "group_type",
    "Species Code": "species_code",
    "Community Code": "community_code",
    "Wild Status": "wild_status",
    "Occurrence Source": "occurrence_source",
    "Comment": "comment",
    "Review Status": "review_status",
    "Processing Status": "processing_status",
    "Review Due Date": "review_due_date",
    "Combined Into Occurrence ID": "combined_occurrence_legacy_id",
}

REQUIRED_COLUMNS = [
    "legacy_id",
    "group_type",
    # Either species_code or community_code should exist depending on group_type;
    # enforce species_code here for flora/fauna via business rule later.
    "occurrence_name",
    "processing_status",
]

PIPELINES = {
    # Identifiers / required basics
    "legacy_id": ["strip", "required"],
    "occurrence_name": ["strip", "blank_to_none", "cap_length_50"],
    "combined_occurrence_legacy_id": ["strip", "blank_to_none"],
    # FK / lookup mappings
    "group_type": ["strip", "blank_to_none", "group_type_by_name", "required"],
    "species_code": [
        "strip",
        "blank_to_none",
        SPECIES_CODE_TRANSFORM,
    ],  # conditional required later
    "community_code": ["strip", "blank_to_none", COMMUNITY_CODE_TRANSFORM],
    "wild_status": ["strip", "blank_to_none", WILD_STATUS_TRANSFORM],
    # MultiSelect (split, trim, validate each)
    "occurrence_source": [
        "strip",
        "blank_to_none",
        "split_multiselect_occ_source",  # implement (e.g. split on ; or ,)
        "validate_occurrence_source_choices",  # implement to validate each element
    ],
    # Simple text
    "comment": ["strip", "blank_to_none"],
    # Choices (enums)
    "review_status": ["strip", "blank_to_none", REVIEW_STATUS_CHOICE_TRANSFORM],
    "processing_status": ["strip", "required", PROCESSING_STATUS_CHOICE_TRANSFORM],
    # Dates
    "review_due_date": ["strip", "blank_to_none", "date_iso"],
}

SCHEMA = Schema(
    column_map=COLUMN_MAP,
    required=REQUIRED_COLUMNS,
    pipelines=PIPELINES,
    # Not using source_choices placeholder here; multiple legacy systems may share this schema
    source_choices=None,
)

# Re-export convenience functions (optional)
normalise_header = SCHEMA.normalise_header
canonical_key = SCHEMA.canonical_key
required_missing = SCHEMA.required_missing
validate_headers = SCHEMA.validate_headers
map_raw_row = SCHEMA.map_raw_row
COLUMN_PIPELINES = SCHEMA.effective_pipelines()


@dataclass
class OccurrenceRow:
    """
    Canonical (post-transform) occurrence data used for persistence,
    independent of which legacy system (TPFL / TEC / TFAUNA) supplied it.
    Field naming follows pipeline output (ids already resolved).
    """

    legacy_id: str
    occurrence_name: str | None
    group_type: int  # FK id (GroupType)
    species_code: int | None = (
        None  # FK id (Species) after transform (could rename to species_id later)
    )
    community_code: int | None = None  # FK id (Community) after transform
    wild_status: int | None = None  # FK id (WildStatus) after transform
    occurrence_source: list[str] | None = None
    comment: str | None = None
    review_status: str | None = None
    processing_status: str | None = None
    review_due_date: object | None = None  # date
    combined_occurrence_legacy_id: str | None = None
