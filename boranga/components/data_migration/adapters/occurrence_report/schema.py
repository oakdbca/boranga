from __future__ import annotations

from dataclasses import dataclass

from boranga.components.data_migration.adapters.schema_base import Schema
from boranga.components.data_migration.mappings import build_legacy_map_transform
from boranga.components.data_migration.registry import choices_transform

# NOTE: Replace example headers / choices with real spreadsheet values as confirmed.

# Legacy → target FK / lookup transforms (require LegacyValueMap rows preloaded)
GROUP_TYPE_TRANSFORM = build_legacy_map_transform(
    "TPFL", "group_type", return_type="id"
)
SPECIES_TRANSFORM = build_legacy_map_transform("TPFL", "species", return_type="id")
COMMUNITY_TRANSFORM = build_legacy_map_transform("TPFL", "community", return_type="id")

# Choice enums (subset you intend to import)
PROCESSING_STATUS_CHOICES = ["with_assessor", "with_approver", "approved"]
CUSTOMER_STATUS_CHOICES = [
    "draft",
    "with_assessor",
    "with_approver",
    "approved",
    "declined",
    "discarded",
    "closed",
]

PROCESSING_STATUS_CHOICE_TRANSFORM = choices_transform(PROCESSING_STATUS_CHOICES)
CUSTOMER_STATUS_CHOICE_TRANSFORM = choices_transform(CUSTOMER_STATUS_CHOICES)

# Header → canonical key
COLUMN_MAP = {
    "Legacy OCR ID": "legacy_id",
    "Group Type": "group_type",
    "Species Code": "species_code",
    "Community Code": "community_code",
    "Processing Status": "processing_status",
    "Customer Status": "customer_status",
    "Observation Date": "observation_date",
    "Observation Time": "observation_time",
    "Assigned Officer Email": "assigned_officer_email",
    "Assigned Approver Email": "assigned_approver_email",
    "Proposed Decline": "proposed_decline_status",
    "Internal Application": "internal_application",
    "Site Description": "site",
    "Record Source": "record_source",
    "Comments": "comments",
    "OCR For OCC Number Hint": "ocr_for_occ_number",
    "OCR For OCC Name Hint": "ocr_for_occ_name",
    "Approver Comment": "approver_comment",
    "Assessor Data": "assessor_data",
}

REQUIRED_COLUMNS = [
    "legacy_id",
    "group_type",
    "processing_status",
    # Conditional: species_code OR community_code (handled after pipelines)
]

PIPELINES = {
    "legacy_id": ["strip", "required"],
    "group_type": ["strip", "blank_to_none", GROUP_TYPE_TRANSFORM, "required"],
    "species_code": ["strip", "blank_to_none", SPECIES_TRANSFORM],
    "community_code": ["strip", "blank_to_none", COMMUNITY_TRANSFORM],
    "processing_status": ["strip", "required", PROCESSING_STATUS_CHOICE_TRANSFORM],
    "customer_status": ["strip", "blank_to_none", CUSTOMER_STATUS_CHOICE_TRANSFORM],
    "observation_date": ["strip", "blank_to_none", "date_iso"],
    "observation_time": ["strip", "blank_to_none"],  # could map via legacy map if coded
    "assigned_officer_email": ["strip", "blank_to_none", "email_to_user_id"],
    "assigned_approver_email": ["strip", "blank_to_none", "email_to_user_id"],
    "proposed_decline_status": ["strip", "blank_to_none", "bool_normalise"],
    "internal_application": ["strip", "blank_to_none", "bool_normalise"],
    "site": ["strip", "blank_to_none"],
    "record_source": ["strip", "blank_to_none"],
    "comments": ["strip", "blank_to_none"],
    "ocr_for_occ_number": ["strip", "blank_to_none"],
    "ocr_for_occ_name": ["strip", "blank_to_none", "cap_length_50"],
    "approver_comment": ["strip", "blank_to_none"],
    "assessor_data": ["strip", "blank_to_none"],
}

SCHEMA = Schema(
    column_map=COLUMN_MAP,
    required=REQUIRED_COLUMNS,
    pipelines=PIPELINES,
    source_choices=None,
)

# Convenience exports
normalise_header = SCHEMA.normalise_header
canonical_key = SCHEMA.canonical_key
required_missing = SCHEMA.required_missing
validate_headers = SCHEMA.validate_headers
map_raw_row = SCHEMA.map_raw_row
COLUMN_PIPELINES = SCHEMA.effective_pipelines()


@dataclass
class OccurrenceReportRow:
    """
    Canonical (post-transform) occurrence report data for persistence.
    """

    legacy_id: str
    group_type: int | None
    species_code: int | None = None  # FK id (Species) after transform
    community_code: int | None = None  # FK id (Community) after transform
    processing_status: str | None = None
    customer_status: str | None = None
    observation_date: object | None = None  # date
    observation_time: str | None = None  # or int/id if mapped later
    assigned_officer_email: str | None = None
    assigned_approver_email: str | None = None
    proposed_decline_status: bool | None = None
    internal_application: bool | None = None
    site: str | None = None
    record_source: str | None = None
    comments: str | None = None
    ocr_for_occ_number: str | None = None
    ocr_for_occ_name: str | None = None
    approver_comment: str | None = None
    assessor_data: str | None = None
