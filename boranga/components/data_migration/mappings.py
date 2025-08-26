from __future__ import annotations

from typing import Any, Literal

from boranga.components.data_migration.registry import (
    TransformContext,
    TransformIssue,
    TransformResult,
    _result,
    registry,
)
from boranga.components.main.models import LegacyValueMap

# Cache: (legacy_system, list_name) -> dict[norm_legacy_value] = mapping dict
_CACHE: dict[tuple[str, str], dict[str, dict]] = {}


def _norm(s: Any) -> str:
    return str(s).strip().casefold()


def preload_map(legacy_system: str, list_name: str, active_only: bool = True):
    key = (legacy_system, list_name)
    if key in _CACHE:
        return
    qs = LegacyValueMap.objects.filter(
        legacy_system=legacy_system,
        list_name=list_name,
    )
    if active_only:
        qs = qs.filter(active=True)
    data: dict[str, dict] = {}
    for row in qs.select_related("target_content_type"):
        data[_norm(row.legacy_value)] = {
            "target_id": row.target_object_id,
            "content_type_id": row.target_content_type_id,
            "canonical": row.canonical_name,
            "raw": row.legacy_value,
        }
    _CACHE[key] = data


ReturnMode = Literal["id", "canonical", "both"]


def build_legacy_map_transform(
    legacy_system: str,
    list_name: str,
    *,
    required: bool = True,
    return_type: ReturnMode = "id",
):
    """
    Create (or return existing) transform that maps legacy enumerated values.
    return_type:
        id         -> returns target_object_id
        canonical  -> returns canonical_name (fallback to raw legacy value)
        both       -> returns tuple (target_id, canonical_name)
    """
    transform_name = (
        f"legacy_map_{legacy_system.lower()}_{list_name.lower()}_{return_type}"
    )
    if transform_name in registry._fns:
        return transform_name

    def fn(value, ctx: TransformContext):
        if value in (None, ""):
            if required:
                return TransformResult(
                    value=None,
                    issues=[TransformIssue("error", f"{list_name} required")],
                )
            return _result(None)
        preload_map(legacy_system, list_name)
        table = _CACHE[(legacy_system, list_name)]
        norm = _norm(value)
        if norm not in table:
            return TransformResult(
                value=None,
                issues=[
                    TransformIssue(
                        "error", f"Unmapped {legacy_system}.{list_name} value '{value}'"
                    )
                ],
            )
        entry = table[norm]
        canonical = entry["canonical"] or entry["raw"]
        if return_type == "id":
            return _result(entry["target_id"])
        if return_type == "canonical":
            return _result(canonical)
        if return_type == "both":
            return _result((entry["target_id"], canonical))
        # Fallback (should not happen with Literal typing)
        return _result(entry["target_id"])

    registry._fns[transform_name] = fn
    return transform_name
