"""Generate historical product events from product revisions.

This module turns product revisions into a flat, self-sufficient stream of
field-level change events. The stream is meant to be published as a public
JSONL dump (`{PREFIX}_historical_events.jsonl.gz`) and analysed offline
(duckdb/parquet), to study how products evolve over time (recipe
reformulation, Nutri-Score evolution, ...).

Each output row describes a single field change at a given revision::

    {"id": "{code}_{rev}", "code": "...", "rev_id": 252, "timestamp": ...,
     "product_type": "food", "comment": "...", "field": "brands",
     "previous": "Nestlé", "current": "Nestlé,Nescafé", "action": "change"}

Two identifiers coexist on purpose:

- ``rev_id`` is the per-product revision number (unique within a product).
- ``id`` is ``{code}_{rev}``, a globally unique key.

Revision metadata (``timestamp``, ``product_type``, ``comment``) is
denormalised onto every row, so the dump can be consumed without any join.

This module is intentionally pure and offline: it does not read from S3, the
Redis stream or the API. Those integrations (live capture and backlog
backfill) are wired separately on top of these functions.
"""

import enum
import gzip
from pathlib import Path
from typing import Any, Iterable

import orjson
from openfoodfacts.types import JSONType
from pydantic import BaseModel


# Per-field change operations, as found in Product Opener revision diffs.
class ChangeAction(enum.StrEnum):
    ADD = "add"
    CHANGE = "change"
    DELETE = "delete"


class FieldChange(BaseModel):
    """A single field change extracted from a revision diff."""

    field: str
    action: ChangeAction


class RevisionInfo(BaseModel):
    """Metadata about a single product revision.

    These fields are denormalised onto every event row generated for the
    revision.
    """

    code: str
    rev_id: int
    timestamp: int
    product_type: str | None = None
    comment: str | None = None

    @property
    def id(self) -> str:
        """Globally unique revision key, in the form ``{code}_{rev_id}``."""
        return f"{self.code}_{self.rev_id}"


def flatten_diffs(diffs: JSONType | None) -> list[FieldChange]:
    """Flatten a Product Opener revision diff into a list of field changes.

    Product Opener groups changes by category (``fields``, ``nutriments``,
    ``packagings``, ...) and then by operation (``add`` / ``change`` /
    ``delete``)::

        {"fields": {"change": ["brands"], "add": ["labels"]},
         "nutriments": {"change": ["energy"]}}

    We deliberately drop the category split to keep the output flat, and
    return one :class:`FieldChange` per (field, action) pair. Duplicate pairs
    are collapsed, and unknown operations are ignored.

    Args:
        diffs: The revision diff, or ``None`` when unavailable.

    Returns:
        The flattened list of field changes, in a deterministic order.
    """
    if not diffs:
        return []

    changes: list[FieldChange] = []
    seen: set[tuple[str, str]] = set()
    for category_value in diffs.values():
        if not isinstance(category_value, dict):
            continue
        for action, fields in category_value.items():
            if action not in ChangeAction or not fields:
                continue
            for field in fields:
                key = (field, action)
                if key not in seen:
                    seen.add(key)
                    changes.append(FieldChange(field=field, action=action))
    return changes


def resolve_field_value(product: JSONType | None, field: str) -> Any:
    """Look up a (possibly dotted) field path in a product revision.

    Field names may be nested paths such as
    ``packaging.as_sold.100g.nutrients.energy.value``. The path is resolved by
    walking successive dictionary keys.

    Args:
        product: The product revision, or ``None`` when the revision does not
            exist (e.g. before creation).
        field: The field name or dotted path to resolve.

    Returns:
        The resolved value, or ``None`` if the product is ``None`` or the path
        is missing.
    """
    if product is None:
        return None

    value: Any = product
    for part in field.split("."):
        if isinstance(value, dict) and part in value:
            value = value[part]
        else:
            return None
    return value


def generate_events(
    revision: RevisionInfo,
    diffs: JSONType | None,
    previous_product: JSONType | None,
    current_product: JSONType | None,
) -> list[JSONType]:
    """Generate the historical event rows for a single revision.

    For each changed field, the previous value is read from the previous
    revision and the current value from the current revision. On an ``add``
    there is no previous value, and on a ``delete`` there is no current value,
    so the corresponding side is left as ``None``.

    Args:
        revision: Metadata about the current revision.
        diffs: The revision diff listing the changed fields.
        previous_product: The previous revision (``rev_id - 1``), or ``None``.
        current_product: The current revision (``rev_id``).

    Returns:
        One row per changed field, ready to be serialised as JSONL.
    """
    events: list[JSONType] = []
    for change in flatten_diffs(diffs):
        previous_value = (
            None
            if change.action == ChangeAction.ADD
            else resolve_field_value(previous_product, change.field)
        )
        current_value = (
            None
            if change.action == ChangeAction.DELETE
            else resolve_field_value(current_product, change.field)
        )
        events.append(
            {
                "id": revision.id,
                "code": revision.code,
                "rev_id": revision.rev_id,
                "timestamp": revision.timestamp,
                "product_type": revision.product_type,
                "comment": revision.comment,
                "field": change.field,
                "previous": previous_value,
                "current": current_value,
                "action": change.action,
            }
        )
    return events


def write_events_jsonl_gz(events: Iterable[JSONType], output_path: Path) -> None:
    """Write event rows to a gzipped JSONL file.

    Args:
        events: The event rows to write.
        output_path: The destination ``.jsonl.gz`` file.
    """
    with gzip.open(output_path, "wb") as fp:
        for event in events:
            fp.write(orjson.dumps(event))
            fp.write(b"\n")
