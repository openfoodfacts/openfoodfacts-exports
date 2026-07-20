import io
import logging
from pathlib import Path
from typing import Iterator

import orjson
from minio import Minio
from openfoodfacts import APIVersion
from openfoodfacts.redis import ProductUpdateEvent
from openfoodfacts.types import JSONType

from openfoodfacts_exports import settings
from openfoodfacts_exports.exports.historical_events import (
    RevisionInfo,
    generate_events,
    write_events_jsonl_gz,
)
from openfoodfacts_exports.tasks.revisions import strip_product_from_user_ids

logger = logging.getLogger(__name__)

# Derived event rows are stored under this prefix in the revision bucket, next to the
# raw revision snapshots. The nightly export concatenates them into the public dump.
HISTORICAL_EVENTS_PREFIX = "historical_events"


def generate_events_path(api_version: APIVersion, barcode: str, rev_id: int) -> str:
    """Generate the S3 path of the historical events derived from a revision.

    Args:
        api_version: The API version used when the revision was stored.
        barcode: The barcode of the product.
        rev_id: The revision ID.

    Returns:
        The S3 object path of the derived event rows.
    """
    return f"{HISTORICAL_EVENTS_PREFIX}/{api_version.value}/{barcode}/{rev_id}.jsonl"


def store_revision_events(
    minio_client: Minio,
    api_version: APIVersion,
    event: ProductUpdateEvent,
    previous_product: JSONType | None,
    current_product: JSONType,
) -> None:
    """Generate and store the historical events for a single product update.

    The event rows are computed from the update diff and the previous/current
    revisions, then uploaded to S3 as JSONL, next to the revision snapshots. Both
    revisions are already stripped of user IDs, so no contributor data is stored.

    Args:
        minio_client: The Minio client.
        api_version: The API version used when the revision was stored.
        event: The product update event from the Redis stream.
        previous_product: The previous revision (``rev - 1``), or ``None``.
        current_product: The current revision, as uploaded by the sync.
    """
    rev_id = current_product.get("rev")
    if rev_id is None:
        logger.warning(
            "Product %s has no revision number, skipping historical events", event.code
        )
        return

    revision = RevisionInfo(
        code=event.code,
        rev_id=rev_id,
        timestamp=int(event.timestamp.timestamp()),
        product_type=event.product_type,
        comment=event.comment,
    )
    events = generate_events(revision, event.diffs, previous_product, current_product)
    if not events:
        logger.debug(
            "No historical event for product %s revision %s", event.code, rev_id
        )
        return

    events_bytes = b"".join(orjson.dumps(row) + b"\n" for row in events)
    object_name = generate_events_path(api_version, event.code, rev_id)
    logger.info(
        "Storing %d historical event(s) for product %s at %s",
        len(events),
        event.code,
        object_name,
    )
    minio_client.put_object(
        bucket_name=settings.AWS_S3_REVISION_BUCKET,
        object_name=object_name,
        data=io.BytesIO(events_bytes),
        length=len(events_bytes),
        content_type="application/x-ndjson",
    )


def backfill_historical_events_to_file(products_dir: Path, output_path: Path) -> None:
    """Generate the whole historical events backlog into a gzipped JSONL file.

    This is the one-off backfill: it rebuilds the historical events for every product
    revision from a bundled ``products/`` directory, so the dump is not limited to
    updates captured live from the Redis stream.

    Args:
        products_dir: The root of the bundled ``products/`` directory.
        output_path: The destination ``.jsonl.gz`` file.
    """
    logger.info("Backfilling historical events from %s", products_dir)
    write_events_jsonl_gz(iter_backfill_events(products_dir), output_path)
    logger.info("Backfill written to %s", output_path)


def iter_backfill_events(products_dir: Path) -> Iterator[JSONType]:
    """Generate historical events for the whole backlog from a ``products/`` bundle.

    The bundle is the on-disk Product Opener ``products/`` directory, in JSON format
    (after the STO to JSON migration). Each product folder holds one snapshot file per
    revision (``{rev}.json``) and a ``changes.json`` file with one record per revision
    (``rev``, ``t``, ``comment``, ``diffs``, ...).

    For each revision, the changed fields come from the ``changes.json`` diff, and the
    previous/current values are read from the ``{rev - 1}.json`` and ``{rev}.json``
    snapshots. Both snapshots are stripped of user IDs, so no contributor data is
    emitted. This is fully offline: no S3, Redis or API access.

    Args:
        products_dir: The root of the bundled ``products/`` directory.

    Yields:
        One event row per changed field, across every product and revision.
    """
    for changes_path in sorted(products_dir.rglob("changes.json")):
        product_dir = changes_path.parent
        changes = _read_json(changes_path)
        if not isinstance(changes, list):
            logger.warning("Unexpected changes file, skipping: %s", changes_path)
            continue
        for record in sorted(changes, key=lambda item: int(item.get("rev", 0))):
            try:
                yield from _generate_revision_events(product_dir, record)
            except Exception:
                logger.exception(
                    "Failed to process revision %s in %s",
                    record.get("rev"),
                    product_dir,
                )


def _generate_revision_events(product_dir: Path, record: JSONType) -> list[JSONType]:
    """Generate the event rows for a single revision of the backlog."""
    rev = record.get("rev")
    if rev is None:
        logger.warning("Change record without a revision number in %s", product_dir)
        return []

    current_product = _read_snapshot(product_dir, rev)
    if current_product is None:
        logger.warning("Missing snapshot for revision %s in %s", rev, product_dir)
        return []

    # The previous snapshot may be absent when the product history is truncated
    # (the oldest revisions are not always kept on disk).
    previous_product = _read_snapshot(product_dir, int(rev) - 1)

    current_product = strip_product_from_user_ids(current_product)
    if previous_product is not None:
        previous_product = strip_product_from_user_ids(previous_product)

    revision = RevisionInfo(
        code=current_product["code"],
        rev_id=rev,
        timestamp=record["t"],
        product_type=current_product.get("product_type"),
        comment=record.get("comment"),
    )
    return generate_events(
        revision, record.get("diffs"), previous_product, current_product
    )


def _read_snapshot(product_dir: Path, rev: int | str) -> JSONType | None:
    """Read a revision snapshot from a product folder, or ``None`` if absent."""
    return _read_json(product_dir / f"{rev}.json")


def _read_json(path: Path) -> JSONType | None:
    """Read and parse a JSON file, or return ``None`` if it does not exist."""
    try:
        return orjson.loads(path.read_bytes())
    except FileNotFoundError:
        return None
