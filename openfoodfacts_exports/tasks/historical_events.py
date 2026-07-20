import io
import logging

import orjson
from minio import Minio
from openfoodfacts import APIVersion
from openfoodfacts.redis import ProductUpdateEvent
from openfoodfacts.types import JSONType

from openfoodfacts_exports import settings
from openfoodfacts_exports.exports.historical_events import (
    RevisionInfo,
    generate_events,
)

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
