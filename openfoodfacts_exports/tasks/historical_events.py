import gzip
import logging
import shutil
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Iterator

import orjson
from minio import Minio
from more_itertools import chunked

from openfoodfacts_exports import settings
from openfoodfacts_exports.exports.historical_events import HistoryEvent
from openfoodfacts_exports.tasks.revisions import get_history_events
from openfoodfacts_exports.utils import get_minio_client

logger = logging.getLogger(__name__)

# Public dump published to the dataset bucket, next to the other exports.
HISTORICAL_EVENTS_DUMP_FILENAME = "openfoodfacts_historical_events.jsonl.gz"
HISTORICAL_EVENTS_DUMP_PATH = settings.DATASET_DIR / HISTORICAL_EVENTS_DUMP_FILENAME

# The Minio client keeps at most 10 connections per host, so more threads would not
# download faster.
DOWNLOAD_WORKERS = 10
# Products are processed in batches, so that only a bounded number of history files
# is kept in memory.
DOWNLOAD_BATCH_SIZE = 1000


def publish_historical_events_dump() -> None:
    """Publish the public historical events dump to the dataset bucket.

    The `history.jsonl` files of all products are concatenated into a single gzipped
    JSONL file, which is then pushed to `s3://openfoodfacts-ds/` next to the other
    exports. This is the job run weekly by the scheduler.
    """
    minio_client = get_minio_client()
    generate_historical_events_dump(minio_client, HISTORICAL_EVENTS_DUMP_PATH)

    if settings.ENABLE_S3_PUSH:
        logger.info("Uploading historical events dump to S3")
        minio_client.fput_object(
            settings.AWS_S3_DATASET_BUCKET,
            HISTORICAL_EVENTS_DUMP_FILENAME,
            file_path=str(HISTORICAL_EVENTS_DUMP_PATH),
        )
        logger.info("Historical events dump uploaded to S3")
    else:
        logger.info("S3 push is disabled, skipping upload of historical events dump")


def generate_historical_events_dump(minio_client: Minio, output_path: Path) -> None:
    """Concatenate the `history.jsonl` files of all products into a gzipped JSONL
    file.

    History files are downloaded in parallel, and written in the order of the bucket
    listing. Products without a history file are skipped. The file is written to a
    temporary location first and moved into place, so consumers never see a partial
    dump.

    Args:
        minio_client: The Minio client.
        output_path: The destination `.jsonl.gz` file.
    """
    logger.info("Generating historical events dump...")
    product_count = 0
    event_count = 0
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir) / HISTORICAL_EVENTS_DUMP_FILENAME
        with (
            gzip.open(tmp_path, "wb") as f,
            ThreadPoolExecutor(max_workers=DOWNLOAD_WORKERS) as executor,
        ):
            for codes in chunked(iter_product_codes(minio_client), DOWNLOAD_BATCH_SIZE):
                for history_events in executor.map(
                    lambda code: fetch_product_history(code, minio_client), codes
                ):
                    if not history_events:
                        continue
                    product_count += 1
                    for event in history_events:
                        f.write(orjson.dumps(event.model_dump()) + b"\n")
                    event_count += len(history_events)
        shutil.move(tmp_path, output_path)

    logger.info(
        "Historical events dump generated: %d events from %d products",
        event_count,
        product_count,
    )


def iter_product_codes(minio_client: Minio) -> Iterator[str]:
    """Iterate over the codes of all products stored in the revision bucket.

    Args:
        minio_client: The Minio client.

    Yields:
        The barcode of each product that has a directory in the bucket.
    """
    objects = minio_client.list_objects(
        bucket_name=settings.AWS_S3_REVISION_BUCKET, prefix="json/"
    )
    for obj in objects:
        if obj.is_dir:
            yield Path(obj.object_name).name


def fetch_product_history(code: str, minio_client: Minio) -> list[HistoryEvent]:
    """Fetch the history events of a product from the revision bucket.

    A failure on a single product is logged and does not stop the dump generation.

    Args:
        code: The barcode of the product.
        minio_client: The Minio client.

    Returns:
        The history events of the product, or an empty list if the product has no
        history file or if it could not be read.
    """
    try:
        return get_history_events(code=code, minio_client=minio_client) or []
    except Exception:
        logger.exception("Exception caught while fetching history for product %s", code)
        return []
