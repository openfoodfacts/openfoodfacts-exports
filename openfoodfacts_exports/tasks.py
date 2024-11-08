import logging

from openfoodfacts import get_dataset
from openfoodfacts.types import DatasetType, Flavor

from openfoodfacts_exports.exports.csv import generate_push_mobile_app_dump
from openfoodfacts_exports.exports.parquet import PARQUET_DATASET_PATH, export_parquet
from openfoodfacts_exports.workers.queues import high_queue

logger = logging.getLogger(__name__)


def export_job(flavor: Flavor) -> None:
    """Download the JSONL dataset and launch exports through new rq jobs."""
    logger.info("Start export job for flavor %s", flavor)
    dataset_path = get_dataset(
        flavor=flavor, dataset_type=DatasetType.jsonl, download_newer=True
    )

    if flavor is Flavor.off:
        export_parquet_job = high_queue.enqueue(
            export_parquet,
            dataset_path,
            PARQUET_DATASET_PATH,
            job_timeout="1h",
        )
        high_queue.enqueue(
            generate_push_mobile_app_dump,
            PARQUET_DATASET_PATH,
            depends_on=export_parquet_job,
            job_timeout="1h",
        )
