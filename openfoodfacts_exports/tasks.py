from openfoodfacts import get_dataset
from openfoodfacts.types import DatasetType, Flavor

from openfoodfacts_exports.exports.csv import (
    MOBILE_APP_DUMP_DATASET_PATH,
    generate_mobile_app_dump,
)
from openfoodfacts_exports.exports.parquet import PARQUET_DATASET_PATH, export_parquet
from openfoodfacts_exports.workers.queues import high_queue


def export_job(flavor: Flavor) -> None:
    """Download the JSONL dataset and launch exports through new rq jobs."""
    dataset_path = get_dataset(
        flavor=flavor, dataset_type=DatasetType.jsonl, force_download=True
    )

    if flavor is Flavor.off:
        export_parquet_job = high_queue.enqueue(
            export_parquet,
            dataset_path,
            PARQUET_DATASET_PATH,
            job_timeout="1h",
        )
        high_queue.enqueue(
            generate_mobile_app_dump,
            PARQUET_DATASET_PATH,
            MOBILE_APP_DUMP_DATASET_PATH,
            depends_on=export_parquet_job,
            job_timeout="1h",
        )
