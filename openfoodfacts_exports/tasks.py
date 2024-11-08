from openfoodfacts import get_dataset
from openfoodfacts.types import DatasetType, Flavor

from openfoodfacts_exports.exports.parquet import export_parquet
from openfoodfacts_exports.workers.queues import high_queue


def download_dataset_job(flavor: Flavor) -> None:
    dataset_path = get_dataset(
        flavor=flavor, dataset_type=DatasetType.jsonl, force_download=True
    )

    if flavor is Flavor.off:
        high_queue.enqueue(export_parquet, dataset_path)
