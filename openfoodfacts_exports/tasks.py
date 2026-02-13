import logging
from pathlib import Path

from openfoodfacts import Flavor, get_dataset
from openfoodfacts.dataset import DEFAULT_CACHE_DIR
from openfoodfacts.types import DatasetType
from openfoodfacts.utils import download_file, should_download_file

from openfoodfacts_exports.exports.csv.mobile import generate_push_mobile_app_dump
from openfoodfacts_exports.exports.parquet import PARQUET_DATASET_PATH, export_parquet
from openfoodfacts_exports.exports.parquet.price import PRICE_DATASET_PATH
from openfoodfacts_exports.exports.parquet.price import (
    export_parquet as export_price_parquet,
)
from openfoodfacts_exports.types import ExportFlavor
from openfoodfacts_exports.workers.queues import high_queue

logger = logging.getLogger(__name__)


def export_job(flavor: ExportFlavor) -> None:
    """Download the JSONL dataset and launch exports through new rq jobs."""
    logger.info("Start export job for flavor %s", flavor)

    if flavor == ExportFlavor.op:
        export_price_job()
        return

    flavor = Flavor[flavor]
    dataset_path = get_dataset(
        flavor=flavor, dataset_type=DatasetType.jsonl, download_newer=True
    )

    if flavor in (Flavor.off, Flavor.obf):
        export_parquet_job = high_queue.enqueue(
            export_parquet,
            dataset_path,
            PARQUET_DATASET_PATH[flavor],
            flavor,
            job_timeout="3h",
        )

        if flavor is Flavor.off:
            high_queue.enqueue(
                generate_push_mobile_app_dump,
                PARQUET_DATASET_PATH[flavor],
                depends_on=export_parquet_job,
                job_timeout="3h",
            )


def export_price_job() -> None:
    """Download the Open Prices dataset (made of 3 JSONL dumps) and launch the
    Parquet export through a new rq job."""
    logger.info("Start export job for price dataset")

    dataset_paths = {}
    for key, file_name in (
        ("price", "prices.jsonl.gz"),
        ("location", "locations.jsonl.gz"),
        ("proof", "proofs.jsonl.gz"),
    ):
        dataset_paths[key] = get_price_dataset(file_name, download_newer=True)

    logger.info("Enqueueing export job for price dataset")
    high_queue.enqueue(
        export_price_parquet,
        dataset_paths,
        PRICE_DATASET_PATH,
        job_timeout="3h",
    )


def get_price_dataset(
    filename: str,
    force_download: bool = False,
    download_newer: bool = False,
    cache_dir: Path | None = None,
) -> Path:
    """Download (and cache) Open Prices datasets.

    The dataset is downloaded the first time and subsequently cached in
    `~/.cache/openfoodfacts/datasets/prices`.

    Args:
        filename (str): The name of the file to download.
        force_download (bool, optional): if True, (re)download the dataset
        even if it was cached, defaults to False
        download_newer (bool, optional): if True, download the dataset if a
        more recent version is available (based on file Etag)
        cache_dir (Path, optional): the cache directory to use, defaults to
        ~/.cache/openfoodfacts/datasets. A subdirectory named `prices` will be
        created, and the dataset will be saved in it.

    Returns:
        The path to the downloaded dataset.
    """
    cache_dir = DEFAULT_CACHE_DIR if cache_dir is None else cache_dir
    price_dir = cache_dir / "prices"
    dataset_path = price_dir / filename
    url = f"https://prices.openfoodfacts.org/data/{filename}"
    price_dir.mkdir(parents=True, exist_ok=True)

    if not should_download_file(url, dataset_path, force_download, download_newer):
        return dataset_path

    logger.info("Downloading dataset, saving it in %s", dataset_path)
    download_file(url, dataset_path)
    return dataset_path
