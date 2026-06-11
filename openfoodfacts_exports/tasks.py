import gzip
import io
import logging
from pathlib import Path

import orjson
import requests
from minio import Minio
from openfoodfacts import APIVersion, Environment, Flavor, get_dataset
from openfoodfacts.api import API
from openfoodfacts.dataset import DEFAULT_CACHE_DIR
from openfoodfacts.images import (
    _generate_file_path,
    generate_image_url,
    generate_json_ocr_url,
)
from openfoodfacts.types import DatasetType, JSONType
from openfoodfacts.utils import (
    download_file,
    get_asset_from_url,
    should_download_file,
)

from openfoodfacts_exports import settings
from openfoodfacts_exports.exports.csv.mobile import generate_push_mobile_app_dump
from openfoodfacts_exports.exports.parquet import PARQUET_DATASET_PATH, export_parquet
from openfoodfacts_exports.exports.parquet.price import PRICE_DATASET_PATH
from openfoodfacts_exports.exports.parquet.price import (
    export_parquet as export_price_parquet,
)
from openfoodfacts_exports.types import ExportFlavor
from openfoodfacts_exports.utils import get_minio_client
from openfoodfacts_exports.workers.queues import high_queue

logger = logging.getLogger(__name__)


def export_job(export_flavor: ExportFlavor) -> None:
    """Download the JSONL dataset and launch exports through new rq jobs."""
    logger.info("Start export job for flavor %s", export_flavor)

    if export_flavor == ExportFlavor.op:
        export_price_job()
        return

    flavor = Flavor[export_flavor]
    dataset_path = get_dataset(
        flavor=flavor, dataset_type=DatasetType.jsonl, download_newer=True
    )

    if flavor in (Flavor.off, Flavor.obf):
        export_parquet_job = high_queue.enqueue(
            export_parquet,
            dataset_path,
            PARQUET_DATASET_PATH[flavor],
            export_flavor,
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


def upload_new_image_to_s3(
    image_id: str, barcode: str, flavor: Flavor, environment: Environment
) -> None:
    """Upload assets to S3 after a new image was uploaded to Product Opener.

    The following assets are uploaded:
    - Image (original and 400px version)
    - OCR result, gzipped

    Args:
        image_id (str): The ID of the image.
        barcode (str): The barcode of the product.
        flavor (Flavor): The flavor of the image.
        environment (Environment): The environment of the image.
    """
    if not settings.ENABLE_S3_PUSH:
        logger.debug("S3 push is disabled, skipping upload")
        return

    client = get_minio_client()
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": settings.USER_AGENT,
        }
    )
    for image_prefix in (image_id, f"{image_id}.400"):
        image_url = generate_image_url(
            barcode,
            image_prefix,
            flavor=flavor,
            environment=environment,
        )
        image_asset = get_asset_from_url(
            asset_url=image_url, error_raise=False, session=session
        )
        image_base_path = _generate_file_path(
            code=barcode, image_id=image_prefix, suffix=".jpg"
        )
        if image_asset.response is not None and image_asset.response.status_code == 200:
            s3_path = f"data{image_base_path}"
            image_fp = io.BytesIO(image_asset.response.content)
            logger.info(
                "Uploading image to %s/%s", settings.AWS_S3_IMAGE_BUCKET, s3_path
            )
            client.put_object(
                bucket_name=settings.AWS_S3_IMAGE_BUCKET,
                object_name=s3_path,
                data=image_fp,
                length=len(image_asset.response.content),
            )
    ocr_url = generate_json_ocr_url(
        barcode,
        image_id,
        flavor=flavor,
        environment=environment,
    )
    ocr_asset = get_asset_from_url(
        asset_url=ocr_url, error_raise=False, session=session
    )
    ocr_base_path = _generate_file_path(
        code=barcode, image_id=image_id, suffix=".json.gz"
    )
    if ocr_asset.response is not None and ocr_asset.response.status_code == 200:
        s3_path = f"data{ocr_base_path}"
        compressed_bytes = gzip.compress(ocr_asset.response.content)
        ocr_fp = io.BytesIO(compressed_bytes)
        logger.info(
            "Uploading OCR file to %s/%s", settings.AWS_S3_IMAGE_BUCKET, s3_path
        )
        client.put_object(
            bucket_name=settings.AWS_S3_IMAGE_BUCKET,
            object_name=s3_path,
            data=ocr_fp,
            length=len(compressed_bytes),
        )


def delete_image_from_s3(image_id: str, barcode: str) -> None:
    """Delete images and OCR results from S3, after the image deletion from Product
    Opener."""
    if not settings.ENABLE_S3_PUSH:
        logger.debug("S3 push is disabled, skipping deletion")
        return

    client = get_minio_client()
    for suffix in (".jpg", ".400.jpg", ".json.gz"):
        file_path = _generate_file_path(barcode, image_id, suffix=suffix)
        s3_path = f"data{file_path}"
        logger.info("Deleting file %s", s3_path)
        client.remove_object(
            bucket_name=settings.AWS_S3_IMAGE_BUCKET,
            object_name=s3_path,
        )


def sync_product_revision(
    barcode: str, environment: Environment, flavor: Flavor
) -> None:
    """Synchronize a product revision to S3.

    This function retrieves the product data from the Open Food Facts API and uploads it
    to S3.

    Args:
        barcode: The barcode of the product.
        environment: The environment to use.
        flavor: The flavor to use.
    """
    api_version = APIVersion.v2
    api = API(
        user_agent=settings.USER_AGENT,
        flavor=flavor,
        environment=environment,
        version=api_version,
    )
    client = get_minio_client()
    try:
        product = api.product.get(code=barcode)
    except Exception as e:
        logger.error("Failed to sync product revision for barcode %s: %s", barcode, e)
        return
    if product is None:
        # Product does not exist
        return
    else:
        # Product found
        upload_revision(
            minio_client=client,
            api_version=api_version,
            barcode=barcode,
            product=product,
            set_as_latest=True,
        )


def delete_product_from_s3(barcode: str) -> None:
    client = get_minio_client()
    remove_latest_revision(
        client,
        APIVersion.v2,
        barcode,
    )


def remove_latest_revision(minio_client: Minio, api_version: APIVersion, barcode: str):
    """Remove the latest revision for a product from S3.

    Args:
        minio_client: The Minio client.
        api_version: The API version we used when calling the Open Food Facts API.
        barcode: The barcode of the product.
    """
    revision_path = generate_revision_path(api_version, barcode, "latest")
    logger.info("Removing latest revision for barcode %s at %s", barcode, revision_path)
    minio_client.remove_object(
        bucket_name=settings.AWS_S3_REVISION_BUCKET,
        object_name=revision_path,
    )


def upload_revision(
    minio_client: Minio,
    api_version: APIVersion,
    barcode: str,
    product: JSONType,
    set_as_latest: bool = False,
) -> None:
    """Upload a product revision to S3.

    Args:
        minio_client: The Minio client.
        api_version: The API version we used when calling the Open Food Facts API.
        barcode: The barcode of the product.
        product: The product data.
        set_as_latest: Whether to upload a revision named "latest.json" alongside the
            regular revision with the same content.
    """
    rev = product["rev"]
    revision_path = generate_revision_path(api_version, barcode, rev)
    product_bytes = orjson.dumps(product)
    fp = io.BytesIO(product_bytes)
    fp.seek(0)
    logger.info(
        "Uploading revision %s for barcode %s at %s", rev, barcode, revision_path
    )
    minio_client.put_object(
        bucket_name=settings.AWS_S3_REVISION_BUCKET,
        object_name=revision_path,
        data=fp,
        length=len(product_bytes),
    )
    if set_as_latest:
        logger.info(
            "Setting revision %s as latest for barcode %s at %s",
            rev,
            barcode,
            revision_path,
        )
        fp.seek(0)
        latest_path = str(Path(revision_path).with_name("latest.json"))
        minio_client.put_object(
            bucket_name=settings.AWS_S3_REVISION_BUCKET,
            object_name=latest_path,
            data=fp,
            length=len(product_bytes),
        )


def generate_revision_path(
    api_version: APIVersion,
    barcode: str,
    rev_id: int | str,
) -> str:
    """Generate the revision path when uploading product revisions to S3.

    Args:
        api_version: The API version we used when calling the Open Food Facts API.
        barcode: The barcode of the product.
        rev_id: The revision ID.

    Returns:
        The revision path.
    """
    return f"{api_version.value}/{barcode}/{rev_id}.json"
