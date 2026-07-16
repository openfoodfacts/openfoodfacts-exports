import gzip
import io
import logging

import requests
from openfoodfacts import Environment, Flavor
from openfoodfacts.images import (
    _generate_file_path,
    generate_image_url,
    generate_json_ocr_url,
)
from openfoodfacts.utils import (
    get_asset_from_url,
)

from openfoodfacts_exports import settings
from openfoodfacts_exports.utils import get_minio_client

logger = logging.getLogger(__name__)


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
