import io
import logging
from pathlib import Path

import orjson
from minio import Minio
from openfoodfacts import APIVersion, Environment, Flavor
from openfoodfacts.api import API
from openfoodfacts.types import JSONType

from openfoodfacts_exports import settings
from openfoodfacts_exports.utils import get_minio_client

logger = logging.getLogger(__name__)


def strip_product_from_user_ids(product: JSONType) -> JSONType:
    """Strip the product from any user ID, so that we respect the right of the user to
    be forgotten. This function is intended to be used when saving historical data, so
    that we don't have to modify these files when a user ask for account deletion."""
    product = product.copy()
    for field_name in list(
        k
        for k in product.keys()
        if k
        in {
            "checkers_tags",
            "correctors_tags",
            "creator",
            "editors_tags",
            "informers_tags",
            "last_checker",
            "last_editor",
            "last_modified_by",
            "photographers_tags",
        }
    ):
        product.pop(field_name)

    images = product.get("images", {})

    if "uploaded" in images:
        # New images schema, with `uploaded` and `selected` fields
        uploaded = images["uploaded"]
        for image_key, image_data in uploaded.items():
            if "uploader" in image_data:
                image_data = image_data.copy()
                image_data.pop("uploader")
                uploaded[image_key] = image_data
    else:
        # Legacy image schema, with image ID as keys
        for image_key, image_data in images.items():
            if "uploader" in image_data:
                image_data = image_data.copy()
                image_data.pop("uploader")
                images[image_key] = image_data
    return product


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
        product = strip_product_from_user_ids(product)
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
        content_type="application/json",
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
            content_type="application/json",
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
