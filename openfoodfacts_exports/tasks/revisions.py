import io
import logging
from pathlib import Path
from typing import Generator, Iterable

import orjson
import tqdm
from minio import Minio, S3Error
from openfoodfacts import APIVersion, Environment, Flavor
from openfoodfacts.api import API
from openfoodfacts.images import extract_barcode_from_path, split_barcode
from openfoodfacts.types import JSONType

from openfoodfacts_exports import settings
from openfoodfacts_exports.exports.historical_events import (
    HistoryEvent,
    RevisionInfo,
    generate_events,
)
from openfoodfacts_exports.utils import get_minio_client

logger = logging.getLogger(__name__)


def upload_revision_history(
    code: str,
    product_dir: Path,
    product_type: str,
    minio_client: Minio,
) -> Generator[HistoryEvent, None, None]:
    changes = product_dir / "changes.json"
    if not changes.exists():
        logger.info("Skipping revision history for %s: changes.json not found", code)
        return
    changes = orjson.loads(changes.read_text())
    changes_by_rev_id = {change["rev"]: change for change in changes}

    if not changes:
        logger.warning("changes.json is empty for %s", code)
        return

    history_events = get_history_events(code=code, minio_client=minio_client)
    last_rev_id = None
    if history_events:
        # History events are sorted by timestamp, so the last one is the most recent
        last_rev_id = history_events[-1].rev_id
    else:
        # history_events is None, it means history.jsonl was not found on the server
        history_events = []

    revision_filepaths = sorted(
        (p for p in product_dir.glob("*.json") if p.stem.isdigit()),
        reverse=True,
    )
    new_events: list[HistoryEvent] = []
    previous_product: JSONType | None = None
    for revision_filepath in revision_filepaths:
        rev_id = int(revision_filepath.stem)
        if last_rev_id is not None and rev_id <= last_rev_id:
            continue
        current_product = strip_product_from_user_ids(
            orjson.loads(revision_filepath.read_text())
        )
        change = changes_by_rev_id.get(rev_id)
        if not change:
            logger.warning("No change found for revision %s", rev_id)
            previous_product = current_product
            continue

        diffs = change.get("diffs", [])
        if not diffs:
            logger.warning("No diffs found for revision %s", rev_id)
            previous_product = current_product
            continue
        rev_info = RevisionInfo(
            code=code,
            rev_id=rev_id,
            timestamp=change.get("t", None),
            product_type=product_type,
            comment=change.get("comment", None),
        )
        new_events += generate_events(
            rev_info, diffs, previous_product, current_product
        )
        previous_product = current_product

    history_events += new_events
    history_events = sorted(history_events, key=lambda e: e.rev_id)
    if history_events:
        upload_history_file(
            events=[h.model_dump() for h in history_events],
            code=code,
            minio_client=minio_client,
        )

    yield from new_events


def get_history_events(code: str, minio_client: Minio) -> list[HistoryEvent] | None:
    """Get the all history events for a product from the Minio bucket.

    Args:
        code: The barcode of the product.
        minio_client: The Minio client to use for downloading.

    Returns:
        The list of events in the history.jsonl file, or None if the history.jsonl file
        does not exist.
    """
    revision_path = generate_revision_path("json", code, "history.jsonl")
    response = None
    try:
        response = minio_client.get_object(
            bucket_name=settings.AWS_S3_REVISION_BUCKET, object_name=revision_path
        )
        return [
            HistoryEvent.model_validate(orjson.loads(line))
            for line in response.data.splitlines()
        ]
    except S3Error as e:
        if e.response.status == 404:
            return None
        else:
            raise
    finally:
        if response:
            response.close()
            response.release_conn()


def upload_history_file(events: list[JSONType], code: str, minio_client: Minio) -> None:
    """Upload the history.jsonl file for a product to the Minio bucket.

    Args:
        events: The list of events to upload.
        code: The barcode of the product.
        minio_client: The Minio client to use for uploading.
    """
    revision_path = generate_revision_path("json", code, "history.jsonl")
    fp = io.BytesIO()
    file_length = 0
    with fp:
        for event in events:
            event_bytes = orjson.dumps(event) + b"\n"
            fp.write(event_bytes)
            file_length += len(event_bytes)
        fp.seek(0)
        logger.info("Uploading history.jsonl for barcode %s at %s", code, revision_path)
        minio_client.put_object(
            bucket_name=settings.AWS_S3_REVISION_BUCKET,
            object_name=revision_path,
            data=fp,
            length=file_length,
            content_type="application/json",
        )


def upload_all_revisions(
    product_type: str,
    root_dir: Path,
    upload_history: bool = True,
    overwrite: bool = False,
    only_codes: Iterable[str] | None = None,
):
    """Upload all revisions of all products, along with their history (if
    `upload_history` is `True`).

    This function iterates over all directories in the given `root_dir`,
    only selecting product directories (directories with only digits in their path)
    that contain revision files (as JSON).

    If `only_codes` is not `None`, product directories that are not in this iterable
    will be skipped.

    All revision files are then uploaded to S3. If `overwrite` is False, we first
    fetch the existing revision file from S3 and compare it with the local files,
    skipping the revision files that were already uploaded.

    If `upload_history` is `True`, we generate and upload to S3 a `history.jsonl`
    file that contains the full change history. We first fetch the `history.jsonl`
    file from S3 (if it exists), optionally add missing history events, and then upload
    the updated file.

    Args:
        product_type (str): The type of product to upload revisions for (ex: `food`,
            `beauty`,...)
        root_dir (Path): The root directory containing product directories, for example
            `/rpool/off-backups/podata-nvme/products/`
        upload_history (bool, optional): Whether to refresh and upload the history.jsonl
            file. Defaults to True.
        overwrite (bool, optional): Whether to overwrite existing revision files on S3,
            even if they already exist. Defaults to False.
        only_codes (Iterable[str] | None, optional): A list of product codes to upload
            revisions for. If `None`, we iterate over all directory to find product
            directory, otherwise we generate product directory paths from the codes.
            Defaults to `None`.
    Yields:
        HistoryEvent: The new history events that were uploaded, if any.
    """
    logger.info("Uploading all revisions from %s...", root_dir)
    minio_client = get_minio_client()
    if only_codes:
        dir_iterator = (root_dir / "/".join(split_barcode(code)) for code in only_codes)
    else:
        dir_iterator = root_dir.glob("**/*")

    for product_dir in tqdm.tqdm(dir_iterator, desc="product directory"):
        if (
            product_dir.is_dir()
            # Check that the directory name is a barcode (all digits)
            and str(product_dir.relative_to(root_dir)).replace("/", "").isdigit()
            # Check that the directory contains JSON files
            and any(True for _ in product_dir.glob("*.json"))
        ):
            # extract_barcode_from_path expects a file path, not a directory path
            # so we add a fake file path to get the barcode
            code = extract_barcode_from_path(
                str(product_dir.relative_to(root_dir) / "f")
            )
            if code is None:
                continue
            upload_revisions_from_product_dir(
                code=code,
                product_dir=product_dir,
                minio_client=minio_client,
                overwrite=overwrite,
            )

            if upload_history:
                for _ in upload_revision_history(
                    code=code,
                    product_dir=product_dir,
                    product_type=product_type,
                    minio_client=minio_client,
                ):
                    pass


def upload_revisions_from_product_dir(
    code: str, product_dir: Path, minio_client: Minio, overwrite: bool = False
) -> None:
    """Upload all revision of a product stored as JSON files in a directory.

    Args:
        code: The code of the product.
        product_dir: The directory containing the product's revision files (JSON).
        minio_client: The Minio client to use for uploading.
        overwrite: Whether to overwrite existing files on S3. If false, existing files
            will be skipped.
    """
    # Revision IDs in reverse order (from highest=latest to lowest)
    revision_filepaths = sorted(
        (p for p in product_dir.glob("*.json") if p.stem.isdigit()),
        reverse=True,
    )
    if not overwrite:
        existing_revisions = get_existing_s3_revisions(code, minio_client)
    else:
        existing_revisions = set()

    for i, revision_filepath in enumerate(revision_filepaths):
        rev_id = int(revision_filepath.stem)
        if rev_id in existing_revisions:
            logger.info(
                "Skipping revision %d for barcode %s (already exists)",
                rev_id,
                code,
            )
            continue
        product = orjson.loads(revision_filepath.read_text())
        product = strip_product_from_user_ids(product)
        logger.info(
            "Uploading revision %d/%d for barcode %s",
            i + 1,
            len(revision_filepaths),
            code,
        )
        upload_revision(
            minio_client=minio_client,
            prefix="json",
            code=code,
            product=product,
            set_as_latest=(i == len(revision_filepaths) - 1),
        )


def get_existing_s3_revisions(code: str, minio_client: Minio) -> set[int]:
    """Return the set of existing S3 revision numbers for the given code.

    Args:
        code: The product code.
        minio_client: The Minio client.
    Returns:
        The set of existing S3 revision numbers, as integers.
    """
    objects = minio_client.list_objects(
        bucket_name=settings.AWS_S3_REVISION_BUCKET, prefix=f"json/{code}/"
    )
    return {
        int(Path(obj.object_name).stem)
        for obj in objects
        if obj.object_name.endswith(".json") and Path(obj.object_name).stem.isdigit()
    }


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


def sync_product_revision(code: str, environment: Environment, flavor: Flavor) -> None:
    """Synchronize a product revision to S3.

    This function retrieves the product data from the Open Food Facts API and uploads it
    to S3.

    Args:
        code: The code of the product.
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
        product = api.product.get(code=code)
    except Exception as e:
        logger.error("Failed to sync product revision for barcode %s: %s", code, e)
        return
    if product is None:
        # Product does not exist
        return
    else:
        product = strip_product_from_user_ids(product)
        # Product found
        upload_revision(
            minio_client=client,
            prefix=api_version.value,
            code=code,
            product=product,
            set_as_latest=True,
        )


def delete_product_from_s3(barcode: str) -> None:
    client = get_minio_client()
    remove_latest_revision(
        client,
        APIVersion.v2.value,
        barcode,
    )


def remove_latest_revision(minio_client: Minio, prefix: str, barcode: str):
    """Remove the latest revision for a product from S3.

    Args:
        minio_client: The Minio client.
        prefix: The API version we used when calling the Open Food Facts API.
        barcode: The barcode of the product.
    """
    revision_path = generate_revision_path(prefix, barcode, "latest.json")
    logger.info("Removing latest revision for barcode %s at %s", barcode, revision_path)
    minio_client.remove_object(
        bucket_name=settings.AWS_S3_REVISION_BUCKET,
        object_name=revision_path,
    )


def upload_revision(
    minio_client: Minio,
    prefix: str,
    code: str,
    product: JSONType,
    set_as_latest: bool = False,
) -> None:
    """Upload a product revision to S3.

    Args:
        minio_client: The Minio client.
        prefix: A path prefix to use for the revision path.
        code: The code of the product.
        product: The product data.
        set_as_latest: Whether to upload a revision named "latest.json" alongside the
            regular revision with the same content.
    """
    rev = product["rev"]
    revision_path = generate_revision_path(prefix, code, f"{rev}.json")
    product_bytes = orjson.dumps(product)
    fp = io.BytesIO(product_bytes)
    fp.seek(0)
    logger.info("Uploading revision %s for barcode %s at %s", rev, code, revision_path)
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
            code,
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
    prefix: str,
    barcode: str,
    filename: str,
) -> str:
    """Generate the file path when uploading product revisions or historical events to
    S3.

    Args:
        prefix: A path prefix to use for the revision path.
        barcode: The barcode of the product.
        filename: The filename to use.

    Returns:
        The revision path.
    """
    return f"{prefix}/{barcode}/{filename}"
