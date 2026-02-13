import logging
import shutil
import tempfile
from pathlib import Path

import duckdb

from openfoodfacts_exports import settings
from openfoodfacts_exports.utils import get_minio_client

logger = logging.getLogger(__name__)

MOBILE_APP_DUMP_DATASET_PATH = (
    settings.DATASET_DIR / "openfoodfacts-mobile-dump-products.tsv.gz"
)

MOBILE_APP_DUMP_SQL_QUERY = r"""
SET threads to 4;
SET preserve_insertion_order = false;
COPY ( 
    SELECT
        code,
        product_name,
        quantity,
        brands,
        nutriscore_grade AS nutrition_grade_fr,
        nova_group,
        ecoscore_grade,
    FROM read_parquet('{dataset_path}')
) TO '{output_path}' (HEADER, DELIMITER '\t')
;
"""


def generate_mobile_app_dump(parquet_path: Path, output_path: Path) -> None:
    logger.info("Start mobile app dump generation")
    if not parquet_path.exists():
        raise FileNotFoundError(f"{str(parquet_path)} was not found.")

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_file_path = Path(tmp_dir) / "mobile_dump.csv.gz"
        query = MOBILE_APP_DUMP_SQL_QUERY.replace(
            "{dataset_path}", str(parquet_path)
        ).replace("{output_path}", str(tmp_file_path))
        duckdb.sql(query)
        # Move dataset file to output_path
        shutil.move(tmp_file_path, output_path)

    logger.info("Mobile app dump generation done")


def generate_push_mobile_app_dump(parquet_path: Path) -> None:
    """Generate mobile app dump from a Parquet dump and push it to AWS S3.

    Args:
        parquet_path (Path): Path to the parquet file to generate the mobile app dump
        from.
    """
    generate_mobile_app_dump(parquet_path, MOBILE_APP_DUMP_DATASET_PATH)

    if settings.ENABLE_S3_PUSH:
        logger.info("Uploading mobile app dump to S3")
        client = get_minio_client()
        client.fput_object(
            settings.AWS_S3_DATASET_BUCKET,
            "openfoodfacts-mobile-dump-products.tsv.gz",
            file_path=str(MOBILE_APP_DUMP_DATASET_PATH),
        )
        logger.info("Mobile app dump uploaded to S3")
    else:
        logger.info("S3 push is disabled, skipping upload of mobile app dump")
