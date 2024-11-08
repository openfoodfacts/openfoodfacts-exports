import logging
import shutil
import tempfile
from pathlib import Path

import duckdb

from openfoodfacts_exports import settings

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
