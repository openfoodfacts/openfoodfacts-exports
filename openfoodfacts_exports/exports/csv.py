import logging
from pathlib import Path

import duckdb

from openfoodfacts_exports import settings

logger = logging.getLogger(__name__)

MOBILE_APP_DUMP_DATASET_PATH = (
    settings.DATASET_DIR / "en.openfoodfacts.org.products.csv"
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

    query = MOBILE_APP_DUMP_SQL_QUERY.replace(
        "{dataset_path}", str(parquet_path)
    ).replace("{output_path}", str(output_path))
    try:
        duckdb.sql(query)
    except duckdb.Error as e:
        logger.error(f"Error executing query: {query}\nError message: {e}")
        raise
