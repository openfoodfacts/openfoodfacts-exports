from enum import StrEnum
import logging
from pathlib import Path

from openfoodfacts import Flavor
from openfoodfacts_exports import settings
from openfoodfacts_exports.exports import push_dataset_file_to_hf
from openfoodfacts_exports.exports.csv_export.csv_from_parquet import (
    export_parquet_to_csv,
)

logger = logging.getLogger(__name__)

CSV_DATASET_PATH = {Flavor.off: settings.DATASET_DIR / "food.csv"}


class Langs(StrEnum):
    ENGLISH = "en"
    FRENCH = "fr"


def export_csv(
    dataset_path: Path,
    output_path: Path,
):
    logger.info("Start CSV export.")
    for lang in Langs:
        logger.info("CSV export in %s.", lang.name)

        # Modify the csv path from food.csv to food_en.csv or food_fr.csv
        csv_path = output_path.with_stem(output_path.stem + f"_{lang}")
        export_parquet_to_csv(
            parquet_path=dataset_path, csv_path=csv_path, lang=lang.value
        )

        if settings.ENABLE_HF_PUSH:
            push_dataset_file_to_hf(
                data_path=csv_path, repo_id="openfoodfacts/product-database"
            )
        else:
            logger.info("Hugging Face push is disabled.")
    logger.info("CSV export finished.")
