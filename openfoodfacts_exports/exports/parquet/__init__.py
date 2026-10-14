import logging
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import tqdm
from more_itertools import chunked
from openfoodfacts import Flavor
from openfoodfacts.utils import jsonl_iter

from openfoodfacts_exports import settings

from .beauty import BEAUTY_DTYPE_MAP, BEAUTY_PRODUCT_SCHEMA, BeautyProduct
from .common import Product
from .food import FOOD_DTYPE_MAP, FOOD_PRODUCT_SCHEMA, FoodProduct
from .petfood import PETFOOD_DTYPE_MAP, PETFOOD_PRODUCT_SCHEMA, PetFoodProduct

logger = logging.getLogger(__name__)


@dataclass
class ExportStats:
    records_read: int = 0
    records_exported: int = 0
    records_rejected: int = 0

    @property
    def rejection_rate(self) -> float:
        return self.records_rejected / self.records_read if self.records_read else 0.0

    def validate(self, max_rejection_rate: float) -> None:
        if not 0 <= max_rejection_rate <= 1:
            raise ValueError("Maximum rejection rate must be between 0 and 1")
        if self.records_exported == 0:
            raise ValueError("Export contains no valid products")
        if self.rejection_rate > max_rejection_rate:
            raise ValueError(
                f"Export rejection rate {self.rejection_rate:.2%} exceeds "
                f"limit {max_rejection_rate:.2%}"
            )


PARQUET_DATASET_PATH = {
    Flavor.off: settings.DATASET_DIR / "food.parquet",
    Flavor.obf: settings.DATASET_DIR / "beauty.parquet",
    Flavor.opff: settings.DATASET_DIR / "petfood.parquet",
}


def export_parquet(
    dataset_path: Path,
    output_path: Path,
    flavor: Flavor,
    use_tqdm: bool = False,
) -> ExportStats:
    """Convert a JSONL dataset to a local Parquet file.

    Args:
        dataset_path (Path): The path to the JSONL dataset.
        output_path (Path): The path where the Parquet file will be saved.
        flavor (Flavor): The flavor of the dataset.
        use_tqdm (bool, optional): Whether to use tqdm to display a progress
            bar. Defaults to False.
    """
    logger.info("Start JSONL export to Parquet.")

    pydantic_cls: type[Product]
    if flavor == Flavor.off:
        pydantic_cls = FoodProduct
        schema = FOOD_PRODUCT_SCHEMA
        dtype_map = FOOD_DTYPE_MAP
    elif flavor == Flavor.obf:
        pydantic_cls = BeautyProduct
        schema = BEAUTY_PRODUCT_SCHEMA
        dtype_map = BEAUTY_DTYPE_MAP
    elif flavor == Flavor.opff:
        pydantic_cls = PetFoodProduct
        schema = PETFOOD_PRODUCT_SCHEMA
        dtype_map = PETFOOD_DTYPE_MAP
    else:
        raise ValueError(f"Unsupported flavor: {flavor}")

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_converted_parquet_path = Path(tmp_dir) / "converted_data.parquet"
        stats = convert_jsonl_to_parquet(
            output_file_path=tmp_converted_parquet_path,
            dataset_path=dataset_path,
            pydantic_cls=pydantic_cls,
            schema=schema,
            dtype_map=dtype_map,
            use_tqdm=use_tqdm,
        )
        stats.validate(settings.MAX_EXPORT_REJECTION_RATE)
        # Move dataset file to output_path
        shutil.move(tmp_converted_parquet_path, output_path)

    logger.info("JSONL to Parquet conversion and postprocessing completed.")
    return stats


def convert_jsonl_to_parquet(
    output_file_path: Path,
    dataset_path: Path,
    pydantic_cls: type[Product],
    schema: pa.Schema,
    dtype_map: dict[str, pa.DataType] | None = None,
    batch_size: int = 1024,
    row_group_size: int = 122_880,  # DuckDB default row group size,
    use_tqdm: bool = False,
) -> ExportStats:
    """Convert the Open Food Facts JSONL dataset to Parquet format.

    Args:
        output_file_path (Path): The path where the Parquet file will be saved.
        dataset_path (Path): The path to the Open Food Facts JSONL dataset.
        pydantic_cls: The Pydantic class used to validate the JSONL items.
        schema (pa.Schema): The schema of the Parquet file.
        dtype_map (dict[str, pa.DataType], optional): A mapping of field names
            to PyArrow data types. Defaults to None.
        batch_size (int, optional): The size of the batches used to convert the
            dataset. Defaults to 1024.
        row_group_size (int, optional): The size of the row groups in the
            Parquet file. Defaults to 122_880.
        use_tqdm (bool, optional): Whether to use tqdm to display a progress
            bar. Defaults to False.
    """
    stats = ExportStats()
    writer = None
    if dtype_map is None:
        dtype_map = {}
    item_iter = jsonl_iter(dataset_path)
    if use_tqdm:
        item_iter = tqdm.tqdm(item_iter, desc="JSONL")

    try:
        for batch in chunked(item_iter, batch_size):
            # We use by_alias=True because some fields start with a digit
            # (ex: nutriments.100g), and we cannot declare the schema with
            # Pydantic without an alias.
            products = []

            for item in batch:
                stats.records_read += 1
                try:
                    product = pydantic_cls(**item).model_dump(by_alias=True)
                except Exception:
                    stats.records_rejected += 1
                    logger.warning(
                        f"Failed to parse item with code {item.get('code', 'unknown')}",
                        exc_info=True,
                    )
                else:
                    products.append(product)
                    stats.records_exported += 1

            if not products:
                continue

            keys = products[0].keys()
            data = {
                key: pa.array(
                    [product[key] for product in products],
                    # Don't let pyarrow guess type for complex types
                    type=dtype_map.get(key, None),
                )
                for key in keys
            }
            record_batch = pa.record_batch(data, schema=schema)
            if writer is None:
                writer = pq.ParquetWriter(output_file_path, schema=record_batch.schema)
            writer.write_batch(record_batch, row_group_size=row_group_size)

    finally:
        if writer is not None:
            writer.close()
    logger.info(
        "Export quality: read=%d exported=%d rejected=%d rejection_rate=%.2f%%",
        stats.records_read,
        stats.records_exported,
        stats.records_rejected,
        stats.rejection_rate * 100,
    )
    return stats
