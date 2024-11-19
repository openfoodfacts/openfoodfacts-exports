import logging
import shutil
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import tqdm
from more_itertools import chunked
from openfoodfacts import Flavor
from openfoodfacts.utils import jsonl_iter

from openfoodfacts_exports import settings

from .beauty import BEAUTY_DTYPE_MAP, BEAUTY_PRODUCT_SCHEMA, BeautyProduct
from .common import Product, push_parquet_file_to_hf
from .food import FOOD_DTYPE_MAP, FOOD_PRODUCT_SCHEMA, FoodProduct

logger = logging.getLogger(__name__)


PARQUET_DATASET_PATH = {
    Flavor.off: settings.DATASET_DIR / "food.parquet",
    Flavor.obf: settings.DATASET_DIR / "beauty.parquet",
}


def export_parquet(
    dataset_path: Path, output_path: Path, flavor: Flavor, use_tqdm: bool = False
) -> None:
    """Convert a JSONL dataset to Parquet format and push it to Hugging Face
    Hub.

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
    else:
        raise ValueError(f"Unsupported flavor: {flavor}")

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_converted_parquet_path = Path(tmp_dir) / "converted_data.parquet"
        convert_jsonl_to_parquet(
            output_file_path=tmp_converted_parquet_path,
            dataset_path=dataset_path,
            pydantic_cls=pydantic_cls,
            schema=schema,
            dtype_map=dtype_map,
            use_tqdm=use_tqdm,
        )
        # Move dataset file to output_path
        shutil.move(tmp_converted_parquet_path, output_path)

    if settings.ENABLE_HF_PUSH:
        push_parquet_file_to_hf(
            data_path=output_path, repo_id="openfoodfacts/product-database"
        )
    else:
        logger.info("Hugging Face push is disabled.")
    logger.info("JSONL to Parquet conversion and postprocessing completed.")


def convert_jsonl_to_parquet(
    output_file_path: Path,
    dataset_path: Path,
    pydantic_cls: type[Product],
    schema: pa.Schema,
    dtype_map: dict[str, pa.DataType] | None = None,
    batch_size: int = 1024,
    row_group_size: int = 122_880,  # DuckDB default row group size,
    use_tqdm: bool = False,
) -> None:
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
    writer = None
    if dtype_map is None:
        dtype_map = {}
    item_iter = jsonl_iter(dataset_path)
    if use_tqdm:
        item_iter = tqdm.tqdm(item_iter, desc="JSONL")

    for batch in chunked(item_iter, batch_size):
        # We use by_alias=True because some fields start with a digit
        # (ex: nutriments.100g), and we cannot declare the schema with
        # Pydantic without an alias.
        products = [pydantic_cls(**item).model_dump(by_alias=True) for item in batch]
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

    if writer is not None:
        writer.close()
