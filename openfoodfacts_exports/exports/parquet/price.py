import datetime
import hashlib
import logging
import shutil
import tempfile
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import tqdm
from more_itertools import chunked
from openfoodfacts.utils import jsonl_iter
from pydantic import BaseModel, field_serializer

from openfoodfacts_exports import settings
from openfoodfacts_exports.exports.parquet.common import push_parquet_file_to_hf

logger = logging.getLogger(__name__)


PRICE_DATASET_PATH = settings.DATASET_DIR / "prices.parquet"


class ProofModel(BaseModel):
    id: int | None = None
    file_path: str | None = None
    mimetype: str | None = None
    type: str
    image_thumb_path: str | None = None
    date: datetime.date | None = None
    currency: str | None = None
    receipt_price_count: int | None = None
    receipt_price_total: Decimal | None = None
    owner: str | None = None
    source: str | None = None
    created: datetime.datetime | None = None
    updated: datetime.datetime | None = None

    @field_serializer("owner")
    def serialize_owner(self, owner: str | None, _info) -> str | None:
        if owner is None:
            return None
        return hashlib.sha256(owner.encode("utf-8")).hexdigest()[:8]


class LocationModel(BaseModel):
    id: int | None = None
    type: str
    osm_id: int | None = None
    osm_type: str | None = None
    osm_name: str | None = None
    osm_display_name: str | None = None
    osm_tag_key: str | None = None
    osm_tag_value: str | None = None
    osm_address_postcode: str | None = None
    osm_address_city: str | None = None
    osm_address_country: str | None = None
    osm_address_country_code: str | None = None
    osm_lat: float | None = None
    osm_lon: float | None = None
    website_url: str | None = None
    source: str | None = None
    created: datetime.datetime | None = None
    updated: datetime.datetime | None = None


class PriceModel(BaseModel):
    id: int | None = None
    type: str
    product_code: str | None = None
    product_name: str | None = None
    category_tag: str | None = None
    labels_tags: list[str] | None = None
    origins_tags: list[str] | None = None
    price: Decimal | None = None
    price_is_discounted: bool | None = None
    price_without_discount: Decimal | None = None
    discount_type: str | None = None
    price_per: str | None = None
    currency: str | None = None
    location_osm_id: int | None = None
    location_osm_type: str | None = None
    location_id: int | None = None
    date: datetime.date | None = None
    proof_id: int | None = None
    receipt_quantity: float | None = None
    owner: str | None = None
    source: str | None = None
    created: datetime.datetime | None = None
    updated: datetime.datetime | None = None

    @field_serializer("owner")
    def serialize_owner(self, owner: str | None, _info) -> str | None:
        if owner is None:
            return None
        return hashlib.sha256(owner.encode("utf-8")).hexdigest()[:8]


PRICE_PRODUCT_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=True),
        pa.field("type", pa.string(), nullable=True),
        pa.field("product_code", pa.string(), nullable=True),
        pa.field("product_name", pa.string(), nullable=True),
        pa.field("category_tag", pa.string(), nullable=True),
        pa.field("labels_tags", pa.list_(pa.string()), nullable=True),
        pa.field("origins_tags", pa.list_(pa.string()), nullable=True),
        pa.field("price", pa.decimal128(10, 3), nullable=True),
        pa.field("price_is_discounted", pa.bool_(), nullable=True),
        pa.field("price_without_discount", pa.decimal128(10, 3), nullable=True),
        pa.field("discount_type", pa.string(), nullable=True),
        pa.field("price_per", pa.string(), nullable=True),
        pa.field("currency", pa.string(), nullable=True),
        pa.field("location_osm_id", pa.int64(), nullable=True),
        pa.field("location_osm_type", pa.string(), nullable=True),
        pa.field("location_id", pa.int32(), nullable=True),
        pa.field("date", pa.date32(), nullable=True),
        pa.field("proof_id", pa.int32(), nullable=True),
        pa.field("receipt_quantity", pa.float32(), nullable=True),
        pa.field("owner", pa.string(), nullable=True),
        pa.field("source", pa.string(), nullable=True),
        pa.field("created", pa.timestamp("us", tz="UTC"), nullable=True),
        pa.field("updated", pa.timestamp("us", tz="UTC"), nullable=True),
        pa.field("proof_file_path", pa.string(), nullable=True),
        pa.field("proof_mimetype", pa.string(), nullable=True),
        pa.field("proof_type", pa.string(), nullable=True),
        pa.field("proof_date", pa.date32(), nullable=True),
        pa.field("proof_currency", pa.string(), nullable=True),
        pa.field("proof_receipt_price_count", pa.int32(), nullable=True),
        pa.field("proof_receipt_price_total", pa.decimal128(10, 3), nullable=True),
        pa.field("proof_owner", pa.string(), nullable=True),
        pa.field("proof_source", pa.string(), nullable=True),
        pa.field("proof_created", pa.timestamp("us", tz="UTC"), nullable=True),
        pa.field("proof_updated", pa.timestamp("us", tz="UTC"), nullable=True),
        pa.field("location_type", pa.string(), nullable=True),
        pa.field("location_osm_display_name", pa.string(), nullable=True),
        pa.field("location_osm_tag_key", pa.string(), nullable=True),
        pa.field("location_osm_tag_value", pa.string(), nullable=True),
        pa.field("location_osm_address_postcode", pa.string(), nullable=True),
        pa.field("location_osm_address_city", pa.string(), nullable=True),
        pa.field("location_osm_address_country", pa.string(), nullable=True),
        pa.field("location_osm_address_country_code", pa.string(), nullable=True),
        pa.field("location_osm_lat", pa.float64(), nullable=True),
        pa.field("location_osm_lon", pa.float64(), nullable=True),
        pa.field("location_website_url", pa.string(), nullable=True),
        pa.field("location_source", pa.string(), nullable=True),
        pa.field("location_created", pa.timestamp("us", tz="UTC"), nullable=True),
        pa.field("location_updated", pa.timestamp("us", tz="UTC"), nullable=True),
    ]
)


PROOF_KEYS = [
    "file_path",
    "mimetype",
    "type",
    "date",
    "currency",
    "receipt_price_count",
    "receipt_price_total",
    "owner",
    "source",
    "created",
    "updated",
]


LOCATION_KEYS = [
    "type",
    "osm_display_name",
    "osm_tag_key",
    "osm_tag_value",
    "osm_address_postcode",
    "osm_address_city",
    "osm_address_country",
    "osm_address_country_code",
    "osm_lat",
    "osm_lon",
    "website_url",
    "source",
    "created",
    "updated",
]


def convert_jsonl_to_parquet(
    output_file_path: Path,
    dataset_price_path: Path,
    dataset_proof_path: Path,
    dataset_location_path: Path,
    batch_size: int = 1024,
    row_group_size: int = 122_880,  # DuckDB default row group size,
    use_tqdm: bool = False,
) -> None:
    """Convert the Open Prices JSONL dataset to Parquet format.

    Args:
        output_file_path (Path): The path where the Parquet file will be saved.
        dataset_price_path (Path): The path to the `price` JSONL dataset.
        dataset_proof_path (Path): The path to the `proof` JSONL dataset.
        dataset_location_path (Path): The path to the `location` JSONL dataset.
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
    item_iter = jsonl_iter(dataset_price_path)
    if use_tqdm:
        item_iter = tqdm.tqdm(item_iter, desc="JSONL")

    proof_by_id = {
        proof["id"]: ProofModel(**proof).model_dump()
        for proof in jsonl_iter(dataset_proof_path)
    }
    location_by_id = {
        location["id"]: LocationModel(**location).model_dump()
        for location in jsonl_iter(dataset_location_path)
    }

    for batch in chunked(item_iter, batch_size):
        prices = []
        keys: list[str] = []
        for item in batch:
            price = PriceModel(**item).model_dump()
            if not keys:
                keys = (
                    list(price.keys())
                    + [f"proof_{key}" for key in PROOF_KEYS]
                    + [f"location_{key}" for key in LOCATION_KEYS]
                )
            for key, store, fk_fields in [
                (
                    "proof",
                    proof_by_id,
                    PROOF_KEYS,
                ),
                (
                    "location",
                    location_by_id,
                    LOCATION_KEYS,
                ),
            ]:
                if price[f"{key}_id"] is not None:
                    value = price[f"{key}_id"]

                    for fk_field in fk_fields:
                        price[f"{key}_{fk_field}"] = store[value][fk_field]

            prices.append(price)

        data = {
            key: pa.array(
                [price.get(key) for price in prices],
                type=None,
            )
            for key in keys
        }
        record_batch = pa.record_batch(data, schema=PRICE_PRODUCT_SCHEMA)
        if writer is None:
            writer = pq.ParquetWriter(output_file_path, schema=record_batch.schema)
        writer.write_batch(record_batch, row_group_size=row_group_size)

    if writer is not None:
        writer.close()


def export_parquet(
    dataset_paths: dict[str, Path], output_path: Path, use_tqdm: bool = False
) -> None:
    """Convert a JSONL dataset to Parquet format and push it to Hugging Face
    Hub.

    Args:
        dataset_path (Path): The path to the JSONL dataset.
        output_path (Path): The path where the Parquet file will be saved.
        use_tqdm (bool, optional): Whether to use tqdm to display a progress
            bar. Defaults to False.
    """
    logger.info("Start Open Prices JSONL export to Parquet.")

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_converted_parquet_path = Path(tmp_dir) / "converted_data.parquet"
        convert_jsonl_to_parquet(
            output_file_path=tmp_converted_parquet_path,
            dataset_price_path=dataset_paths["price"],
            dataset_proof_path=dataset_paths["proof"],
            dataset_location_path=dataset_paths["location"],
            use_tqdm=use_tqdm,
        )
        # Move dataset file to output_path
        shutil.move(tmp_converted_parquet_path, output_path)

    if settings.ENABLE_HF_PUSH:
        push_parquet_file_to_hf(
            data_path=output_path, repo_id="openfoodfacts/open-prices"
        )
    else:
        logger.info("Hugging Face push is disabled.")
    logger.info("JSONL to Parquet conversion and postprocessing completed.")
