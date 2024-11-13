import logging
import shutil
import tempfile
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from huggingface_hub import HfApi
from pydantic import BaseModel, model_validator

from openfoodfacts_exports import settings

logger = logging.getLogger(__name__)


PARQUET_DATASET_PATH = settings.DATASET_DIR / "openfoodfacts-products.parquet"

SQL_QUERY = r"""
SET threads to 4;
SET preserve_insertion_order = false;
COPY ( 
    SELECT
        code,
        additives_n,
        additives_tags,
        allergens_from_ingredients,
        allergens_from_user,
        allergens_tags,
        brands,
        brands_tags,
        categories_properties_tags,
        categories,
        checkers_tags,
        cities_tags,
        compared_to_category,
        complete,
        completeness,
        correctors_tags,
        countries_tags,
        created_t,
        creator,
        data_quality_errors_tags,
        data_quality_info_tags,
        data_quality_warnings_tags,
        data_sources_tags,
        ecoscore_data,
        ecoscore_grade,
        ecoscore_score,
        ecoscore_tags,
        editors,
        emb_codes,
        emb_codes_tags,
        entry_dates_tags,
        environment_impact_level,
        food_groups_tags,
        forest_footprint_data,
        generic_name,
        grades,
        images,
        informers_tags,
        ingredients_analysis_tags,
        ingredients_from_palm_oil_n,
        ingredients_n,
        ingredients_tags,
        ingredients_text_with_allergens,
        ingredients_text,
        COLUMNS('ingredients_text_\w{2}$'),
        ingredients_with_specified_percent_n,
        ingredients_with_unspecified_percent_n,
        ciqual_food_name_tags,
        ingredients_percent_analysis,
        ingredients_original_tags,
        ingredients_without_ciqual_codes_n,
        ingredients_without_ciqual_codes,
        ingredients,
        known_ingredients_n,
        labels_tags,
        lang,
        languages_tags,
        languages_codes,
        last_edit_dates_tags,
        last_editor,
        last_image_t,
        last_modified_by,
        last_modified_t,
        last_updated_t,
        link,
        main_countries_tags,
        manufacturing_places,
        manufacturing_places_tags,
        max_imgid,
        misc_tags,
        minerals_tags,
        new_additives_n,
        no_nutrition_data,
        nova_group,
        nova_groups,
        nova_groups_markers,
        nova_groups_tags,
        nucleotides_tags,
        nutrient_levels_tags,
        unknown_nutrients_tags,
        nutriments,
        nutriscore_data,
        nutriscore_grade,
        nutriscore_score,
        nutriscore_tags,
        nutrition_data_prepared_per,
        nutrition_data,
        nutrition_grades_tags,
        nutrition_score_beverage,
        nutrition_score_warning_fruits_vegetables_nuts_estimate_from_ingredients,
        nutrition_score_warning_no_fiber,
        nutrition_score_warning_no_fruits_vegetables_nuts,
        obsolete_since_date,
        obsolete,
        origins_tags,
        packaging_recycling_tags,
        packaging_shapes_tags,
        packaging_tags,
        packagings_materials,
        packagings_n,
        packagings_n,
        photographers,
        pnns_groups_1_tags,
        pnns_groups_2_tags,
        popularity_key,
        popularity_tags,
        product_name,
        product_quantity_unit,
        product_quantity,
        purchase_places_tags,
        quantity,
        rev,
        scans_n,
        scores,
        serving_quantity,
        serving_size,
        sources,
        sources_fields,
        specific_ingredients,
        states_tags,
        stores,
        stores_tags,
        traces_tags,
        unique_scans_n,
        unknown_ingredients_n,
        vitamins_tags,
        weighers_tags,
        with_non_nutritive_sweeteners,
        with_sweeteners,
    FROM read_ndjson('{dataset_path}', ignore_errors=True)
) TO '{output_path}' (FORMAT PARQUET)
;
"""

_SIZE_SCHEMA = pa.struct(
    [
        pa.field("h", pa.int32(), nullable=True),
        pa.field("w", pa.int32(), nullable=True),
    ]
)

IMAGES_DATATYPE = pa.list_(
    pa.struct(
        [
            pa.field("key", pa.string(), nullable=True),
            pa.field("imgid", pa.int32(), nullable=True),
            pa.field(
                "sizes",
                pa.struct(
                    [
                        pa.field("100", _SIZE_SCHEMA, nullable=True),
                        pa.field("200", _SIZE_SCHEMA, nullable=True),
                        pa.field("400", _SIZE_SCHEMA, nullable=True),
                        pa.field("full", _SIZE_SCHEMA, nullable=True),
                    ]
                ),
                nullable=True,
            ),
            pa.field("uploaded_t", pa.int64(), nullable=True),
            pa.field("uploader", pa.string(), nullable=True),
        ]
    )
)

ALLOWED_IMAGE_SIZE_KEYS = {"100", "200", "400", "full"}

SCHEMAS = {"images": IMAGES_DATATYPE}


class ImageSize(BaseModel):
    h: int | None = None
    w: int | None = None


class Image(BaseModel):
    """`Images` schema for postprocessing used for field postprocessing."""

    key: str
    sizes: dict[str, ImageSize | None]
    uploaded_t: int | None = None
    imgid: int | None = None
    uploader: str | None = None

    @model_validator(mode="after")
    def ignore_extra_sizes(self):
        """Literal doesn't accept extra values, returning an error in case of additional
        keys.
        """
        self.sizes = {
            k: v for k, v in self.sizes.items() if k in ALLOWED_IMAGE_SIZE_KEYS
        }
        return self

    @model_validator(mode="before")
    @classmethod
    def parse_int_from_string(cls, data: dict):
        """Some int are considered as string like '"1517312996"', leading to
        int parsing issues
        """
        imgid = data.get("imgid")
        uploaded_t = data.get("uploaded_t")
        if imgid and isinstance(imgid, str):
            data.update({"imgid": imgid.strip('"')})
        if uploaded_t and isinstance(uploaded_t, str):
            data.update({"uploaded_t": uploaded_t.strip('"')})
        return data


def export_parquet(dataset_path: Path, output_path: Path) -> None:
    """Convert a JSONL dataset to Parquet format and push it to Hugging Face
    Hub.
    """
    logger.info("Start JSONL export to Parquet.")
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_converted_parquet_path = Path(tmp_dir) / "converted_data.parquet"
        tmp_postprocessed_parquet_path = Path(tmp_dir) / "postprocessed_data.parquet"
        convert_jsonl_to_parquet(
            output_file_path=tmp_converted_parquet_path, dataset_path=dataset_path
        )
        postprocess_parquet(
            parquet_path=tmp_converted_parquet_path,
            output_path=tmp_postprocessed_parquet_path,
        )
        # Move dataset file to output_path
        shutil.move(tmp_postprocessed_parquet_path, output_path)

    if settings.ENABLE_HF_PUSH:
        push_parquet_file_to_hf(data_path=output_path)
    else:
        logger.info("Hugging Face push is disabled.")
    logger.info("JSONL to Parquet conversion and postprocessing completed.")


def convert_jsonl_to_parquet(
    output_file_path: Path,
    dataset_path: Path,
) -> None:
    logger.info("Start conversion from JSONL to Parquet.")
    if not dataset_path.exists():
        raise FileNotFoundError(f"{str(dataset_path)} was not found.")
    query = SQL_QUERY.replace("{dataset_path}", str(dataset_path)).replace(
        "{output_path}", str(output_file_path)
    )
    try:
        duckdb.sql(query)
    except duckdb.Error as e:
        logger.error("Error executing query: %s \nError message: %s", query, e)
        raise
    logger.info("JSONL successfully converted into Parquet file.")


def push_parquet_file_to_hf(
    data_path: Path,
    repo_id: str = "openfoodfacts/product-database",
    revision: str = "main",
    commit_message: str = "Database updated",
) -> None:
    logger.info("Start pushing data to Hugging Face at %s", repo_id)
    if not data_path.exists():
        raise FileNotFoundError(f"Data is missing: {data_path}")
    if data_path.suffix != ".parquet":
        raise ValueError(f"A parquet file is expected. Got {data_path.suffix} instead.")
    # We use the HF_Hub api since it gives us way more flexibility than
    # push_to_hub()
    HfApi().upload_file(
        path_or_fileobj=data_path,
        repo_id=repo_id,
        revision=revision,
        repo_type="dataset",
        path_in_repo="products.parquet",
        commit_message=commit_message,
    )
    logger.info("Data succesfully pushed to Hugging Face at %s", repo_id)


def postprocess_parquet(
    parquet_path: Path, output_path: Path, batch_size: int = 10000
) -> None:
    logger.info("Start postprocessing parquet")
    parquet_file = pq.ParquetFile(parquet_path)
    updated_schema = update_schema(parquet_file.schema.to_arrow_schema())
    with pq.ParquetWriter(output_path, schema=updated_schema) as writer:
        for batch in parquet_file.iter_batches(batch_size=batch_size):
            batch = postprocess_arrow_batch(batch)
            writer.write_batch(batch)
    logger.info("Parquet post processing done.")


def update_schema(schema: pa.Schema) -> pa.Schema:
    for field_name, field_datatype in SCHEMAS.items():
        schema = _update_schema_by_field(
            schema=schema, field_name=field_name, field_datatype=field_datatype
        )
    return schema


def _update_schema_by_field(
    schema: pa.Schema, field_name: str, field_datatype: pa.DataType
) -> pa.schema:
    field_index = schema.get_field_index(field_name)
    schema = schema.remove(field_index)
    schema = schema.insert(field_index, pa.field(field_name, field_datatype))
    return schema


def postprocess_arrow_batch(batch: pa.RecordBatch) -> pa.RecordBatch:
    """Add new processing features here."""
    batch = postprocess_images(batch)
    return batch


def postprocess_images(
    batch: pa.RecordBatch, datatype: pa.DataType = IMAGES_DATATYPE
) -> pa.RecordBatch:
    """The `Images` field is a nested JSON with inconsistent data type.
    We extract and structure the data as a list of dict using Pydantic.
    Each dict corresponds to an image from the same product.

    ### Notes:
    The process is quite long (20 - 30 min).
    Possibilities: concurrency, pyarrow.compute, ...
    """
    # Duckdb converted the json filed into a map_array:
    # https://arrow.apache.org/docs/python/generated/pyarrow.MapArray.html#pyarrow-maparray
    postprocessed_images = [
        [Image(key=key, **value).model_dump() for key, value in image] if image else []
        for image in batch["images"].to_pylist()
    ]
    images_col_index = batch.schema.get_field_index("images")
    batch = batch.set_column(
        images_col_index,
        pa.field("images", datatype),
        pa.array(postprocessed_images, type=datatype),
    )
    return batch
