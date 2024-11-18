import logging
import shutil
import tempfile
from pathlib import Path

import orjson
import pyarrow as pa
import pyarrow.parquet as pq
from huggingface_hub import HfApi
from more_itertools import chunked
from openfoodfacts.utils import jsonl_iter
from pydantic import BaseModel, Field, field_serializer, model_validator

from openfoodfacts_exports import settings

logger = logging.getLogger(__name__)


PARQUET_DATASET_PATH = settings.DATASET_DIR / "openfoodfacts-products.parquet"


IMAGE_SIZE_SCHEMA = pa.struct(
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
                        pa.field("100", IMAGE_SIZE_SCHEMA, nullable=True),
                        pa.field("200", IMAGE_SIZE_SCHEMA, nullable=True),
                        pa.field("400", IMAGE_SIZE_SCHEMA, nullable=True),
                        pa.field("full", IMAGE_SIZE_SCHEMA, nullable=True),
                    ]
                ),
                nullable=True,
            ),
            pa.field("uploaded_t", pa.int64(), nullable=True),
            pa.field("uploader", pa.string(), nullable=True),
        ]
    )
)

INGREDIENTS_TEXT_DATATYPE = pa.list_(
    pa.struct(
        [
            pa.field("lang", pa.string()),
            pa.field("text", pa.string()),
        ],
    )
)


LANGUAGE_FIELD_DATATYPE = pa.list_(
    pa.struct(
        [
            pa.field("lang", pa.string()),
            pa.field("text", pa.string()),
        ]
    ),
)

NUTRIMENTS_DATATYPE = pa.list_(
    pa.struct(
        [
            pa.field("name", pa.string()),
            pa.field("value", pa.float32(), nullable=True),
            pa.field("100g", pa.float32(), nullable=True),
            pa.field("serving", pa.float32(), nullable=True),
            pa.field("unit", pa.string(), nullable=True),
            pa.field("prepared_value", pa.float32(), nullable=True),
            pa.field("prepared_100g", pa.float32(), nullable=True),
            pa.field("prepared_serving", pa.float32(), nullable=True),
            pa.field("prepared_unit", pa.string(), nullable=True),
        ]
    )
)

PACKAGING_FIELD_DATATYPE = pa.list_(
    pa.struct(
        [
            pa.field("material", pa.string(), nullable=True),
            pa.field("number_of_units", pa.int64(), nullable=True),
            pa.field("quantity_per_unit", pa.string(), nullable=True),
            pa.field("quantity_per_unit_unit", pa.string(), nullable=True),
            pa.field("quantity_per_unit_value", pa.string(), nullable=True),
            pa.field("recycling", pa.string(), nullable=True),
            pa.field("shape", pa.string(), nullable=True),
            pa.field("weight_measured", pa.float32(), nullable=True),
        ]
    )
)

PRODUCT_SCHEMA = pa.schema(
    [
        pa.field("additives_n", pa.int32(), nullable=True),
        pa.field("additives_tags", pa.list_(pa.string()), nullable=True),
        pa.field("allergens_tags", pa.list_(pa.string()), nullable=True),
        pa.field("brands_tags", pa.list_(pa.string()), nullable=True),
        pa.field("brands", pa.string(), nullable=True),
        pa.field("categories", pa.string(), nullable=True),
        pa.field("categories_tags", pa.list_(pa.string()), nullable=True),
        pa.field("checkers_tags", pa.list_(pa.string()), nullable=True),
        pa.field("ciqual_food_name_tags", pa.list_(pa.string()), nullable=True),
        pa.field("cities_tags", pa.list_(pa.string()), nullable=True),
        pa.field("code", pa.string()),
        pa.field("compared_to_category", pa.string(), nullable=True),
        pa.field("complete", pa.int32(), nullable=True),
        pa.field("completeness", pa.float32(), nullable=True),
        pa.field("correctors_tags", pa.list_(pa.string()), nullable=True),
        pa.field("countries_tags", pa.list_(pa.string()), nullable=True),
        pa.field("created_t", pa.int64(), nullable=True),
        pa.field("creator", pa.string(), nullable=True),
        pa.field("data_quality_errors_tags", pa.list_(pa.string()), nullable=True),
        pa.field("data_quality_info_tags", pa.list_(pa.string()), nullable=True),
        pa.field("data_quality_warnings_tags", pa.list_(pa.string()), nullable=True),
        pa.field("data_sources_tags", pa.list_(pa.string()), nullable=True),
        pa.field("ecoscore_grade", pa.string(), nullable=True),
        pa.field("ecoscore_score", pa.int32(), nullable=True),
        pa.field("ecoscore_tags", pa.list_(pa.string()), nullable=True),
        pa.field("editors", pa.list_(pa.string()), nullable=True),
        pa.field("emb_codes_tags", pa.list_(pa.string()), nullable=True),
        pa.field("emb_codes", pa.string(), nullable=True),
        pa.field("entry_dates_tags", pa.list_(pa.string()), nullable=True),
        pa.field("food_groups_tags", pa.list_(pa.string()), nullable=True),
        pa.field("generic_name", LANGUAGE_FIELD_DATATYPE, nullable=True),
        pa.field("images", IMAGES_DATATYPE, nullable=True),
        pa.field("informers_tags", pa.list_(pa.string()), nullable=True),
        pa.field("ingredients_analysis_tags", pa.list_(pa.string()), nullable=True),
        pa.field("ingredients_from_palm_oil_n", pa.int32(), nullable=True),
        pa.field("ingredients_n", pa.int32(), nullable=True),
        pa.field("ingredients_original_tags", pa.list_(pa.string()), nullable=True),
        pa.field("ingredients_percent_analysis", pa.int32(), nullable=True),
        pa.field("ingredients_tags", pa.list_(pa.string()), nullable=True),
        pa.field("ingredients_text", LANGUAGE_FIELD_DATATYPE, nullable=True),
        pa.field("ingredients_with_specified_percent_n", pa.int32(), nullable=True),
        pa.field("ingredients_with_unspecified_percent_n", pa.int32(), nullable=True),
        pa.field("ingredients_without_ciqual_codes_n", pa.int32(), nullable=True),
        pa.field(
            "ingredients_without_ciqual_codes", pa.list_(pa.string()), nullable=True
        ),
        pa.field("ingredients", pa.string(), nullable=True),
        pa.field("known_ingredients_n", pa.int32(), nullable=True),
        pa.field("labels_tags", pa.list_(pa.string()), nullable=True),
        pa.field("labels", pa.string(), nullable=True),
        pa.field("lang", pa.string(), nullable=True),
        pa.field("languages_tags", pa.list_(pa.string()), nullable=True),
        pa.field("last_edit_dates_tags", pa.list_(pa.string()), nullable=True),
        pa.field("last_editor", pa.string(), nullable=True),
        pa.field("last_image_t", pa.int64(), nullable=True),
        pa.field("last_modified_by", pa.string(), nullable=True),
        pa.field("last_modified_t", pa.int64(), nullable=True),
        pa.field("last_updated_t", pa.int64(), nullable=True),
        pa.field("link", pa.string(), nullable=True),
        pa.field("main_countries_tags", pa.list_(pa.string()), nullable=True),
        pa.field("manufacturing_places_tags", pa.list_(pa.string()), nullable=True),
        pa.field("manufacturing_places", pa.string(), nullable=True),
        pa.field("max_imgid", pa.int32(), nullable=True),
        pa.field("minerals_tags", pa.list_(pa.string()), nullable=True),
        pa.field("misc_tags", pa.list_(pa.string()), nullable=True),
        pa.field("new_additives_n", pa.int32(), nullable=True),
        pa.field("no_nutrition_data", pa.bool_(), nullable=True),
        pa.field("nova_group", pa.int32(), nullable=True),
        pa.field("nova_groups_tags", pa.list_(pa.string()), nullable=True),
        pa.field("nova_groups", pa.string(), nullable=True),
        pa.field("nucleotides_tags", pa.list_(pa.string()), nullable=True),
        pa.field("nutrient_levels_tags", pa.list_(pa.string()), nullable=True),
        pa.field("nutriments", NUTRIMENTS_DATATYPE, nullable=True),
        pa.field("nutriscore_grade", pa.string(), nullable=True),
        pa.field("nutriscore_score", pa.int32(), nullable=True),
        pa.field("nutrition_data_per", pa.string(), nullable=True),
        pa.field("obsolete", pa.bool_()),
        pa.field("origins_tags", pa.list_(pa.string()), nullable=True),
        pa.field("origins", pa.string(), nullable=True),
        pa.field("owner", pa.string(), nullable=True),
        pa.field("packagings_complete", pa.bool_(), nullable=True),
        pa.field("packaging_recycling_tags", pa.list_(pa.string()), nullable=True),
        pa.field("packaging_shapes_tags", pa.list_(pa.string()), nullable=True),
        pa.field("packaging_tags", pa.list_(pa.string()), nullable=True),
        pa.field("packaging_text", LANGUAGE_FIELD_DATATYPE, nullable=True),
        pa.field("packaging", pa.string(), nullable=True),
        pa.field("packagings", PACKAGING_FIELD_DATATYPE, nullable=True),
        pa.field("photographers", pa.list_(pa.string()), nullable=True),
        pa.field("popularity_key", pa.int64(), nullable=True),
        pa.field("popularity_tags", pa.list_(pa.string()), nullable=True),
        pa.field("product_name", LANGUAGE_FIELD_DATATYPE, nullable=True),
        pa.field("product_quantity_unit", pa.string(), nullable=True),
        pa.field("product_quantity", pa.string(), nullable=True),
        pa.field("purchase_places_tags", pa.list_(pa.string()), nullable=True),
        pa.field("quantity", pa.string(), nullable=True),
        pa.field("rev", pa.int32(), nullable=True),
        pa.field("scans_n", pa.int32(), nullable=True),
        pa.field("serving_quantity", pa.string(), nullable=True),
        pa.field("serving_size", pa.string(), nullable=True),
        pa.field("states_tags", pa.list_(pa.string()), nullable=True),
        pa.field("stores_tags", pa.list_(pa.string()), nullable=True),
        pa.field("stores", pa.string(), nullable=True),
        pa.field("traces_tags", pa.list_(pa.string()), nullable=True),
        pa.field("unique_scans_n", pa.int32(), nullable=True),
        pa.field("unknown_ingredients_n", pa.int32(), nullable=True),
        pa.field("unknown_nutrients_tags", pa.list_(pa.string()), nullable=True),
        pa.field("vitamins_tags", pa.list_(pa.string()), nullable=True),
        pa.field("with_non_nutritive_sweeteners", pa.int32(), nullable=True),
        pa.field("with_sweeteners", pa.int32(), nullable=True),
    ]
)


LANGUAGE_FIELDS = [
    "ingredients_text",
    "product_name",
    "packaging_text",
    "generic_name",
]


class ImageSize(BaseModel):
    h: int | None = None
    w: int | None = None


ALLOWED_IMAGE_SIZE_KEYS = {"100", "200", "400", "full"}


class Image(BaseModel):
    """`Images` schema for postprocessing used for field postprocessing."""

    key: str | None = None
    sizes: dict[str, ImageSize] | None = None
    uploaded_t: int | None = None
    imgid: int | None = None
    uploader: str | None = None

    @model_validator(mode="after")
    def ignore_extra_sizes(self):
        """Literal doesn't accept extra values, returning an error in case of
        additional keys.
        """
        if self.sizes:
            self.sizes = {
                k: v for k, v in self.sizes.items() if k in ALLOWED_IMAGE_SIZE_KEYS
            }
        return self

    @model_validator(mode="before")
    @classmethod
    def parse_sizes(cls, data: dict) -> dict:
        sizes = data.pop("sizes", None)
        if sizes:
            sizes = {key: values for key, values in sizes.items() if values}
        data["sizes"] = sizes or None
        return data


class Ingredient(BaseModel):
    percent_max: float | None = None
    percent_min: float | None = None
    is_in_taxonomy: int | None = None
    percent_estimate: float | None = None
    vegan: str | None = None
    id: str | None = None
    text: str | None = None
    vegetarian: str | None = None
    ciqual_food_code: str | None = None
    percent: float | None = None
    from_palm_oil: str | None = None
    ingredients: list["Ingredient"] | None = None
    ecobalyse_code: str | None = None
    processing: str | None = None
    labels: str | None = None
    origins: str | None = None
    ecobalyse_proxy_code: str | None = None
    quantity: str | None = None
    quantity_g: float | None = None
    ciqual_proxy_food_code: str | None = None

    @model_validator(mode="before")
    @classmethod
    def parse_nested_ingredients(cls, data: dict):
        if "ingredients" in data and isinstance(data["ingredients"], list):
            data["ingredients"] = [
                cls.model_validate(ing) for ing in data["ingredients"]
            ]
        return data


class LanguageField(BaseModel):
    lang: str
    text: str


class NutrimentField(BaseModel):
    name: str
    value: float | None = None
    per_100g: float | None = Field(default=None, alias="100g")
    serving: float | None = None
    unit: str | None = None
    prepared_value: float | None = None
    prepared_100g: float | None = None
    prepared_serving: float | None = None
    prepared_unit: str | None = None


class PackagingField(BaseModel):
    material: str | None = None
    number_of_units: int | None = None
    quantity_per_unit: str | None = None
    quantity_per_unit_unit: str | None = None
    quantity_per_unit_value: str | None = Field(
        default=None, coerce_numbers_to_str=True
    )
    recycling: str | None = None
    shape: str | None = None
    weight_measured: float | None = None


class Product(BaseModel):
    additives_n: int | None = None
    additives_tags: list[str] | None = None
    allergens_tags: list[str] | None = None
    brands_tags: list[str] | None = None
    brands: str | None = None
    categories: str | None = None
    categories_tags: list[str] | None = None
    checkers_tags: list[str] | None = None
    ciqual_food_name_tags: list[str] | None = None
    cities_tags: list[str] | None = None
    code: str
    compared_to_category: str | None = None
    complete: int | None = None
    completeness: float | None = None
    correctors_tags: list[str] | None = None
    countries_tags: list[str] | None = None
    created_t: int | None = None
    creator: str | None = None
    data_quality_errors_tags: list[str] | None = None
    data_quality_info_tags: list[str] | None = None
    data_quality_warnings_tags: list[str] | None = None
    data_sources_tags: list[str] | None = None
    ecoscore_grade: str | None = None
    ecoscore_score: int | None = None
    ecoscore_tags: list[str] | None = None
    editors: list[str] | None = None
    emb_codes_tags: list[str] | None = None
    emb_codes: str | None = None
    entry_dates_tags: list[str] | None = None
    food_groups_tags: list[str] | None = None
    generic_name: list[LanguageField] | None = None
    images: list[Image] | None = None
    informers_tags: list[str] | None = None
    ingredients_analysis_tags: list[str] | None = None
    ingredients_from_palm_oil_n: int | None = None
    ingredients_n: int | None = None
    ingredients_original_tags: list[str] | None = None
    ingredients_percent_analysis: int | None = None
    ingredients_tags: list[str] | None = None
    ingredients_with_specified_percent_n: int | None = None
    ingredients_with_unspecified_percent_n: int | None = None
    ingredients_without_ciqual_codes_n: int | None = None
    ingredients_without_ciqual_codes: list[str] | None = None
    ingredients: list[Ingredient] | None = None
    known_ingredients_n: int | None = None
    labels_tags: list[str] | None = None
    labels: str | None = None
    lang: str | None = None
    languages_tags: list[str] | None = None
    last_edit_dates_tags: list[str] | None = None
    last_editor: str | None = None
    last_image_t: int | None = None
    last_modified_by: str | None = None
    last_modified_t: int | None = None
    last_updated_t: int | None = None
    link: str | None = None
    main_countries_tags: list[str] | None = None
    manufacturing_places_tags: list[str] | None = None
    manufacturing_places: str | None = None
    max_imgid: int | None = None
    minerals_tags: list[str] | None = None
    misc_tags: list[str] | None = None
    new_additives_n: int | None = None
    no_nutrition_data: bool | None = None
    nova_group: int | None = None
    nova_groups_tags: list[str] | None = None
    nova_groups: str | None = None
    nucleotides_tags: list[str] | None = None
    nutrient_levels_tags: list[str] | None = None
    nutriments: list[NutrimentField] | None = None
    nutriscore_grade: str | None = None
    nutriscore_score: int | None = None
    nutrition_data_per: str | None = None
    obsolete: bool = False
    origins_tags: list[str] | None = None
    origins: str | None = None
    owner: str | None = None
    packagings_complete: bool | None = None
    packaging_recycling_tags: list[str] | None = None
    packaging_shapes_tags: list[str] | None = None
    packaging_tags: list[str] | None = None
    packaging_text: list[LanguageField] | None = None
    packaging: str | None = None
    packagings: list[PackagingField] | None = None
    photographers: list[str] | None = None
    popularity_key: int | None = None
    popularity_tags: list[str] | None = None
    product_name: list[LanguageField] | None = None
    product_quantity_unit: str | None = None
    product_quantity: str | None = Field(default=None, coerce_numbers_to_str=True)
    purchase_places_tags: list[str] | None = None
    quantity: str | None = None
    rev: int | None = None
    scans_n: int | None = None
    serving_quantity: str | None = Field(default=None, coerce_numbers_to_str=True)
    serving_size: str | None = None
    states_tags: list[str] | None = None
    stores_tags: list[str] | None = None
    stores: str | None = None
    traces_tags: list[str] | None = None
    unique_scans_n: int | None = None
    unknown_ingredients_n: int | None = None
    unknown_nutrients_tags: list[str] | None = None
    vitamins_tags: list[str] | None = None
    with_non_nutritive_sweeteners: int | None = None
    with_sweeteners: int | None = None
    ingredients_text: list[LanguageField] | None = None

    @model_validator(mode="before")
    @classmethod
    def parse_bool_values(cls, data: dict):
        """Parse boolean values from string to bool."""
        data.pop("obsolete", None)
        for field_name in ("no_nutrition_data",):
            if field_name in data:
                data[field_name] = data[field_name] in (
                    "on",
                    "true",
                    1,
                    True,
                )
        return data

    @model_validator(mode="before")
    @classmethod
    def parse_nutriments(cls, data: dict):
        nutriments = data.pop("nutriments", None)
        parsed_nutriments: dict[str, dict] = {}
        nutriments_end_mapping = {
            "_prepared_100g": "prepared_100g",
            "_prepared_serving": "prepared_serving",
            "_prepared_unit": "prepared_unit",
            "_prepared_value": "prepared_value",
            "_unit": "unit",
            "_value": "value",
            "_100g": "100g",
            "_serving": "serving",
        }
        if nutriments:
            for key, value in nutriments.items():
                for end_key, new_key in nutriments_end_mapping.items():
                    if key.endswith(end_key):
                        key = key.replace(end_key, "")
                        parsed_nutriments.setdefault(key, {})
                        parsed_nutriments[key][new_key] = value

            data["nutriments"] = [
                {"name": key, **value} for key, value in parsed_nutriments.items()
            ]

        else:
            data["nutriments"] = None
        return data

    @model_validator(mode="before")
    @classmethod
    def parse_language_fields(cls, data: dict) -> dict:
        """Parse language fields (such as `ingredients_text`) into a list of
        dictionaries with `lang` and `text` keys.

        In Open Food Facts, main language is stored in the field without a
        suffix, while other languages are stored with a suffix of the
        language code.

        To make the schema compatible with Parquet, we convert these fields
        into a list of dictionaries with `lang` and `text` keys.
        This way, the structure is consistent across all language fields.

        The main language is stored with a `lang` value of "main", while other
        languages are stored with their language code (2-letter code).
        """
        for field_name in LANGUAGE_FIELDS:
            main_language_value = data.pop(field_name, None)
            data[field_name] = []

            if main_language_value:
                data[field_name].append({"lang": "main", "text": main_language_value})

            for key in list(data.keys()):
                if key.startswith(f"{field_name}_"):
                    lang = key.rsplit("_", maxsplit=1)[-1]
                    value = data.pop(key)
                    # Sometimes we have a "debug" field that is not a language
                    # Sometimes we have a language field with a None value
                    if len(lang) == 2 and value is not None and len(value):
                        data[field_name].append({"lang": lang, "text": value})

            if data[field_name] == {}:
                data[field_name] = None

        return data

    @model_validator(mode="before")
    @classmethod
    def parse_images(cls, data: dict) -> dict:
        """Parse images field into a list of dictionaries with `key`, `imgid`,
        `sizes`, `uploaded_t`, and `uploader` keys.

        In Open Food Facts, images are stored as a dictionary with the image
        key as the key and the image data as the value.

        To make the schema compatible with Parquet, we convert these fields
        into a list of dictionaries with `key`, `imgid`, `sizes`, `uploaded_t`,
        and `uploader` keys. We copy the image key (ex: `3`, `nutrition_fr`,...)
        from the original dictionary and add it as a field under the `key` key.
        """
        images = data.pop("images", None)
        data["images"] = []
        if images:
            for key, value in images.items():
                data["images"].append({"key": key, **value})
        return data

    @model_validator(mode="before")
    @classmethod
    def parse_ecoscore_score(cls, data: dict):
        ecoscore_score = data.get("ecoscore_score")
        if ecoscore_score and isinstance(ecoscore_score, float):
            # Some `ecoscore_score` are float, we need to convert them to int
            # to prevent Pydantic from raising an error
            data["ecoscore_score"] = int(ecoscore_score)

        return data

    @field_serializer("ingredients")
    def serialize_ingredients(
        self, ingredients: list[Ingredient] | None, _info
    ) -> str | None:
        """Ingredients can be nested, which seems difficult to implement as an
        Arrow struct.
        To alleviate this, we serialize the ingredients as a JSON string."""
        if ingredients is None:
            return None
        return orjson.dumps([ing.model_dump() for ing in ingredients]).decode("utf-8")


def export_parquet(dataset_path: Path, output_path: Path) -> None:
    """Convert a JSONL dataset to Parquet format and push it to Hugging Face
    Hub."""
    logger.info("Start JSONL export to Parquet.")
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_converted_parquet_path = Path(tmp_dir) / "converted_data.parquet"
        convert_jsonl_to_parquet(
            output_file_path=tmp_converted_parquet_path,
            dataset_path=dataset_path,
            schema=PRODUCT_SCHEMA,
        )
        # Move dataset file to output_path
        shutil.move(tmp_converted_parquet_path, output_path)

    if settings.ENABLE_HF_PUSH:
        push_parquet_file_to_hf(data_path=output_path)
    else:
        logger.info("Hugging Face push is disabled.")
    logger.info("JSONL to Parquet conversion and postprocessing completed.")


def convert_jsonl_to_parquet(
    output_file_path: Path,
    dataset_path: Path,
    schema: pa.Schema = PRODUCT_SCHEMA,
    batch_size: int = 1024,
    row_group_size: int = 122_880,  # DuckDB default row group size
) -> None:
    """Convert the Open Food Facts JSONL dataset to Parquet format.

    Args:
        output_file_path (Path): The path where the Parquet file will be saved.
        dataset_path (Path): The path to the Open Food Facts JSONL dataset.
        schema (pa.Schema): The schema of the Parquet file.
        batch_size (int, optional): The size of the batches used to convert the
            dataset. Defaults to 1024.
    """
    writer = None
    DTYPE_MAP = {
        "images": IMAGES_DATATYPE,
        "nutriments": NUTRIMENTS_DATATYPE,
        "packagings": PACKAGING_FIELD_DATATYPE,
    }
    for batch in chunked(jsonl_iter(dataset_path), batch_size):
        # We use by_alias=True because some fields start with a digit
        # (ex: nutriments.100g), and we cannot declare the schema with
        # Pydantic without an alias.
        products = [Product(**item).model_dump(by_alias=True) for item in batch]
        keys = products[0].keys()
        data = {
            key: pa.array(
                [product[key] for product in products],
                # Don't let pyarrow guess type for complex types
                type=DTYPE_MAP.get(key, None),
            )
            for key in keys
        }
        record_batch = pa.record_batch(data, schema=schema)
        if writer is None:
            writer = pq.ParquetWriter(output_file_path, schema=record_batch.schema)
        writer.write_batch(record_batch, row_group_size=row_group_size)

    if writer is not None:
        writer.close()


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
        path_in_repo="food.parquet",
        commit_message=commit_message,
    )
    logger.info("Data succesfully pushed to Hugging Face at %s", repo_id)
