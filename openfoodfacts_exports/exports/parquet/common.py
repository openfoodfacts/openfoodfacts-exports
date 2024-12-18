import logging
from pathlib import Path

import pyarrow as pa
from huggingface_hub import HfApi
from pydantic import BaseModel, Field, model_validator

logger = logging.getLogger(__name__)


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
    rev: int | None = None
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


class LanguageField(BaseModel):
    lang: str
    text: str


class OwnerField(BaseModel):
    field_name: str
    timestamp: int


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
    _LANGUAGE_FIELDS = ["product_name", "generic_name"]

    brands_tags: list[str] | None = None
    brands: str | None = None
    categories: str | None = None
    categories_tags: list[str] | None = None
    checkers_tags: list[str] | None = None
    cities_tags: list[str] | None = None
    code: str
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
    editors: list[str] | None = None
    entry_dates_tags: list[str] | None = None
    generic_name: list[LanguageField] | None = None
    images: list[Image] | None = None
    informers_tags: list[str] | None = None
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
    misc_tags: list[str] | None = None
    obsolete: bool = False
    origins_tags: list[str] | None = None
    origins: str | None = None
    owner: str | None = None
    owner_fields: list[OwnerField] | None = None
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
    states_tags: list[str] | None = None
    stores_tags: list[str] | None = None
    stores: str | None = None
    unique_scans_n: int | None = None

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

    @classmethod
    def get_language_fields(cls) -> list[str]:
        return ["product_name", "generic_name"]

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
        for field_name in cls.get_language_fields():
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
        `rev`, `sizes`, `uploaded_t`, and `uploader` keys.

        In Open Food Facts, images are stored as a dictionary with the image
        key as the key and the image data as the value.

        To make the schema compatible with Parquet, we convert these fields
        into a list of dictionaries with `key`, `imgid`, `rev`, `sizes`, `uploaded_t`,
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
    def parse_owner_fields(cls, data: dict):
        owner_fields = data.pop("owner_fields", None)
        if owner_fields:
            data["owner_fields"] = [
                {"field_name": key, "timestamp": value}
                for key, value in owner_fields.items()
            ]
        return data


PA_IMAGE_SIZE_DATATYPE = pa.struct(
    [
        pa.field("h", pa.int32(), nullable=True),
        pa.field("w", pa.int32(), nullable=True),
    ]
)

PA_IMAGES_DATATYPE = pa.list_(
    pa.struct(
        [
            pa.field("key", pa.string(), nullable=True),
            pa.field("imgid", pa.int32(), nullable=True),
            pa.field("rev", pa.int32(), nullable=True),
            pa.field(
                "sizes",
                pa.struct(
                    [
                        pa.field("100", PA_IMAGE_SIZE_DATATYPE, nullable=True),
                        pa.field("200", PA_IMAGE_SIZE_DATATYPE, nullable=True),
                        pa.field("400", PA_IMAGE_SIZE_DATATYPE, nullable=True),
                        pa.field("full", PA_IMAGE_SIZE_DATATYPE, nullable=True),
                    ]
                ),
                nullable=True,
            ),
            pa.field("uploaded_t", pa.int64(), nullable=True),
            pa.field("uploader", pa.string(), nullable=True),
        ]
    )
)

PA_INGREDIENTS_TEXT_DATATYPE = pa.list_(
    pa.struct(
        [
            pa.field("lang", pa.string()),
            pa.field("text", pa.string()),
        ],
    )
)


PA_LANGUAGE_FIELD_DATATYPE = pa.list_(
    pa.struct(
        [
            pa.field("lang", pa.string()),
            pa.field("text", pa.string()),
        ]
    ),
)

PA_NUTRIMENTS_DATATYPE = pa.list_(
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

PA_PACKAGING_FIELD_DATATYPE = pa.list_(
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

PA_OWNER_FIELD_DATATYPE = pa.list_(
    pa.struct(
        [
            pa.field("field_name", pa.string()),
            pa.field("timestamp", pa.int64()),
        ]
    )
)


def push_parquet_file_to_hf(
    data_path: Path,
    repo_id: str,
    revision: str = "main",
    commit_message: str = "Database updated",
) -> None:
    """Push a Parquet file to Hugging Face Hub.

    Args:
        data_path (Path): The path to the Parquet file to push. The name of the
            file will be used as the path in the repository.
        repo_id (str, optional): The repository ID on Hugging Face Hub.
        revision (str, optional): The revision to push the data to. Defaults to
            "main".
        commit_message (str, optional): The commit message. Defaults to
            "Database updated".
    """
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
        path_in_repo=data_path.name,
        commit_message=commit_message,
    )
    logger.info("Data succesfully pushed to Hugging Face at %s", repo_id)
