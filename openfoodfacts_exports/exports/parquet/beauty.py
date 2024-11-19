import orjson
import pyarrow as pa
from pydantic import Field, field_serializer

from .common import (
    PA_IMAGES_DATATYPE,
    PA_LANGUAGE_FIELD_DATATYPE,
    PA_OWNER_FIELD_DATATYPE,
    PA_PACKAGING_FIELD_DATATYPE,
    Ingredient,
    LanguageField,
    Product,
)


class BeautyProduct(Product):
    additives_n: int | None = None
    additives_tags: list[str] | None = None
    allergens_tags: list[str] | None = None
    emb_codes_tags: list[str] | None = None
    emb_codes: str | None = None
    ingredients_analysis_tags: list[str] | None = None
    ingredients_from_palm_oil_n: int | None = None
    ingredients_n: int | None = None
    ingredients_original_tags: list[str] | None = None
    ingredients_percent_analysis: int | None = None
    ingredients_tags: list[str] | None = None
    ingredients_text: list[LanguageField] | None = None
    ingredients_with_specified_percent_n: int | None = None
    ingredients_with_unspecified_percent_n: int | None = None
    ingredients: list[Ingredient] | None = None
    known_ingredients_n: int | None = None
    minerals_tags: list[str] | None = None
    nucleotides_tags: list[str] | None = None
    nutrient_levels_tags: list[str] | None = None
    nutrition_data_per: str | None = None
    serving_quantity: str | None = Field(default=None, coerce_numbers_to_str=True)
    serving_size: str | None = None
    traces_tags: list[str] | None = None
    unknown_ingredients_n: int | None = None
    unknown_nutrients_tags: list[str] | None = None
    vitamins_tags: list[str] | None = None

    @classmethod
    def get_language_fields(cls) -> list[str]:
        return [
            "ingredients_text",
            "product_name",
            "packaging_text",
            "generic_name",
        ]

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


BEAUTY_PRODUCT_SCHEMA = pa.schema(
    [
        pa.field("additives_n", pa.int32(), nullable=True),
        pa.field("additives_tags", pa.list_(pa.string()), nullable=True),
        pa.field("allergens_tags", pa.list_(pa.string()), nullable=True),
        pa.field("brands_tags", pa.list_(pa.string()), nullable=True),
        pa.field("brands", pa.string(), nullable=True),
        pa.field("categories", pa.string(), nullable=True),
        pa.field("categories_tags", pa.list_(pa.string()), nullable=True),
        pa.field("checkers_tags", pa.list_(pa.string()), nullable=True),
        pa.field("cities_tags", pa.list_(pa.string()), nullable=True),
        pa.field("code", pa.string()),
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
        pa.field("editors", pa.list_(pa.string()), nullable=True),
        pa.field("emb_codes_tags", pa.list_(pa.string()), nullable=True),
        pa.field("emb_codes", pa.string(), nullable=True),
        pa.field("entry_dates_tags", pa.list_(pa.string()), nullable=True),
        pa.field("generic_name", PA_LANGUAGE_FIELD_DATATYPE, nullable=True),
        pa.field("images", PA_IMAGES_DATATYPE, nullable=True),
        pa.field("informers_tags", pa.list_(pa.string()), nullable=True),
        pa.field("ingredients_analysis_tags", pa.list_(pa.string()), nullable=True),
        pa.field("ingredients_from_palm_oil_n", pa.int32(), nullable=True),
        pa.field("ingredients_n", pa.int32(), nullable=True),
        pa.field("ingredients_original_tags", pa.list_(pa.string()), nullable=True),
        pa.field("ingredients_percent_analysis", pa.int32(), nullable=True),
        pa.field("ingredients_tags", pa.list_(pa.string()), nullable=True),
        pa.field("ingredients_text", PA_LANGUAGE_FIELD_DATATYPE, nullable=True),
        pa.field("ingredients_with_specified_percent_n", pa.int32(), nullable=True),
        pa.field("ingredients_with_unspecified_percent_n", pa.int32(), nullable=True),
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
        pa.field("nucleotides_tags", pa.list_(pa.string()), nullable=True),
        pa.field("nutrient_levels_tags", pa.list_(pa.string()), nullable=True),
        pa.field("nutrition_data_per", pa.string(), nullable=True),
        pa.field("obsolete", pa.bool_()),
        pa.field("origins_tags", pa.list_(pa.string()), nullable=True),
        pa.field("origins", pa.string(), nullable=True),
        pa.field("owner_fields", PA_OWNER_FIELD_DATATYPE, nullable=True),
        pa.field("owner", pa.string(), nullable=True),
        pa.field("packagings_complete", pa.bool_(), nullable=True),
        pa.field("packaging_recycling_tags", pa.list_(pa.string()), nullable=True),
        pa.field("packaging_shapes_tags", pa.list_(pa.string()), nullable=True),
        pa.field("packaging_tags", pa.list_(pa.string()), nullable=True),
        pa.field("packaging_text", PA_LANGUAGE_FIELD_DATATYPE, nullable=True),
        pa.field("packaging", pa.string(), nullable=True),
        pa.field("packagings", PA_PACKAGING_FIELD_DATATYPE, nullable=True),
        pa.field("photographers", pa.list_(pa.string()), nullable=True),
        pa.field("popularity_key", pa.int64(), nullable=True),
        pa.field("popularity_tags", pa.list_(pa.string()), nullable=True),
        pa.field("product_name", PA_LANGUAGE_FIELD_DATATYPE, nullable=True),
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
    ]
)


BEAUTY_DTYPE_MAP = {
    "images": PA_IMAGES_DATATYPE,
    "packagings": PA_PACKAGING_FIELD_DATATYPE,
}
