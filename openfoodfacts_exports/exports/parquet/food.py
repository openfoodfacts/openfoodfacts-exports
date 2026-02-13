import orjson
import pyarrow as pa
from pydantic import Field, field_serializer, model_validator
from openfoodfacts.types import JSONType

from openfoodfacts.types import NutritionV3
from .common import (
    PA_CATEGORIES_PROPERTIES_DATATYPE,
    PA_IMAGES_DATATYPE,
    PA_LANGUAGE_FIELD_DATATYPE,
    PA_NUTRIMENTS_DATATYPE,
    PA_OWNER_FIELD_DATATYPE,
    PA_PACKAGING_FIELD_DATATYPE,
    CategoriesProperties,
    Ingredient,
    LanguageField,
    NutrimentField,
    Product,
)


class FoodProduct(Product):
    additives_n: int | None = None
    additives_tags: list[str] | None = None
    allergens_tags: list[str] | None = None
    categories_properties: CategoriesProperties | None = None
    ciqual_food_name_tags: list[str] | None = None
    compared_to_category: str | None = None
    ecoscore_data: dict | None = None
    ecoscore_grade: str | None = None
    ecoscore_score: int | None = None
    ecoscore_tags: list[str] | None = None
    emb_codes_tags: list[str] | None = None
    emb_codes: str | None = None
    food_groups_tags: list[str] | None = None
    ingredients_analysis_tags: list[str] | None = None
    ingredients_from_palm_oil_n: int | None = None
    ingredients_n: int | None = None
    ingredients_original_tags: list[str] | None = None
    ingredients_percent_analysis: int | None = None
    ingredients_tags: list[str] | None = None
    ingredients_text: list[LanguageField] | None = None
    ingredients_with_specified_percent_n: int | None = None
    ingredients_with_unspecified_percent_n: int | None = None
    ingredients_without_ciqual_codes_n: int | None = None
    ingredients_without_ciqual_codes: list[str] | None = None
    ingredients: list[Ingredient] | None = None
    known_ingredients_n: int | None = None
    minerals_tags: list[str] | None = None
    no_nutrition_data: bool | None = None
    new_additives_n: int | None = None
    nova_group: int | None = None
    nova_groups_tags: list[str] | None = None
    nova_groups: str | None = None
    nucleotides_tags: list[str] | None = None
    nutrient_levels_tags: list[str] | None = None
    nutriments: list[NutrimentField] | None = None
    nutriscore_grade: str | None = None
    nutriscore_score: int | None = None
    nutrition_data_per: str | None = None
    serving_quantity: str | None = Field(default=None, coerce_numbers_to_str=True)
    serving_size: str | None = None
    traces_tags: list[str] | None = None
    unknown_ingredients_n: int | None = None
    unknown_nutrients_tags: list[str] | None = None
    vitamins_tags: list[str] | None = None
    with_non_nutritive_sweeteners: int | None = None
    with_sweeteners: int | None = None

    @classmethod
    def get_language_fields(cls) -> list[str]:
        return [
            "ingredients_text",
            "product_name",
            "packaging_text",
            "generic_name",
        ]

    @model_validator(mode="before")
    @classmethod
    def parse_nutriments(cls, data: dict):
        schema_version = data.get("schema_version", 999)
        if schema_version < 1003:
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
            nutrition = NutritionV3.model_validate(data.get("nutrition", {}))
            aggregated_set = nutrition.aggregated_set
            if aggregated_set:
                per = aggregated_set.per
                preparation = aggregated_set.preparation
                nutriments = []
                for (
                    nutrient_name,
                    nutrient_data,
                ) in aggregated_set.nutrients.items():
                    item: JSONType = {
                        "name": nutrient_name,
                        # value and prepared_value are always kept null here (contrary
                        # to product with schema version < 1003), as they're ambiguous
                        "value": None,
                        "prepared_value": None,
                        # The rest of the fields are populated if they're found, from
                        # the aggregated set
                        "100g": None,
                        "serving": None,
                        "unit": None,
                        "prepared_100g": None,
                        "prepared_serving": None,
                        "prepared_unit": None,
                    }
                    if preparation == "as_sold":
                        key = "100g" if per in ("100g", "100ml") else "serving"
                        item[key] = nutrient_data.value
                        item["unit"] = nutrient_data.unit
                    else:
                        key = (
                            "prepared_100g"
                            if per in ("100g", "100ml")
                            else "prepared_serving"
                        )
                        item[key] = nutrient_data.value
                        item["prepared_unit"] = nutrient_data.unit

                    nutriments.append(item)

                data["nutriments"] = nutriments

        # Make sure that the `nutriments` field is present in `data`
        data.setdefault("nutriments", None)
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

    @model_validator(mode="before")
    @classmethod
    def parse_nova_groups(cls, data: dict):
        """It seems some nova_groups can be int, but str expected."""
        nova_groups = data.get("nova_groups")
        if nova_groups and isinstance(nova_groups, int):
            data["nova_groups"] = str(nova_groups)
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

    @field_serializer("ecoscore_data")
    def serialize_ecoscore_data(self, ecoscore_data: dict | None, _info) -> str | None:
        """Ecoscore data is a complex structure, leave it as a JSON string for
        now."""
        if ecoscore_data is None:
            return None
        return orjson.dumps(ecoscore_data).decode("utf-8")


FOOD_PRODUCT_SCHEMA = pa.schema(
    [
        pa.field("additives_n", pa.int32(), nullable=True),
        pa.field("additives_tags", pa.list_(pa.string()), nullable=True),
        pa.field("allergens_tags", pa.list_(pa.string()), nullable=True),
        pa.field("brands_tags", pa.list_(pa.string()), nullable=True),
        pa.field("brands", pa.string(), nullable=True),
        pa.field("categories", pa.string(), nullable=True),
        pa.field("categories_tags", pa.list_(pa.string()), nullable=True),
        pa.field("categories_properties", PA_CATEGORIES_PROPERTIES_DATATYPE),
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
        pa.field("ecoscore_data", pa.string(), nullable=True),
        pa.field("ecoscore_grade", pa.string(), nullable=True),
        pa.field("ecoscore_score", pa.int32(), nullable=True),
        pa.field("ecoscore_tags", pa.list_(pa.string()), nullable=True),
        pa.field("editors", pa.list_(pa.string()), nullable=True),
        pa.field("emb_codes_tags", pa.list_(pa.string()), nullable=True),
        pa.field("emb_codes", pa.string(), nullable=True),
        pa.field("entry_dates_tags", pa.list_(pa.string()), nullable=True),
        pa.field("food_groups_tags", pa.list_(pa.string()), nullable=True),
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
        pa.field("nutriments", PA_NUTRIMENTS_DATATYPE, nullable=True),
        pa.field("nutriscore_grade", pa.string(), nullable=True),
        pa.field("nutriscore_score", pa.int32(), nullable=True),
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
        pa.field("with_non_nutritive_sweeteners", pa.int32(), nullable=True),
        pa.field("with_sweeteners", pa.int32(), nullable=True),
        pa.field("schema_version", pa.int32(), nullable=True),
    ]
)


FOOD_DTYPE_MAP = {
    "images": PA_IMAGES_DATATYPE,
    "nutriments": PA_NUTRIMENTS_DATATYPE,
    "packagings": PA_PACKAGING_FIELD_DATATYPE,
    "categories_properties": PA_CATEGORIES_PROPERTIES_DATATYPE,
}
