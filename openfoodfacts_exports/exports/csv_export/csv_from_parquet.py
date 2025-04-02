from datetime import datetime
from pathlib import Path

import polars as pl

from .processes import (
    process_url,
    process_tags,
    process_lang,
    process_text,
    process_image_url,
    process_nutrients,
)


def export_parquet_to_csv(parquet_path: Path, csv_path: Path, lang: str) -> None:
    df = pl.scan_parquet(parquet_path).select(
        [
            pl.col("code"),
            pl.col("code")
            .alias("url")
            .map_elements(
                lambda x: process_url(code=x, lang=lang),
                return_dtype=pl.String,
            ),
            pl.col("creator"),
            pl.col("created_t"),
            pl.col("created_t")
            .alias("created_datetime")
            .map_elements(datetime.fromtimestamp, return_dtype=pl.Datetime),
            pl.col("last_modified_t"),
            pl.col("last_modified_t")
            .alias("last_modified_datetime")
            .map_elements(datetime.fromtimestamp, return_dtype=pl.Datetime),
            pl.col("last_modified_by"),
            pl.col("last_updated_t"),
            pl.col("last_updated_t")
            .alias("last_updated_datetime")
            .map_elements(datetime.fromtimestamp, return_dtype=pl.Datetime),
            pl.col("product_name").map_elements(
                lambda x: process_text(elts=x, lang=lang),
                return_dtype=pl.String,
            ),
            pl.col("generic_name").map_elements(
                lambda x: process_text(elts=x, lang=lang),
                return_dtype=pl.String,
            ),
            pl.col("quantity"),
            pl.col("packaging"),
            pl.col("packaging_tags").map_elements(process_tags, return_dtype=pl.String),
            pl.col("packaging_tags")
            .alias(f"packaging_{lang}")
            .map_elements(
                lambda x: process_lang(tags=x, lang=lang),
                return_dtype=pl.String,
            ),
            pl.col("packaging_text").map_elements(
                lambda x: process_text(elts=x, lang=lang),
                return_dtype=pl.String,
            ),
            pl.col("brands"),
            pl.col("brands_tags").map_elements(process_tags, return_dtype=pl.String),
            pl.col("categories"),
            pl.col("categories_tags").map_elements(
                process_tags, return_dtype=pl.String
            ),
            pl.col("categories_tags")
            .alias(f"categories_{lang}")
            .map_elements(
                lambda x: process_lang(tags=x, lang=lang),
                return_dtype=pl.String,
            ),
            pl.col("origins"),
            pl.col("origins_tags").map_elements(process_tags, return_dtype=pl.String),
            pl.col("origins_tags")
            .alias(f"origins_{lang}")
            .map_elements(
                lambda x: process_lang(tags=x, lang=lang),
                return_dtype=pl.String,
            ),
            pl.col("manufacturing_places"),
            pl.col("manufacturing_places_tags").map_elements(
                process_tags, return_dtype=pl.String
            ),
            pl.col("labels"),
            pl.col("labels_tags").map_elements(process_tags, return_dtype=pl.String),
            pl.col("labels_tags")
            .alias(f"labels_{lang}")
            .map_elements(
                lambda x: process_lang(tags=x, lang=lang),
                return_dtype=pl.String,
            ),
            pl.col("emb_codes"),
            pl.col("emb_codes_tags").map_elements(process_tags, return_dtype=pl.String),
            pl.col("cities_tags").map_elements(process_tags, return_dtype=pl.String),
            pl.col("purchase_places_tags")
            .alias("purchase_places")
            .map_elements(process_tags, return_dtype=pl.String),
            pl.col("stores"),
            # NOTE: Decided to not add it, the field doesn't have a clear structure in the CSV
            # pl.col("countries_tags")
            # .alias("countries")
            # .map_elements(lambda x: process_lang(x, lang=lang), return_dtype=pl.String),
            pl.col("countries_tags").map_elements(process_tags, return_dtype=pl.String),
            pl.col("countries_tags")
            .alias(f"countries_{lang}")
            .map_elements(
                lambda x: process_lang(tags=x, lang=lang),
                return_dtype=pl.String,
            ),
            pl.col("ingredients_text").map_elements(
                lambda x: process_text(elts=x, lang=lang),
                return_dtype=pl.String,
            ),
            pl.col("ingredients_tags").map_elements(
                process_tags, return_dtype=pl.String
            ),
            pl.col("ingredients_analysis_tags").map_elements(
                process_tags, return_dtype=pl.String
            ),
            pl.col("allergens_tags")
            .alias("allergens")
            .map_elements(process_tags, return_dtype=pl.String),
            pl.col("allergens_tags")
            .alias(f"allergens_{lang}")
            .map_elements(
                lambda x: process_lang(tags=x, lang=lang),
                return_dtype=pl.String,
            ),
            pl.col("traces_tags")
            .alias("traces")
            .map_elements(process_tags, return_dtype=pl.String),
            pl.col("traces_tags").map_elements(process_tags, return_dtype=pl.String),
            pl.col("traces_tags")
            .alias(f"traces_{lang}")
            .map_elements(
                lambda x: process_lang(tags=x, lang=lang),
                return_dtype=pl.String,
            ),
            pl.col("serving_size"),
            pl.col("serving_quantity"),
            pl.col("no_nutrition_data"),
            pl.col("additives_n"),
            pl.col("additives_tags").map_elements(process_tags, return_dtype=pl.String),
            pl.col("additives_tags")
            .alias(f"additives_{lang}")
            .map_elements(
                lambda x: process_lang(tags=x, lang=lang),
                return_dtype=pl.String,
            ),
            pl.col("nutriscore_score"),
            pl.col("nutriscore_grade"),
            pl.col("nova_group"),
            pl.col("food_groups_tags")
            .alias("food_groups")
            .map_elements(process_tags, return_dtype=pl.String),
            pl.col("food_groups_tags").map_elements(
                process_tags, return_dtype=pl.String
            ),
            pl.col("food_groups_tags")
            .alias(f"food_groups_{lang}")
            .map_elements(
                lambda x: process_lang(tags=x, lang=lang),
                return_dtype=pl.String,
            ),
            pl.col("states_tags")
            .alias("states")
            .map_elements(process_tags, return_dtype=pl.String),
            pl.col("states_tags").map_elements(process_tags, return_dtype=pl.String),
            pl.col("states_tags")
            .alias(f"states_{lang}")
            .map_elements(
                lambda x: process_lang(tags=x, lang=lang),
                return_dtype=pl.String,
            ),
            pl.col("ecoscore_score"),
            pl.col("ecoscore_grade"),
            pl.col("nutrient_levels_tags").map_elements(
                process_tags, return_dtype=pl.String
            ),
            pl.col("product_quantity"),
            pl.col("owner"),
            pl.col("data_quality_errors_tags").map_elements(
                process_tags, return_dtype=pl.String
            ),
            pl.col("unique_scans_n").cast(pl.Int32),
            pl.col("popularity_tags").map_elements(
                process_tags, return_dtype=pl.String
            ),
            pl.col("completeness"),
            pl.col("last_image_t"),
            pl.col("last_image_t")
            .alias("last_image_datetime")
            .map_elements(datetime.fromtimestamp, return_dtype=pl.Datetime),
            # NOTE: Unclear what is the main category
            # pl.col("categories_tags")
            # .alias("main_category")
            # .map_elements(
            #     process_last_item_from_array,
            #     return_dtype=pl.String,
            #     skip_nulls=True,
            # ),
            pl.struct(["images", "code"])
            .map_elements(
                lambda combined: process_image_url(
                    images=combined["images"],
                    code=combined["code"],
                    lang=lang,
                    image_key="front",
                    size="400",
                ),
                return_dtype=pl.String,
            )
            .alias("image_url"),
            pl.struct(["images", "code"])
            .map_elements(
                lambda combined: process_image_url(
                    images=combined["images"],
                    code=combined["code"],
                    lang=lang,
                    image_key="front",
                    size="200",
                ),
                return_dtype=pl.String,
            )
            .alias("image_small_url"),
            pl.struct(["images", "code"])
            .map_elements(
                lambda combined: process_image_url(
                    images=combined["images"],
                    code=combined["code"],
                    lang=lang,
                    image_key="ingredients",
                    size="400",
                ),
                return_dtype=pl.String,
            )
            .alias("image_ingredients_url"),
            pl.struct(["images", "code"])
            .map_elements(
                lambda combined: process_image_url(
                    images=combined["images"],
                    code=combined["code"],
                    lang=lang,
                    image_key="ingredients",
                    size="200",
                ),
                return_dtype=pl.String,
            )
            .alias("image_ingredients_small_url"),
            pl.struct(["images", "code"])
            .map_elements(
                lambda combined: process_image_url(
                    images=combined["images"],
                    code=combined["code"],
                    lang=lang,
                    image_key="nutrition",
                    size="400",
                ),
                return_dtype=pl.String,
            )
            .alias("image_nutrition_url"),
            pl.struct(["images", "code"])
            .map_elements(
                lambda combined: process_image_url(
                    images=combined["images"],
                    code=combined["code"],
                    lang=lang,
                    image_key="nutrition",
                    size="200",
                ),
                return_dtype=pl.String,
            )
            .alias("image_nutrition_small_url"),
            pl.col("nutriments")
            .alias("energy-kj_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="energy-kj"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("energy-kcal_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="energy-kcal"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("energy_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="energy"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("energy-from-fat_100g")
            .map_elements(
                lambda x: process_nutrients(
                    nutrients=x, nutrient_name="energy-from-fat"
                ),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("fat_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="fat"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("saturated-fat_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="saturated-fat"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("butyric-acid_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="butyric-acid"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("caproic-acid_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="caproic-acid"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("caprylic-acid_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="caprylic-acid"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("capric-acid_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="capric-acid"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("lauric-acid_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="lauric-acid"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("myristic-acid_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="myristic-acid"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("palmitic-acid_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="palmitic-acid"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("stearic-acid_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="stearic-acid"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("arachidic-acid_100g")
            .map_elements(
                lambda x: process_nutrients(
                    nutrients=x, nutrient_name="arachidic-acid"
                ),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("behenic-acid_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="behenic-acid"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("lignoceric-acid_100g")
            .map_elements(
                lambda x: process_nutrients(
                    nutrients=x, nutrient_name="lignoceric-acid"
                ),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("cerotic-acid_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="cerotic-acid"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("montanic-acid_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="montanic-acid"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("melissic-acid_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="melissic-acid"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("unsaturated-fat_100g")
            .map_elements(
                lambda x: process_nutrients(
                    nutrients=x, nutrient_name="unsaturated-fat"
                ),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("monounsaturated-fat_100g")
            .map_elements(
                lambda x: process_nutrients(
                    nutrients=x, nutrient_name="monounsaturated-fat"
                ),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("omega-9-fat_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="omega-9-fat"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("polyunsaturated-fat_100g")
            .map_elements(
                lambda x: process_nutrients(
                    nutrients=x, nutrient_name="polyunsaturated-fat"
                ),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("omega-3-fat_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="omega-3-fat"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("omega-6-fat_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="omega-6-fat"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("alpha-linolenic-acid_100g")
            .map_elements(
                lambda x: process_nutrients(
                    nutrients=x, nutrient_name="alpha-linolenic-acid"
                ),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("eicosapentaenoic-acid_100g")
            .map_elements(
                lambda x: process_nutrients(
                    nutrients=x, nutrient_name="eicosapentaenoic-acid"
                ),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("docosahexaenoic-acid_100g")
            .map_elements(
                lambda x: process_nutrients(
                    nutrients=x, nutrient_name="docosahexaenoic-acid"
                ),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("linoleic-acid_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="linoleic-acid"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("arachidonic-acid_100g")
            .map_elements(
                lambda x: process_nutrients(
                    nutrients=x, nutrient_name="arachidonic-acid"
                ),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("gamma-linolenic-acid_100g")
            .map_elements(
                lambda x: process_nutrients(
                    nutrients=x, nutrient_name="gamma-linolenic-acid"
                ),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("dihomo-gamma-linolenic-acid_100g")
            .map_elements(
                lambda x: process_nutrients(
                    nutrients=x, nutrient_name="dihomo-gamma-linolenic-acid"
                ),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("oleic-acid_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="oleic-acid"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("elaidic-acid_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="elaidic-acid"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("gondoic-acid_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="gondoic-acid"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("mead-acid_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="mead-acid"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("erucic-acid_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="erucic-acid"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("nervonic-acid_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="nervonic-acid"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("trans-fat_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="trans-fat"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("cholesterol_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="cholesterol"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("carbohydrates_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="carbohydrates"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("sugars_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="sugars"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("added-sugars_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="added-sugars"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("sucrose_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="sucrose"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("glucose_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="glucose"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("fructose_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="fructose"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("lactose_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="lactose"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("maltose_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="maltose"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("maltodextrins_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="maltodextrins"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("starch_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="starch"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("polyols_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="polyols"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("erythritol_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="erythritol"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("fiber_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="fiber"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("soluble-fiber_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="soluble-fiber"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("insoluble-fiber_100g")
            .map_elements(
                lambda x: process_nutrients(
                    nutrients=x, nutrient_name="insoluble-fiber"
                ),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("proteins_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="proteins"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("casein_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="casein"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("serum-proteins_100g")
            .map_elements(
                lambda x: process_nutrients(
                    nutrients=x, nutrient_name="serum-proteins"
                ),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("nucleotides_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="nucleotides"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("salt_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="salt"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("added-salt_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="added-salt"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("sodium_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="sodium"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("alcohol_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="alcohol"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("vitamin-a_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="vitamin-a"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("beta-carotene_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="beta-carotene"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("vitamin-d_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="vitamin-d"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("vitamin-e_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="vitamin-e"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("vitamin-k_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="vitamin-k"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("vitamin-c_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="vitamin-c"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("vitamin-b1_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="vitamin-b1"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("vitamin-b2_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="vitamin-b2"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("vitamin-pp_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="vitamin-pp"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("vitamin-b6_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="vitamin-b6"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("vitamin-b9_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="vitamin-b9"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("folates_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="folates"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("vitamin-b12_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="vitamin-b12"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("biotin_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="biotin"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("pantothenic-acid_100g")
            .map_elements(
                lambda x: process_nutrients(
                    nutrients=x, nutrient_name="pantothenic-acid"
                ),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("silica_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="silica"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("bicarbonate_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="bicarbonate"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("potassium_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="potassium"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("chloride_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="chloride"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("calcium_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="calcium"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("phosphorus_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="phosphorus"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("iron_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="iron"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("magnesium_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="magnesium"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("zinc_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="zinc"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("copper_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="copper"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("manganese_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="manganese"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("fluoride_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="fluoride"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("selenium_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="selenium"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("chromium_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="chromium"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("molybdenum_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="molybdenum"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("iodine_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="iodine"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("caffeine_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="caffeine"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("taurine_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="taurine"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("ph_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="ph"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("fruits-vegetables-nuts_100g")
            .map_elements(
                lambda x: process_nutrients(
                    nutrients=x, nutrient_name="fruits-vegetables-nuts"
                ),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("fruits-vegetables-nuts-dried_100g")
            .map_elements(
                lambda x: process_nutrients(
                    nutrients=x, nutrient_name="fruits-vegetables-nuts-dried"
                ),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("fruits-vegetables-nuts-estimate_100g")
            .map_elements(
                lambda x: process_nutrients(
                    nutrients=x, nutrient_name="fruits-vegetables-nuts-estimate"
                ),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("fruits-vegetables-nuts-estimate-from-ingredients_100g")
            .map_elements(
                lambda x: process_nutrients(
                    nutrients=x,
                    nutrient_name="fruits-vegetables-nuts-estimate-from-ingredients",
                ),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("collagen-meat-protein-ratio_100g")
            .map_elements(
                lambda x: process_nutrients(
                    nutrients=x, nutrient_name="collagen-meat-protein-ratio"
                ),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("cocoa_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="cocoa"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("chlorophyl_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="chlorophyl"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("carbon-footprint_100g")
            .map_elements(
                lambda x: process_nutrients(
                    nutrients=x, nutrient_name="carbon-footprint"
                ),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("carbon-footprint-from-meat-or-fish_100g")
            .map_elements(
                lambda x: process_nutrients(
                    nutrients=x, nutrient_name="carbon-footprint-from-meat-or-fish"
                ),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("nutrition-score-fr_100g")
            .map_elements(
                lambda x: process_nutrients(
                    nutrients=x, nutrient_name="nutrition-score-fr"
                ),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("nutrition-score-uk_100g")
            .map_elements(
                lambda x: process_nutrients(
                    nutrients=x, nutrient_name="nutrition-score-uk"
                ),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("glycemic-index_100g")
            .map_elements(
                lambda x: process_nutrients(
                    nutrients=x, nutrient_name="glycemic-index"
                ),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("water-hardness_100g")
            .map_elements(
                lambda x: process_nutrients(
                    nutrients=x, nutrient_name="water-hardness"
                ),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("choline_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="choline"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("phylloquinone_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name=""),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("beta-glucan_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="beta-glucan"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("inositol_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="inositol"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("carnitine_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="carnitine"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("sulphate_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="sulphate"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("nitrate_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="nitrate"),
                return_dtype=pl.Float32,
            ),
            pl.col("nutriments")
            .alias("acidity_100g")
            .map_elements(
                lambda x: process_nutrients(nutrients=x, nutrient_name="acidity"),
                return_dtype=pl.Float32,
            ),
        ]
    )

    df.sink_csv(csv_path)
