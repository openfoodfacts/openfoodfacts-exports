from datetime import datetime

import pyarrow.parquet as pq
import polars as pl

from .processes import (
    process_url,
    process_tags,
    process_lang,
    process_text,
)

LANG = "en"
BATCH_SIZE = 10000
PARQUET_PATH = "/home/jeremy/projects/openfoodfacts/data/food.parquet"

LANG_FIELDS = [
    "packaging",
    "categories",
    "origins",
    "labels",
    "countries", # Not clear what is this exactly. Duplicate with tags (Remove?)
    "allergens",
    "traces",
    "additives",
    "food_groups",
    "states",
    "main_category",  # missing in Parquet, hard to define ???
    "url",  # Composed with code: │ http://fr.openfoodfacts.org/produit/9999999999994/ or http://world.openfoodfacts.org/product/9999999999994/
]

FIELDS = [
    "code",
    # "url",
    "creator",
    "created_t",
    # "created_datetime", # created_t into created_datetime
    "last_modified_t",
    # "last_modified_datetime", # same
    "last_modified_by",
    "last_updated_t",
    # "last_updated_datetime", # same
    "product_name",
    "abbreviated_product_name",  # Haven't figured out the process. Just take the product_name, which is a struct with main and lang (Removed for now)
    "generic_name",
    "quantity",
    "packaging",
    "packaging_tags",
    "packaging_text",
    "brands",
    "brands_tags",
    "categories",
    "categories_tags",
    "origins",
    "origins_tags",
    "manufacturing_places",
    "manufacturing_places_tags",
    "labels",
    "labels_tags",
    "emb_codes",
    "emb_codes_tags",
    # "first_packaging_code_geo", # Don't know where to find it
    # "cities", # Just empty, remove it
    "cities_tags",
    "purchase_places",  # purchase_places_tags
    "stores",
    "countries",  # = countries_tags
    "countries_tags",
    "ingredients_text",
    "ingredients_tags",
    "ingredients_analysis_tags",
    "allergens",  # allergens_tags
    "traces",  # trace_tags
    "traces_tags",
    "serving_size",
    "serving_quantity",
    "no_nutrition_data",
    "additives_n",
    "additives",  # additives_tags
    "additives_tags",
    "nutriscore_score",
    "nutriscore_grade",
    "nova_group",
    "pnns_groups_1",  # missing
    "pnns_groups_2",  # missing
    "food_groups",  # food_group_tags
    "food_groups_tags",
    "states",  # states_tags
    "states_tags",
    "brand_owner",  # WHat is this ????
    "ecoscore_score",
    "ecoscore_grade",
    "nutrient_levels_tags",
    "product_quantity",
    "owner",
    "data_quality_errors_tags",
    "unique_scans_n",
    "popularity_tags",
    "completeness",
    "last_image_t",
    "last_image_datetime",  # from last_image_t
    "main_category",  # Seems to be the last element of categories_tags
    "image_url",
    "image_small_url",  # small correspond to image size
    "image_ingredients_url",
    "image_ingredients_small_url",
    "image_nutrition_url",  # careful lang 'nutrition_lg'
    "image_nutrition_small_url",
    "energy_kj_100g",
    "energy_kcal_100g",
    "energy_100g",
    "fat_100g",
    "saturated_fat_100g",  # in nutriments
    "butyric_acid_100g",
    "caproic_acid_100g",
    "caprylic_acid_100g",
    "capric_acid_100g",
    "lauric_acid_100g",
    "myristic_acid_100g",
    "palmitic_acid_100g",
    "stearic_acid_100g",
    "arachidic_acid_100g",
    "behenic_acid_100g",
    "lignoceric_acid_100g",
    "cerotic_acid_100g",
    "montanic_acid_100g",
    "melissic_acid_100g",
    "unsaturated_fat_100g",
    "monounsaturated_fat_100g",
    "polyunsaturated_fat_100g",
    "eicosapentaenoic_acid_100g",
    "docosahexaenoic_acid_100g",
    "linoleic_acid_100g",
    "arachidonic_acid_100g",
    "oleic_acid_100g",
    "elaidic_acid_100g",
    "gondoic_acid_100g",
    "mead_acid_100g",
    "erucic_acid_100g",
    "nervonic_acid_100g",
    "trans_fat_100g",
    "cholesterol_100g",
    "carbohydrates_100g",
    "sugars_100g",
    "added_sugars_100g",
    "sucrose_100g",
    "glucose_100g",
    "fructose_100g",
    "lactose_100g",
    "maltose_100g",
    "maltodextrins_100g",
    "starch_100g",
    "polyols_100g",
    "erythritol_100g",
    "fiber_100g",
    "soluble_fiber_100g",
    "insoluble_fiber_100g",
    "proteins_100g",
    "casein_100g",
    "serum_proteins_100g",
    "nucleotides_100g",
    "salt_100g",
    "added_salt_100g",
    "sodium_100g",
    "alcohol_100g",
    "vitamin_a_100g",
    "beta_carotene_100g",
    "vitamin_d_100g",
    "vitamin_e_100g",
    "vitamin_k_100g",
    "vitamin_c_100g",
    "vitamin_b1_100g",
    "vitamin_b2_100g",
    "vitamin_pp_100g",
    "vitamin_b6_100g",
    "vitamin_b9_100g",
    "folates_100g",
    "vitamin_b12_100g",
    "biotin_100g",
    "pantothenic_acid_100g",
    "silica_100g",
    "bicarbonate_100g",
    "potassium_100g",
    "chloride_100g",
    "calcium_100g",
    "phosphorus_100g",
    "iron_100g",
    "magnesium_100g",
    "zinc_100g",
    "copper_100g",
    "manganese_100g",
    "fluoride_100g",
    "selenium_100g",
    "chromium_100g",
    "molybdenum_100g",
    "iodine_100g",
    "caffeine_100g",
    "taurine_100g",
    "ph_100g",
    "cocoa_100g",
    "chlorophyl_100g",
    "carbon_footprint_100g",
    "glycemic_index_100g",
    "water_hardness_100g",
    "choline_100g",
    "phylloquinone_100g",
    "beta_glucan_100g",
    "inositol_100g",
    "carnitine_100g",
    "sulphate_100g",
    "nitrate_100g",
]

df = (
    pl.scan_parquet(PARQUET_PATH, n_rows=BATCH_SIZE)
    .select(
        [
            pl.col("code"),
            pl.col("code")
            .alias("url")
            .map_elements(
                lambda x: process_url(code=x, lang=LANG), return_dtype=pl.String
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
                lambda x: process_text(texts=x, lang=LANG), return_dtype=pl.String
            ),
            pl.col("generic_name").map_elements(
                lambda x: process_text(texts=x, lang=LANG), return_dtype=pl.String
            ),
            pl.col("quantity"),
            pl.col("packaging"),
            pl.col("packaging_tags").map_elements(process_tags, return_dtype=pl.String),
            pl.col("packaging_tags")
            .alias(f"packaging_{LANG}")
            .map_elements(
                lambda x: process_lang(tags=x, lang=LANG), return_dtype=pl.String
            ),
            pl.col("packaging_text").map_elements(
                lambda x: process_text(texts=x, lang=LANG), return_dtype=pl.String
            ),
            pl.col("brands"),
            pl.col("brands_tags").map_elements(process_tags, return_dtype=pl.String),
            pl.col("categories"),
            pl.col("categories_tags").map_elements(
                process_tags, return_dtype=pl.String
            ),
            pl.col("categories_tags")
            .alias(f"categories_{LANG}")
            .map_elements(
                lambda x: process_lang(tags=x, lang=LANG), return_dtype=pl.String
            ),
            pl.col("origins"),
            pl.col("origins_tags").map_elements(process_tags, return_dtype=pl.String),
            pl.col("origins_tags")
            .alias("origins_{LANG}")
            .map_elements(
                lambda x: process_lang(tags=x, lang=LANG), return_dtype=pl.String
            ),
            pl.col("manufacturing_places"),
            pl.col("manufacturing_places_tags").map_elements(
                process_tags, return_dtype=pl.String
            ),
            pl.col("labels"),
            pl.col("labels_tags").map_elements(process_tags, return_dtype=pl.String),
            pl.col("labels_tags")
            .alias(f"labels_{LANG}")
            .map_elements(
                lambda x: process_lang(tags=x, lang=LANG), return_dtype=pl.String
            ),
            pl.col("emb_codes"),
            pl.col("emb_codes_tags").map_elements(process_tags, return_dtype=pl.String),
            pl.col("cities_tags").map_elements(process_tags, return_dtype=pl.String),
            pl.col("purchase_places_tags")
            .alias("purchase_places")
            .map_elements(process_tags, return_dtype=pl.String),
            pl.col("stores"),
            #NOTE: to add or not?
            # pl.col("countries_tags")
            # .alias("countries")
            # .map_elements(lambda x: process_lang(x, lang=LANG), return_dtype=pl.String),
            pl.col("countries_tags").map_elements(process_tags, return_dtype=pl.String),
            pl.col("countries_tags")
            .alias(f"countries_{LANG}")
            .map_elements(
                lambda x: process_lang(tags=x, lang=LANG), return_dtype=pl.String
            ),
            pl.col("ingredients_text").map_elements(
                lambda x: process_text(texts=x, lang=LANG), return_dtype=pl.String
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
            .alias(f"allergens_{LANG}")
            .map_elements(
                lambda x: process_lang(tags=x, lang=LANG), return_dtype=pl.String
            ),
            pl.col("traces_tags")
            .alias("traces")
            .map_elements(process_tags, return_dtype=pl.String),
            pl.col("traces_tags").map_elements(
                process_tags,
                return_dtype=pl.String
            ),
            pl.col("traces_tags")
            .alias(f"traces_{LANG}")
            .map_elements(
                lambda x: process_lang(tags=x, lang=LANG),
                return_dtype=pl.String
            ),
            pl.col("serving_size"),
            pl.col("serving_quantity"),
            pl.col("no_nutrition_data"),
            pl.col("additives_n"),
            pl.col("additives_tags").alias("additives_tags").map_elements(
                process_tags,
                return_dtype=pl.String
            ),
            pl.col("additives_tags").map_elements(
                process_tags,
                return_dtype=pl.String
            ),
            pl.col("additives_tags")
            .alias(f"additives_{LANG}")
            .map_elements(
                lambda x: process_lang(tags=x, lang=LANG),
                return_dtype=pl.String
            ),
            pl.col("nutriscore_score"),
            pl.col("nutriscore_grade"),
            pl.col("nova_group"),
            pl.col("food_groups_tags").alias("food_groups").map_elements(
                process_tags,
                return_dtype=pl.String
            ),
            pl.col("food_groups_tags").map_elements(
                process_tags,
                return_dtype=pl.String
            ),
            pl.col("food_groups_tags")
            .alias(f"food_groups_{LANG}")
            .map_elements(
                lambda x: process_lang(tags=x, lang=LANG),
                return_dtype=pl.String
            ),
            pl.col("states_tags").alias("states").map_elements(
                process_tags,
                return_dtype=pl.String
            ),
            pl.col("states_tags").map_elements(
                process_tags,
                return_dtype=pl.String
            ),
            pl.col("states_tags")
            .alias(f"states_{LANG}")
            .map_elements(
                lambda x: process_lang(tags=x, lang=LANG),
                return_dtype=pl.String
            ),
            pl.col("ecoscore_score"),
            pl.col("ecoscore_grade"),
            pl.col("nutrient_levels_tags").map_elements(
                process_tags,
                return_dtype=pl.String
            ),
            pl.col("product_quantity"),
            pl.col("owner"),
            pl.col("data_quality_errors_tags").map_elements(
                process_tags,
                return_dtype=pl.String
            ),
            pl.col("unique_scans_n"),
            pl.col("popularity_tags").map_elements(
                process_tags,
                return_dtype=pl.String
            ),
            pl.col("completeness"),
            pl.col("last_image_t"),
            pl.col("last_image_t")
            .alias("last_image_datetime")
            .map_elements(datetime.fromtimestamp, return_dtype=pl.Datetime),
            pl.col("categories_tags")
            .alias("main_category")
            .map_elements(
                lambda arr: arr[-1],
                return_dtype=pl.String
            ),
            
            # pl.col("image_url"),
            # pl.col("image_small_url"),
            # pl.col("image_ingredients_url"),
            # pl.col("image_ingredients_small_url"),
            # pl.col("image_nutrition_url"),
            # pl.col("image_nutrition_small_url")
        ]
    )
    .sink_csv("data/processed.csv")
)

# processed = df.collect(streaming=True)


# print("job done.")
