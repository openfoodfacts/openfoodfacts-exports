from typing import Literal, Any

from openfoodfacts_exports.exports.parquet.common import Image
from openfoodfacts_exports.exports.parquet.food import NutrimentField

IMAGE_MAP = {
    "front": "front_",
    "ingredients": "ingredients_",
    "nutrition": "nutrition_",
}

nutrient_names = Literal[
    "energy-kj_100g"
    "energy-kcal_100g"
    "energy_100g"
    "energy-from-fat_100g"
    "fat_100g" 
    "saturated-fat_100g"
    "butyric-acid_100g"
    "caproic-acid_100g"
    "caprylic-acid_100g"
    "capric-acid_100g"
    "lauric-acid_100g"
    "myristic-acid_100g"
    "palmitic-acid_100g"
    "stearic-acid_100g"
    "arachidic-acid_100g"
    "behenic-acid_100g"
    "lignoceric-acid_100g"
    "cerotic-acid_100g"
    "montanic-acid_100g"
    "melissic-acid_100g"
    "unsaturated-fat_100g"
    "monounsaturated-fat_100g"
    "omega-9-fat_100g"
    "polyunsaturated-fat_100g"
    "omega-3-fat_100g"
    "omega-6-fat_100g"
    "alpha-linolenic-acid_100g"
    "eicosapentaenoic-acid_100g"
    "docosahexaenoic-acid_100g"
    "linoleic-acid_100g"
    "arachidonic-acid_100g"
    "gamma-linolenic-acid_100g"
    "dihomo-gamma-linolenic-acid_100g"
    "oleic-acid_100g"
    "elaidic-acid_100g"
    "gondoic-acid_100g"
    "mead-acid_100g"
    "erucic-acid_100g"
    "nervonic-acid_100g"
    "trans-fat_100g"
    "cholesterol_100g"
    "carbohydrates_100g"
    "sugars_100g"
    "added-sugars_100g"
    "sucrose_100g"
    "glucose_100g"
    "fructose_100g"
    "lactose_100g"
    "maltose_100g"
    "maltodextrins_100g"
    "starch_100g"
    "polyols_100g"
    "erythritol_100g"
    "fiber_100g"
    "soluble-fiber_100g"
    "insoluble-fiber_100g"
    "proteins_100g"
    "casein_100g"
    "serum-proteins_100g"
    "nucleotides_100g"
    "salt_100g"
    "added-salt_100g"
    "sodium_100g"
    "alcohol_100g"
    "vitamin-a_100g"
    "beta-carotene_100g"
    "vitamin-d_100g"
    "vitamin-e_100g"
    "vitamin-k_100g"
    "vitamin-c_100g"
    "vitamin-b1_100g"
    "vitamin-b2_100g"
    "vitamin-pp_100g"
    "vitamin-b6_100g"
    "vitamin-b9_100g"
    "folates_100g"
    "vitamin-b12_100g"
    "biotin_100g"
    "pantothenic-acid_100g"
    "silica_100g"
    "bicarbonate_100g"
    "potassium_100g"
    "chloride_100g"
    "calcium_100g"
    "phosphorus_100g"
    "iron_100g"
    "magnesium_100g"
    "zinc_100g"
    "copper_100g"
    "manganese_100g"
    "fluoride_100g"
    "selenium_100g"
    "chromium_100g"
    "molybdenum_100g"
    "iodine_100g"
    "caffeine_100g"
    "taurine_100g"
    "ph_100g"
    "fruits-vegetables-nuts_100g"
    "fruits-vegetables-nuts-dried_100g"
    "fruits-vegetables-nuts-estimate_100g"
    "fruits-vegetables-nuts-estimate-from-ingredients_100g"
    "collagen-meat-protein-ratio_100g"
    "cocoa_100g"
    "chlorophyl_100g"
    "carbon-footprint_100g"
    "carbon-footprint-from-meat-or-fish_100g"
    "nutrition-score-fr_100g"
    "nutrition-score-uk_100g"
    "glycemic-index_100g"
    "water-hardness_100g"
    "choline_100g"
    "phylloquinone_100g"
    "beta-glucan_100g"
    "inositol_100g"
    "carnitine_100g"
    "sulphate_100g"
    "nitrate_100g"
    "acidity_100g"
]


def process_url(lang: str, code: str) -> str:
    base_url = f"http://{lang}.openfoodfacts.org/product/"
    return base_url + code


def process_tags(tags: list[str]) -> str:
    return ",".join(tags)


def process_lang(tags: list[str], lang: str) -> str:
    """We extract from 'tags' all element tagged by the appropriate language

    Example:
    - [en:plastic, en:tray, fr:barquette-en-plastique, fr:film-en-plastique]
    - [en:glass, en:bottle]

    -Output: glass if lang="en"
    """
    return ",".join([tag.split(":")[-1] for tag in tags if tag.startswith(f"{lang}:")])


def process_text(texts: list[dict], lang: str) -> str:
    """In the Parquet, texts are grouped by lang as a dict as such:
    {
        "lang": "main", "text": <text>,
        "lang": "pl", "text": <text>
    }"""
    for elt in texts:
        return elt["text"] if elt["lang"] == lang else elt.get("main", "")


def process_image_url(
    images: list[dict],
    lang: str,
    code: str,
    image_key: Literal["front", "ingredients", "nutrition"],
    size: Literal["100", "200", "400", "full"],
) -> str | None:
    """
    Parquet: [
        {'key': ingredients_fr, 'imgid': 1, 'rev': 5, 'sizes': {'100': {'h': 100, 'w': 99}, '200': {'h': 200, 'w': 198}, '400': {'h': 400, 'w': 395}, 'full': {'h': 1200, 'w': 1185}}, 'uploaded_t': NULL, 'uploader': NULL},
        {'key': ingredients_en, 'imgid': 1, 'rev': 4, 'sizes': {'100': {'h': 100, 'w': 99}, '200': {'h': 200, 'w': 198}, '400': {'h': 400, 'w': 395}, 'full': {'h': 1200, 'w': 1185}}, 'uploaded_t': NULL, 'uploader': NULL},
        {'key': 2, 'imgid': NULL, 'rev': NULL, 'sizes': {'100': {'h': 100, 'w': 56}, '200': NULL, '400': {'h': 400, 'w': 225}, 'full': {'h': 3264, 'w': 1836}}, 'uploaded_t': 1582317258, 'uploader': morganfay},
        {'key': 3, 'imgid': NULL, 'rev': NULL, 'sizes': {'100': {'h': 100, 'w': 44}, '200': NULL, '400': {'h': 348, 'w': 152}, 'full': {'h': 348, 'w': 152}}, 'uploaded_t': 1647551173, 'uploader': kiliweb},
        {'key': 1, 'imgid': NULL, 'rev': NULL, 'sizes': {'100': {'h': 100, 'w': 99}, '200': NULL, '400': {'h': 400, 'w': 395}, 'full': {'h': 1200, 'w': 1185}}, 'uploaded_t': 1544781442, 'uploader': kiliweb},
        {'key': front_en, 'imgid': 3, 'rev': 14, 'sizes': {'100': {'h': 100, 'w': 44}, '200': {'h': 200, 'w': 87}, '400': {'h': 348, 'w': 152}, 'full': {'h': 348, 'w': 152}}, 'uploaded_t': NULL, 'uploader': NULL}
    ]
    CSV (EN): https://images.openfoodfacts.org/images/products/007/435/000/0027/front_en.14.400.jpg

    The URL is composed of:
        * The Barcode 3x3 + the rest (0074350000027 => /007/435/000/0027)
        * The key depending on the lang (front_en)
        * The Revision (Rev)
        * The size (400 here)

    Check documentation for more information: https://openfoodfacts.github.io/openfoodfacts-server/api/how-to-download-images/
    """

    for image in images:
        image_model = Image.model_validate(image)
        if image_model.key == IMAGE_MAP[image_key] + lang:
            splitted_code = (
                code[:3] + "/" + code[3:6] + "/" + code[6:9] + "/" + code[9:]
            )
            image_url = (
                f"https://images.openfoodfacts.org/images/products/"
                f"{splitted_code}/"
                f"{image_model.key}."
                f"{image_model.rev}."
                f"{image_model.sizes.get(size)}"
                f".jpg"
            )
            return image_url


def process_last_item_from_array(array: list[Any]) -> Any:
    if array:
        return array[-1]


def process_nutrients(
    nutrients: list[dict],
    nutrient_name: nutrient_names,
) -> float | None:
    for nutrient in nutrients:
        nutrient_model = NutrimentField.model_validate(nutrient)
        if nutrient_model.name == nutrient_name:
            return nutrient_model.per_100g
