from typing import Literal

from openfoodfacts_exports.exports.parquet.common import Image
from openfoodfacts_exports.exports.parquet.food import NutrimentField

from .utils import ENGLISH_FRENCH_COUNTRY_MAPPER

IMAGE_MAP = {
    "front": "front_",
    "ingredients": "ingredients_",
    "nutrition": "nutrition_",
}


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

    Output:
    - "barquette-en-plastique,film-en-plastique" if lang="fr
    - "glass" if lang="en"

    There's also a specific case when the only available tag is xx.
    In this condition, we use xx: tag.

    Original CSV
    ┌──────────────┬──────────────┬──────────────┐
    │    brands    │ brands_tags  │  brands_fr   │
    │   varchar    │   varchar    │   varchar    │
    ├──────────────┼──────────────┼──────────────┤
    │ vidifood     │ xx:vidifood  │ vidifood     │
    │ Knorr        │ xx:knorr     │ knorr        │
    └────────────────────────────────────────────┘

    """
    output = ",".join(
        [tag.split(":")[-1].strip() for tag in tags if tag.startswith(f"{lang}:")]
    )
    if output == "":
        output = ",".join(
            [tag.split(":")[-1].strip() for tag in tags if tag.startswith("xx:")]
        )
    return output


def process_lang_countries(tags: list[str], lang: str) -> str:
    """For these tags, we translate the countries to its language.
    CSV:
        ┌──────────────────┬──────────────────┬──────────────┐
        │    countries     │  countries_tags  │ countries_fr │
        │     varchar      │     varchar      │   varchar    │
        ├──────────────────┼──────────────────┼──────────────┤
        │ en:fr            │ en:france        │ France       │
        │ en:France        │ en:france        │ France       │
        │ en:france        │ en:france        │ France       │
        │ en:Ireland       │ en:ireland       │ Irlande      │
        │ United States    │ en:united-states │ États-Unis   │
        │ en:Ireland       │ en:ireland       │ Irlande      │
        │ United States    │ en:united-states │ États-Unis   │
        │ en:france        │ en:france        │ France       │
        │ en:fr            │ en:france        │ France       │
        │ en:United States │ en:united-states │ États-Unis   │
        ├──────────────────┴──────────────────┴──────────────┤
        │ 10 rows                                  3 columns │
        └────────────────────────────────────────────────────┘
    """
    output = ",".join(
        [
            tag.split(":")[-1].strip().lower()
            for tag in tags
            if tag.startswith(f"{lang}:")
        ]
    )
    # If no corresponding lang, we take the first English tag we translate in French
    if output == "":
        output = [
            tag.split(":")[-1].strip().lower() for tag in tags if tag.startswith("en:")
        ][0]
    return ENGLISH_FRENCH_COUNTRY_MAPPER.get(output, "") if lang == "fr" else output


def process_text(elts: list[dict[str, str]], lang: str) -> str | None:
    """In the Parquet, texts are grouped by lang as a dict as such:
    {
        "lang": "main", "text": <text>,
        "lang": "pl", "text": <text>
    }"""
    if len(elts) == 0:
        return None
    for elt in elts:
        if elt["lang"] == lang:
            return elt["text"]
    # In the case the lang wasn't found, take "main"
    for elt in elts:
        if elt["lang"] == "main":
            return elt["text"]


def process_image_url(
    images: list[dict[str, str | int]],
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
                f"{size}"
                f".jpg"
            )
            return image_url


def process_nutrients(
    nutrients: list[dict[str, float]],
    nutrient_name: str,
) -> float | None:
    for nutrient in nutrients:
        nutrient_model = NutrimentField.model_validate(nutrient)
        if nutrient_model.name == nutrient_name:
            return nutrient_model.per_100g
