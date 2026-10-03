"""Stream the English legacy tab-separated food export from JSONL."""

import csv
import gzip
import json
import logging
import tempfile

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import requests

from openfoodfacts.images import generate_image_url
from openfoodfacts.utils import jsonl_iter

from openfoodfacts_exports import settings
from openfoodfacts_exports.exports.parquet import ExportStats
from openfoodfacts_exports.utils import get_minio_client

from .columns import FOOD_CSV_COLUMNS

logger = logging.getLogger(__name__)
FOOD_CSV_PATH = settings.DATASET_DIR / "en.openfoodfacts.org.products.csv.gz"
TAXONOMY_FIELDS = (
    "packaging",
    "categories",
    "origins",
    "labels",
    "countries",
    "allergens",
    "traces",
    "additives",
    "food_groups",
    "states",
)
LANGUAGE_FIELDS = {
    "product_name",
    "abbreviated_product_name",
    "generic_name",
    "packaging_text",
    "ingredients_text",
}


def load_english_names() -> dict[str, dict[str, str]]:
    """Cache taxonomy files and retain only English/international display names."""
    cache = settings.DATASET_DIR / "taxonomies"
    cache.mkdir(exist_ok=True)
    result = {}
    for field in TAXONOMY_FIELDS:
        # Traces use the allergen taxonomy, rather than a separate taxonomy.
        taxonomy_name = "allergens" if field == "traces" else field
        filename = (
            "additives.json"
            if taxonomy_name == "additives"
            else f"{taxonomy_name}.full.json"
        )
        path = cache / filename
        response = None
        if not path.exists():
            response = requests.get(
                f"https://static.openfoodfacts.org/data/taxonomies/{path.name}",
                headers={"User-Agent": settings.USER_AGENT},
                timeout=60,
            )
            response.raise_for_status()
            taxonomy = response.json()
        else:
            with path.open() as stream:
                taxonomy = json.load(stream)
        if not isinstance(taxonomy, dict) or any(
            not isinstance(data, dict) or not isinstance(data.get("name", {}), dict)
            for data in taxonomy.values()
        ):
            raise ValueError(f"Invalid taxonomy response for {field}")
        result[field] = {
            tag: data.get("name", {}).get("en") or data.get("name", {}).get("xx") or tag
            for tag, data in taxonomy.items()
        }
        if response is not None:
            # Validate before caching; clean up temporary files on any failure.
            with tempfile.TemporaryDirectory(dir=cache) as temp_dir:
                temporary = Path(temp_dir) / path.name
                temporary.write_bytes(response.content)
                temporary.replace(path)
    return result


def format_value(value) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return ",".join(format_value(item) for item in value)
    if isinstance(value, dict):
        raise ValueError("Cannot export an object as a scalar CSV field")
    if isinstance(value, float) and "e" in str(value).lower():
        return format(Decimal(str(value)), "f")
    # Keep one physical line per product, as in the legacy exporter.
    return str(value).replace("\t", " ").replace("\r", " ").replace("\n", " ")


def localized_tags(product: dict, field: str, names: dict) -> str:
    tags = product.get(f"{field}_tags") or []
    if field == "brands":
        return ",".join(tag.removeprefix("xx:") for tag in tags)
    return ",".join(names.get(field, {}).get(tag, tag) for tag in tags)


def image_urls(product: dict) -> dict[str, str]:
    result = {}
    images = product.get("images") or {}
    for kind in ("front", "ingredients", "nutrition"):
        selected = images.get("selected", {}).get(kind, {})
        if not selected:
            selected = {
                key[len(kind) + 1 :]: data
                for key, data in images.items()
                if key.startswith(f"{kind}_")
            }
        languages = ["en", product.get("lang", "en"), *sorted(selected)]
        language = next((lang for lang in languages if selected.get(lang)), None)
        if language is None:
            continue
        image = selected[language]
        field = "image" if kind == "front" else f"image_{kind}"
        for size, suffix in (("400", "_url"), ("200", "_small_url")):
            url = (image.get("sizes", {}).get(size) or {}).get("url")
            if not url and image.get("rev") is not None:
                url = generate_image_url(
                    str(product["code"]), f"{kind}_{language}.{image['rev']}.{size}"
                )
            if url:
                result[field + suffix] = url
    return result


def product_to_row(product: dict, names: dict) -> list[str]:
    code = product.get("code")
    if not code or not str(code).isdigit() or int(code) == 0 or product.get("empty"):
        raise ValueError("Product is empty or has no valid barcode")
    derived = image_urls(product)
    derived["url"] = f"https://world-en.openfoodfacts.org/product/{code}"
    categories = product.get("categories_tags") or []
    derived["main_category"] = categories[-1] if categories else ""
    derived["main_category_en"] = names.get("categories", {}).get(
        derived["main_category"], derived["main_category"]
    )
    nutrition = (product.get("nutrition") or {}).get("aggregated_set") or {}
    nutriments = product.get("nutriments") or {}
    row = []
    for column in FOOD_CSV_COLUMNS:
        if column in derived:
            value = derived[column]
        elif column.endswith("_datetime"):
            timestamp = product.get(column.removesuffix("_datetime") + "_t")
            value = (
                datetime.fromtimestamp(float(timestamp), timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                )
                if timestamp and float(timestamp) > 0
                else ""
            )
        elif column.endswith("_100g"):
            if nutrition:
                value = None
                if nutrition.get("per") in ("100g", "100ml"):
                    value = (
                        nutrition.get("nutrients", {}).get(column.removesuffix("_100g"))
                        or {}
                    ).get("value")
            else:
                value = nutriments.get(column)
        elif column.endswith("_en") and column.removesuffix("_en") in (
            *TAXONOMY_FIELDS,
            "brands",
        ):
            value = product.get(column)
            if value is None:
                value = localized_tags(product, column.removesuffix("_en"), names)
        elif column in LANGUAGE_FIELDS:
            value = product.get(column + "_en") or product.get(column)
        elif column == "no_nutrition_data":
            value = product.get(column)
            if isinstance(value, bool):
                value = "on" if value else ""
        elif column.startswith("environmental_score_"):
            value = product.get(
                column, product.get(column.replace("environmental_score_", "ecoscore_"))
            )
        else:
            value = product.get(column)
        row.append(format_value(value))
    return row


def export_food_csv(
    dataset_path: Path,
    output_path: Path = FOOD_CSV_PATH,
    names: dict[str, dict[str, str]] | None = None,
) -> ExportStats:
    """Write gzip-compressed TSV, validating quality before replacing the output."""
    if not dataset_path.is_file():
        raise FileNotFoundError(dataset_path)
    if names is None:
        names = load_english_names()
    stats = ExportStats()
    with tempfile.TemporaryDirectory(dir=output_path.parent) as temp_dir:
        temporary = Path(temp_dir) / output_path.name
        with gzip.open(temporary, "wt", encoding="utf-8", newline="") as stream:
            writer = csv.writer(stream, delimiter="\t", lineterminator="\n")
            writer.writerow(FOOD_CSV_COLUMNS)
            for product in jsonl_iter(dataset_path):
                stats.records_read += 1
                try:
                    row = product_to_row(product, names)
                except (ValueError, TypeError, OverflowError, AttributeError) as error:
                    stats.records_rejected += 1
                    logger.warning(
                        "CSV rejected product %s: %s",
                        product.get("code", "unknown"),
                        error,
                    )
                    continue
                writer.writerow(row)
                stats.records_exported += 1
        logger.info("CSV quality: %s", stats)
        stats.validate(settings.MAX_EXPORT_REJECTION_RATE)
        temporary.replace(output_path)
    return stats


def upload_food_csv(data_path: Path = FOOD_CSV_PATH) -> None:
    """Upload the completed export independently of conversion."""
    if not settings.ENABLE_S3_PUSH:
        logger.info("S3 push is disabled, skipping CSV upload")
        return
    get_minio_client().fput_object(
        settings.AWS_S3_DATASET_BUCKET,
        data_path.name,
        str(data_path),
        content_type="application/gzip",
    )
