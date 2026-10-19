"""Stream the English legacy tab-separated food export from JSONL."""

import csv
import gzip
import logging
import shutil
import tempfile
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import tqdm
import zstandard
from openfoodfacts import Flavor
from openfoodfacts.images import generate_image_url
from openfoodfacts.taxonomy import get_taxonomy
from openfoodfacts.types import TaxonomyType
from openfoodfacts.utils import jsonl_iter

from openfoodfacts_exports import settings
from openfoodfacts_exports.exports.parquet import ExportStats
from openfoodfacts_exports.utils import get_minio_client

from .columns import FOOD_CSV_COLUMNS

logger = logging.getLogger(__name__)

LANGUAGE_FIELDS = {
    "product_name",
    "abbreviated_product_name",
    "generic_name",
    "packaging_text",
    "ingredients_text",
}

DATETIME_COLUMNS = {
    column: column.removesuffix("_datetime") + "_t"
    for column in FOOD_CSV_COLUMNS
    if column.endswith("_datetime")
}
NUTRIENT_COLUMNS = tuple(
    column for column in FOOD_CSV_COLUMNS if column.endswith("_100g")
)
TAXONOMY_TYPES = {
    "packaging": TaxonomyType.packaging,
    "categories": TaxonomyType.category,
    "origins": TaxonomyType.origin,
    "labels": TaxonomyType.label,
    "countries": TaxonomyType.country,
    "allergens": TaxonomyType.allergen,
    "traces": TaxonomyType.allergen,
    "additives": TaxonomyType.additive,
    "states": TaxonomyType.state,
    "food_groups": TaxonomyType.food_group,
}
TAXONOMY_FIELDS = tuple(TAXONOMY_TYPES)
LOCALIZED_FIELDS = (*TAXONOMY_FIELDS, "brands")


def _load_sdk_taxonomy_names(taxonomy_type: TaxonomyType, lang: str) -> dict[str, str]:
    """Load taxonomy names through the SDK's ETag-aware cache."""
    taxonomy = get_taxonomy(
        taxonomy_type,
        flavor=Flavor.off,
        download_newer=True,
        cache_dir=settings.DATASET_DIR / "taxonomies",
    )
    return {tag: taxonomy.get_localized_name(tag, lang) for tag in taxonomy.keys()}


def load_names(lang: str) -> dict[str, dict[str, str]]:
    """Load names in a specific language with SDK caching; traces share the allergen
    taxonomy."""
    # Deduplicate aliases such as traces/allergens so each taxonomy loads once.
    names_by_type = {
        taxonomy_type: _load_sdk_taxonomy_names(taxonomy_type, lang)
        for taxonomy_type in set(TAXONOMY_TYPES.values())
    }
    names = {
        field: names_by_type[taxonomy_type]
        for field, taxonomy_type in TAXONOMY_TYPES.items()
    }
    return names


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


def generate_image_urls(product: dict) -> dict[str, str]:
    result = {}
    images = product.get("images") or {}
    for kind in ("front", "ingredients", "nutrition"):
        selected = images.get("selected", {}).get(kind, {})
        if not selected:
            selected = {
                key.removeprefix(f"{kind}_"): data
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


def format_datetime(timestamp: int | float | str | None) -> str:
    if not timestamp:
        return ""
    seconds = float(timestamp)
    if seconds <= 0:
        return ""
    return datetime.fromtimestamp(seconds, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def nutrient_values(product: dict) -> dict:
    """Flatten either nutrition schema into the legacy per-100g columns."""
    nutrition = (product.get("nutrition") or {}).get("aggregated_set") or {}
    if not nutrition:
        nutriments = product.get("nutriments") or {}
        return {column: nutriments.get(column) for column in NUTRIENT_COLUMNS}
    if nutrition.get("per") not in ("100g", "100ml"):
        return dict.fromkeys(NUTRIENT_COLUMNS)
    nutrients = nutrition.get("nutrients") or {}
    return {
        column: (nutrients.get(column.removesuffix("_100g")) or {}).get("value")
        for column in NUTRIENT_COLUMNS
    }


def product_to_row(product: dict, names: dict) -> list[str]:
    code = product.get("code")
    if not code or not str(code).isdigit() or int(code) == 0 or product.get("empty"):
        raise ValueError("Product is empty or has no valid barcode")

    # Prepare the derived values once, then serialize in the fixed header order.
    values = {**product, **generate_image_urls(product), **nutrient_values(product)}
    values["url"] = f"https://world-en.openfoodfacts.org/product/{code}"
    categories = product.get("categories_tags") or []
    main_category = categories[-1] if categories else ""
    values["main_category"] = main_category
    values["main_category_en"] = names.get("categories", {}).get(
        main_category, main_category
    )
    values.update(
        {
            column: format_datetime(product.get(source))
            for column, source in DATETIME_COLUMNS.items()
        }
    )
    for field in LOCALIZED_FIELDS:
        column = f"{field}_en"
        if product.get(column) is None:
            values[column] = localized_tags(product, field, names)
    for field in LANGUAGE_FIELDS:
        values[field] = product.get(f"{field}_en") or product.get(field)
    for suffix in ("score", "grade"):
        column = f"environmental_score_{suffix}"
        values[column] = product.get(column, product.get(f"ecoscore_{suffix}"))
    if isinstance(product.get("no_nutrition_data"), bool):
        values["no_nutrition_data"] = "on" if product["no_nutrition_data"] else ""
    return [
        "" if (value := values.get(column)) is None else format_value(value)
        for column in FOOD_CSV_COLUMNS
    ]


def export_food_csv(
    dataset_path: Path,
    output_path: Path,
    names: dict[str, dict[str, str]] | None = None,
    lang: str | None = None,
) -> ExportStats:
    """Write a validated plain TSV once for both compression jobs.

    Args:
        dataset_path: Path to the gzipped JSONL product dataset.
        output_path: Path where the plain tab-separated CSV is written.
        names: Optional taxonomy-name maps, keyed by field and taxonomy tag.
            English taxonomy names are loaded when omitted.
        lang: the language used to load the names from taxonomy. If None,
            "en" is used unless `names` is provided.
    """
    if not dataset_path.is_file():
        raise FileNotFoundError(dataset_path)
    if names is None:
        names = load_names(lang or "en")

    logger.info("Exporting CSV dump from %s to %s", dataset_path, output_path)
    stats = ExportStats()
    with tempfile.TemporaryDirectory(dir=output_path.parent) as temp_dir:
        temporary = Path(temp_dir) / output_path.name
        with temporary.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.writer(stream, delimiter="\t", lineterminator="\n")

            writer.writerow(FOOD_CSV_COLUMNS)
            for product in tqdm.tqdm(jsonl_iter(dataset_path), desc="products"):
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


def compress_food_csv_gzip(csv_path: Path, output_path: Path) -> None:
    """Compress the validated plain CSV without repeating row mapping."""
    level = settings.CSV_GZIP_COMPRESSION_LEVEL
    if not 0 <= level <= 9:
        raise ValueError("CSV gzip compression level must be between 0 and 9")
    with tempfile.TemporaryDirectory(dir=output_path.parent) as temp_dir:
        temporary = Path(temp_dir) / output_path.name
        with (
            csv_path.open("rb") as source,
            gzip.open(temporary, "wb", compresslevel=level) as output,
        ):
            shutil.copyfileobj(source, output, length=1024 * 1024)
        temporary.replace(output_path)


def compress_food_csv_zstd(csv_path: Path, output_path: Path) -> None:
    """Compress the validated plain CSV using bounded Zstandard buffers."""
    level = settings.CSV_ZSTD_COMPRESSION_LEVEL
    if not 1 <= level <= 22:
        raise ValueError("CSV Zstandard compression level must be between 1 and 22")
    compressor = zstandard.ZstdCompressor(level=level, write_checksum=True)
    with tempfile.TemporaryDirectory(dir=output_path.parent) as temp_dir:
        temporary = Path(temp_dir) / output_path.name
        with csv_path.open("rb") as source, temporary.open("wb") as output:
            compressor.copy_stream(source, output)
        temporary.replace(output_path)


def cleanup_food_csv(csv_path: Path) -> None:
    """Remove this run's plain CSV after both compression jobs succeed."""
    csv_path.unlink(missing_ok=True)


def upload_food_csv(data_path: Path) -> None:
    """Upload the completed export independently of conversion."""
    content_types = {".gz": "application/gzip", ".zst": "application/zstd"}
    if data_path.suffix not in content_types:
        raise ValueError("CSV upload expects a .gz or .zst file")
    if not settings.ENABLE_S3_PUSH:
        logger.info("S3 push is disabled, skipping CSV upload")
        return
    if not settings.ENABLE_CSV_PUSH:
        logger.info("CSV push is disabled, skipping CSV upload")
        return
    get_minio_client().fput_object(
        settings.AWS_S3_DATASET_BUCKET,
        data_path.name,
        str(data_path),
        content_type=content_types[data_path.suffix],
    )
