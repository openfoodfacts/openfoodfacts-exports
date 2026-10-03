import csv
import gzip
import json
from pathlib import Path

import pytest

from openfoodfacts_exports import settings
from openfoodfacts_exports.exports.csv.columns import FOOD_CSV_COLUMNS
from openfoodfacts_exports.exports.csv.food import (
    export_food_csv,
    image_urls,
    load_english_names,
    product_to_row,
    upload_food_csv,
)


def as_dict(product, names=None):
    return dict(zip(FOOD_CSV_COLUMNS, product_to_row(product, names or {})))


def test_legacy_field_mapping():
    row = as_dict(
        {
            "code": "0012345678905",
            "product_name": "Nom",
            "product_name_en": "Name",
            "ingredients_text": "A\tB\nC",
            "brands_tags": ["xx:brand"],
            "created_t": 1,
            "last_modified_t": 0,
            "categories_tags": ["en:foods", "en:biscuits"],
            "countries_tags": ["en:france"],
            "nutriments": {"fat_100g": 0, "salt_100g": 7e-5},
            "ecoscore_score": 42,
            "images": {"front_fr": {"rev": "2", "sizes": {}}},
        },
        {
            "categories": {"en:foods": "Foods", "en:biscuits": "Biscuits"},
            "countries": {"en:france": "France"},
        },
    )
    assert row["code"] == "0012345678905"
    assert row["product_name"] == "Name"
    assert row["ingredients_text"] == "A B C"
    assert row["brands_tags"] == "xx:brand"
    assert row["brands_en"] == "brand"
    assert row["categories_en"] == "Foods,Biscuits"
    assert row["countries_en"] == "France"
    assert row["main_category"] == "en:biscuits"
    assert row["main_category_en"] == "Biscuits"
    assert row["created_datetime"] == "1970-01-01T00:00:01Z"
    assert row["last_modified_datetime"] == ""
    assert row["fat_100g"] == "0"
    assert row["salt_100g"] == "0.00007"
    assert row["environmental_score_score"] == "42"
    assert row["image_url"].endswith("/001/234/567/8905/front_fr.2.400.jpg")
    assert row["image_small_url"].endswith("front_fr.2.200.jpg")


def test_new_nutrition_and_image_schema():
    row = as_dict(
        {
            "code": "123",
            "nutrition": {
                "aggregated_set": {
                    "per": "100g",
                    "nutrients": {"fat": {"value": 12.5}},
                }
            },
            "images": {
                "selected": {
                    "front": {
                        "en": {
                            "sizes": {"400": {"url": "https://example.org/front.jpg"}},
                        }
                    }
                }
            },
        }
    )
    assert row["fat_100g"] == "12.5"
    assert row["image_url"] == "https://example.org/front.jpg"
    assert row["salt_100g"] == ""
    assert (
        as_dict(
            {
                "code": "123",
                "nutrition": {
                    "aggregated_set": {
                        "per": "serving",
                        "nutrients": {"fat": {"value": 12.5}},
                    }
                },
            }
        )["fat_100g"]
        == ""
    )


def test_images_prefer_english_then_main_language():
    urls = image_urls(
        {
            "code": "123",
            "lang": "fr",
            "images": {
                "front_en": {"rev": 3},
                "front_fr": {"rev": 4},
                "ingredients_fr": {"rev": 5},
            },
        }
    )
    assert urls["image_url"].endswith("front_en.3.400.jpg")
    assert urls["image_ingredients_url"].endswith("ingredients_fr.5.400.jpg")


@pytest.mark.parametrize(
    "records,passes",
    [
        ([], False),
        ([{}], False),
        ([{"code": "123"}, {}, {}], False),
        ([{"code": "123"}, {}], True),
    ],
)
def test_csv_quality_and_previous_file(tmp_path, monkeypatch, records, passes):
    monkeypatch.setattr(settings, "MAX_EXPORT_REJECTION_RATE", 0.5)
    source = tmp_path / "source.jsonl.gz"
    with gzip.open(source, "wt") as stream:
        for record in records:
            stream.write(json.dumps(record) + "\n")
    output = tmp_path / "food.csv.gz"
    output.write_bytes(b"previous export")
    if not passes:
        with pytest.raises(ValueError):
            export_food_csv(source, output, names={})
        assert output.read_bytes() == b"previous export"
    else:
        stats = export_food_csv(source, output, names={})
        assert stats.records_read == 2
        assert stats.records_exported == stats.records_rejected == 1
        with gzip.open(output, "rt") as stream:
            rows = list(csv.reader(stream, delimiter="\t"))
        assert rows[0] == list(FOOD_CSV_COLUMNS)
        assert len(rows[1]) == 211
        assert rows[1][0] == "123"
    assert not list(tmp_path.glob("tmp*"))


def test_csv_missing_source(tmp_path, mocker):
    load = mocker.patch("openfoodfacts_exports.exports.csv.food.load_english_names")
    with pytest.raises(FileNotFoundError):
        export_food_csv(tmp_path / "missing", tmp_path / "output.csv.gz")
    load.assert_not_called()


def test_taxonomy_names_and_cache(tmp_path, monkeypatch, mocker):
    monkeypatch.setattr(settings, "DATASET_DIR", tmp_path)

    payload = {
        "en:france": {"name": {"en": "France"}},
        "xx:test": {"name": {"xx": "International"}},
        "fr:unknown": {},
    }
    response = mocker.Mock()
    response.json.return_value = payload
    response.content = json.dumps(payload).encode()
    fetch = mocker.patch(
        "openfoodfacts_exports.exports.csv.food.requests.get", return_value=response
    )
    names = load_english_names()
    assert names["countries"]["en:france"] == "France"
    assert names["countries"]["xx:test"] == "International"
    assert names["countries"]["fr:unknown"] == "fr:unknown"
    fetch.reset_mock()
    assert load_english_names() == names
    fetch.assert_not_called()


@pytest.mark.parametrize("enabled", [0, 1])
def test_csv_upload_setting(monkeypatch, mocker, enabled):
    monkeypatch.setattr(settings, "ENABLE_S3_PUSH", enabled)
    client = mocker.patch("openfoodfacts_exports.exports.csv.food.get_minio_client")
    output = Path("food.csv.gz")
    upload_food_csv(output)
    if enabled:
        client.return_value.fput_object.assert_called_once_with(
            settings.AWS_S3_DATASET_BUCKET,
            output.name,
            str(output),
            content_type="application/gzip",
        )
    else:
        client.assert_not_called()


def test_failed_taxonomy_download_is_not_cached(tmp_path, monkeypatch, mocker):
    import requests

    monkeypatch.setattr(settings, "DATASET_DIR", tmp_path)
    response = mocker.Mock()
    response.raise_for_status.side_effect = requests.HTTPError("download failed")
    fetch = mocker.patch(
        "openfoodfacts_exports.exports.csv.food.requests.get", return_value=response
    )
    with pytest.raises(requests.HTTPError):
        load_english_names()
    assert list((tmp_path / "taxonomies").iterdir()) == []
    assert fetch.call_args.kwargs["headers"]["User-Agent"] == settings.USER_AGENT


@pytest.mark.parametrize("value,expected", [(True, "on"), (False, ""), ("on", "on")])
def test_no_nutrition_flag(value, expected):
    assert (
        as_dict({"code": "123", "no_nutrition_data": value})["no_nutrition_data"]
        == expected
    )


def test_invalid_taxonomy_response_is_not_cached(tmp_path, monkeypatch, mocker):
    monkeypatch.setattr(settings, "DATASET_DIR", tmp_path)
    response = mocker.Mock()
    response.json.return_value = ["unexpected schema"]
    mocker.patch(
        "openfoodfacts_exports.exports.csv.food.requests.get", return_value=response
    )
    with pytest.raises(ValueError, match="Invalid taxonomy"):
        load_english_names()
    assert list((tmp_path / "taxonomies").iterdir()) == []
