import json
from pathlib import Path

import pyarrow.parquet as pq

from openfoodfacts_exports.exports.parquet.price import (
    PRICE_PRODUCT_SCHEMA,
    convert_jsonl_to_parquet,
)

PROOF = {
    "id": 10,
    "type": "RECEIPT",
    "file_path": "0014/q2nrQoF0A7.jpg",
    "image_thumb_path": "0014/q2nrQoF0A7.400.jpg",
    "owner": "user",
    "currency": "EUR",
    "tags": ["challenge-1"],
}
LOCATION = {
    "id": 20,
    "type": "OSM",
    "osm_name": "Carrefour City",
    "osm_brand": "Carrefour",
    "osm_tags": ["organic:only"],
    "osm_version": 3,
    "tags": ["bulk"],
    "osm_address_city": "Paris",
}
PRODUCT = {
    "id": 30,
    "code": "3017620422003",
    "source": "off",
    "product_name": "Nutella",
    "product_quantity": 400,
    "product_quantity_unit": "g",
    "categories_tags": ["en:spreads"],
    "brands_tags": ["ferrero"],
    "nutriscore_grade": "e",
    "nova_group": 4,
    "creator": "kiliweb",
    "price_count": 2,
}
PRICES = [
    {
        "id": 1,
        "type": "PRODUCT",
        "product_id": 30,
        "product_code": "3017620422003",
        "product_name": "Nutella 400g",
        "price": "3.5",
        "currency": "EUR",
        "proof_id": 10,
        "location_id": 20,
        "tags": ["challenge-5"],
        "duplicate_of": None,
    },
    {
        "id": 2,
        "type": "CATEGORY",
        "product_id": None,
        "category_tag": "en:apples",
        "price": "2.0",
        "currency": "EUR",
        "proof_id": 10,
        "location_id": 20,
        "tags": [],
        "duplicate_of": 1,
    },
]


def write_jsonl(path: Path, items: list[dict]) -> Path:
    path.write_text("".join(json.dumps(item) + "\n" for item in items))
    return path


def test_convert_jsonl_to_parquet_joins_proof_location_and_product(tmp_path):
    output_path = tmp_path / "prices.parquet"
    convert_jsonl_to_parquet(
        output_file_path=output_path,
        dataset_price_path=write_jsonl(tmp_path / "prices.jsonl", PRICES),
        dataset_proof_path=write_jsonl(tmp_path / "proofs.jsonl", [PROOF]),
        dataset_location_path=write_jsonl(tmp_path / "locations.jsonl", [LOCATION]),
        dataset_product_path=write_jsonl(tmp_path / "products.jsonl", [PRODUCT]),
    )

    table = pq.read_table(output_path)
    assert table.schema == PRICE_PRODUCT_SCHEMA
    product_price, category_price = table.to_pylist()

    assert product_price["product_id"] == 30
    assert product_price["product_name"] == "Nutella 400g"
    assert product_price["product_product_name"] == "Nutella"
    assert product_price["product_product_quantity"] == 400
    assert product_price["product_categories_tags"] == ["en:spreads"]
    assert product_price["product_nova_group"] == 4
    assert product_price["product_creator"] == "kiliweb"
    assert product_price["tags"] == ["challenge-5"]
    assert product_price["duplicate_of"] is None
    assert product_price["proof_type"] == "RECEIPT"
    assert product_price["proof_tags"] == ["challenge-1"]
    assert product_price["proof_image_thumb_path"] == "0014/q2nrQoF0A7.400.jpg"
    assert product_price["location_osm_address_city"] == "Paris"
    assert product_price["location_osm_name"] == "Carrefour City"
    assert product_price["location_osm_brand"] == "Carrefour"
    assert product_price["location_osm_tags"] == ["organic:only"]
    assert product_price["location_osm_version"] == 3
    assert product_price["location_tags"] == ["bulk"]

    assert category_price["product_id"] is None
    assert category_price["category_tag"] == "en:apples"
    assert category_price["tags"] == []
    assert category_price["duplicate_of"] == 1
    assert category_price["product_product_name"] is None
    assert category_price["proof_type"] == "RECEIPT"
