from pathlib import Path

import pytest

from openfoodfacts_exports.exports.parquet import convert_jsonl_to_parquet
from openfoodfacts_exports.exports.parquet.beauty import (
    BEAUTY_DTYPE_MAP,
    BEAUTY_PRODUCT_SCHEMA,
    BeautyProduct,
)
from openfoodfacts_exports.exports.parquet.common import Product


class TestConvertJSONLToParquet:
    def test_convert_jsonl_to_parquet_data_missing(self):
        non_existing_path = Path("non/existing/dataset/path")
        with pytest.raises(FileNotFoundError):
            convert_jsonl_to_parquet(
                output_file_path="any_path",
                dataset_path=non_existing_path,
                pydantic_cls=BeautyProduct,
                schema=BEAUTY_PRODUCT_SCHEMA,
                dtype_map=BEAUTY_DTYPE_MAP,
            )


PARSED_IMAGES_WITH_LEGACY_SCHEMA = [
    {
        "key": "1",
        "sizes": {
            "100": {"h": 100, "w": 56},
            "400": {"h": 400, "w": 225},
            "full": {"h": 3555, "w": 2000},
        },
        "uploaded_t": "1490702616",
        "uploader": "user1",
    },
    {
        "key": "nutrition_fr",
        "angle": None,
        "geometry": "0x0-0-0",
        "imgid": "1",
        "normalize": "0",
        "ocr": 1,
        "orientation": "0",
        "rev": "18",
        "sizes": {
            "100": {"h": 53, "w": 100},
            "200": {"h": 107, "w": 200},
            "400": {"h": 213, "w": 400},
            "full": {"h": 1093, "w": 2050},
        },
        "white_magic": "0",
        "x1": None,
        "x2": None,
        "y1": None,
        "y2": None,
    },
]


IMAGES_WITH_NEW_SCHEMA = {
    "uploaded": {
        "1": {
            "sizes": {
                "100": {
                    "h": 100,
                    "w": 56,
                    "url": "https://images.openfoodfacts.org/images/products/326/385/950/6216/1.100.jpg",
                },
                "400": {
                    "h": 400,
                    "w": 225,
                    "url": "https://images.openfoodfacts.org/images/products/326/385/950/6216/1.400.jpg",
                },
                "full": {
                    "h": 3555,
                    "w": 2000,
                    "url": "https://images.openfoodfacts.org/images/products/326/385/950/6216/1.jpg",
                },
            },
            "uploaded_t": "1490702616",
            "uploader": "user1",
        },
    },
    "selected": {
        "nutrition": {
            "fr": {
                "imgid": "1",
                "rev": "18",
                "sizes": {
                    "100": {
                        "h": 53,
                        "w": 100,
                        "url": "https://images.openfoodfacts.org/images/products/326/385/950/6216/nutrition_fr.18.100.jpg",
                    },
                    "200": {
                        "h": 107,
                        "w": 200,
                        "url": "https://images.openfoodfacts.org/images/products/326/385/950/6216/nutrition_fr.18.200.jpg",
                    },
                    "400": {
                        "h": 213,
                        "w": 400,
                        "url": "https://images.openfoodfacts.org/images/products/326/385/950/6216/nutrition_fr.18.400.jpg",
                    },
                    "full": {
                        "h": 1093,
                        "w": 2050,
                        "url": "https://images.openfoodfacts.org/images/products/326/385/950/6216/nutrition_fr.18.full.jpg",
                    },
                },
                "generation": {
                    "white_magic": "0",
                    "x1": None,
                    "x2": None,
                    "y1": None,
                    "y2": None,
                    "normalize": "0",
                    "ocr": 1,
                    "orientation": "0",
                    "angle": None,
                    "geometry": "0x0-0-0",
                },
            },
        }
    },
}


class TestProduct:
    def test_parse_images(self):
        assert Product.parse_images({"images": IMAGES_WITH_NEW_SCHEMA}) == {
            "images": PARSED_IMAGES_WITH_LEGACY_SCHEMA
        }
