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


@pytest.mark.parametrize(
    "records,expected",
    [
        ([], (0, 0, 0)),
        ([{}, {}], (2, 0, 2)),
        ([{}, {}, {"code": "123"}, {}], (4, 1, 3)),
    ],
)
def test_conversion_counts_and_empty_batches(tmp_path, records, expected):
    import json

    import pyarrow.parquet as pq

    source = tmp_path / "input.jsonl"
    source.write_text("".join(json.dumps(record) + "\n" for record in records))
    output = tmp_path / "output.parquet"
    stats = convert_jsonl_to_parquet(
        output,
        source,
        BeautyProduct,
        BEAUTY_PRODUCT_SCHEMA,
        BEAUTY_DTYPE_MAP,
        batch_size=2,
    )
    assert (
        stats.records_read,
        stats.records_exported,
        stats.records_rejected,
    ) == expected
    if expected[1]:
        assert pq.read_table(output).column("code").to_pylist() == ["123"]
    else:
        assert not output.exists()


@pytest.mark.parametrize(
    "read,exported,rejected,limit,passes",
    [
        (0, 0, 0, 0.01, False),
        (2, 0, 2, 1, False),
        (100, 99, 1, 0.01, True),
        (100, 98, 2, 0.01, False),
        (100, 50, 50, 0.5, True),
        (100, 49, 51, 0.5, False),
        (1, 1, 0, -0.1, False),
        (1, 1, 0, 1.1, False),
    ],
)
def test_quality_limits(read, exported, rejected, limit, passes):
    from openfoodfacts_exports.exports.parquet import ExportStats

    stats = ExportStats(read, exported, rejected)
    if passes:
        stats.validate(limit)
    else:
        with pytest.raises(ValueError):
            stats.validate(limit)


@pytest.mark.parametrize("records", [[], [{}], [{"code": "123"}, {}]])
def test_invalid_export_preserves_previous_file_and_blocks_upload(
    tmp_path,
    monkeypatch,
    mocker,
    records,
):
    import json

    from openfoodfacts import Flavor

    from openfoodfacts_exports import settings
    from openfoodfacts_exports.exports.parquet import export_parquet

    source = tmp_path / "input.jsonl"
    source.write_text("".join(json.dumps(record) + "\n" for record in records))
    output = tmp_path / "beauty.parquet"
    output.write_bytes(b"previous export")
    monkeypatch.setattr(settings, "ENABLE_HF_PUSH", 1)
    monkeypatch.setattr(settings, "MAX_EXPORT_REJECTION_RATE", 0.01)
    upload = mocker.patch(
        "openfoodfacts_exports.exports.parquet.push_parquet_file_to_hf"
    )
    with pytest.raises(ValueError):
        export_parquet(source, output, Flavor.obf)
    upload.assert_not_called()
    assert output.read_bytes() == b"previous export"


def test_valid_export_is_published(tmp_path, monkeypatch, mocker):
    from openfoodfacts import Flavor

    from openfoodfacts_exports import settings
    from openfoodfacts_exports.exports.parquet import export_parquet

    source = tmp_path / "input.jsonl"
    source.write_text('{"code":"123"}\n')
    output = tmp_path / "beauty.parquet"
    monkeypatch.setattr(settings, "ENABLE_HF_PUSH", 1)
    monkeypatch.setattr(settings, "MAX_EXPORT_REJECTION_RATE", 0.01)
    upload = mocker.patch(
        "openfoodfacts_exports.exports.parquet.push_parquet_file_to_hf"
    )
    stats = export_parquet(source, output, Flavor.obf)
    assert stats.records_exported == 1
    assert output.exists()
    upload.assert_called_once_with(
        data_path=output, repo_id="openfoodfacts/product-database"
    )

