from pathlib import Path

import pytest

from openfoodfacts_exports.exports.parquet import convert_jsonl_to_parquet
from openfoodfacts_exports.exports.parquet.beauty import (
    BEAUTY_DTYPE_MAP,
    BEAUTY_PRODUCT_SCHEMA,
    BeautyProduct,
)


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
