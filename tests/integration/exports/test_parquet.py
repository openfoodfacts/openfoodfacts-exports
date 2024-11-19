import shutil
import tempfile
from pathlib import Path
from urllib.parse import urlparse

import pyarrow as pa
import pytest
import requests
from openfoodfacts.utils import download_file

from openfoodfacts_exports.exports.parquet import convert_jsonl_to_parquet
from openfoodfacts_exports.exports.parquet.beauty import (
    BEAUTY_DTYPE_MAP,
    BEAUTY_PRODUCT_SCHEMA,
    BeautyProduct,
)
from openfoodfacts_exports.exports.parquet.common import Product
from openfoodfacts_exports.exports.parquet.food import (
    FOOD_DTYPE_MAP,
    FOOD_PRODUCT_SCHEMA,
    FoodProduct,
)


class TestConvertJSONLToParquet:
    @pytest.mark.parametrize(
        "dataset_url,expected_output_url,pydantic_cls,schema,dtype_map",
        [
            (
                "https://raw.githubusercontent.com/openfoodfacts/test-data/refs/heads/main/openfoodfacts-exports/tests/openfoodfacts-products-min.jsonl.gz",
                "https://raw.githubusercontent.com/openfoodfacts/test-data/refs/heads/main/openfoodfacts-exports/tests/openfoodfacts-min.parquet",
                FoodProduct,
                FOOD_PRODUCT_SCHEMA,
                FOOD_DTYPE_MAP,
            ),
            (
                "https://raw.githubusercontent.com/openfoodfacts/test-data/refs/heads/main/openfoodfacts-exports/tests/openbeautyfacts-products-min.jsonl.gz",
                "https://raw.githubusercontent.com/openfoodfacts/test-data/refs/heads/main/openfoodfacts-exports/tests/openbeautyfacts-min.parquet",
                BeautyProduct,
                BEAUTY_PRODUCT_SCHEMA,
                BEAUTY_DTYPE_MAP,
            ),
        ],
    )
    def test_convert_jsonl_to_parquet_integration(
        self,
        dataset_url: str,
        expected_output_url: str,
        pydantic_cls: type[Product],
        schema: pa.Schema,
        dtype_map: dict[str, pa.DataType],
        output_dir: Path,
        update_results: bool,
    ):
        with tempfile.TemporaryDirectory() as tmpdirname:
            dataset_path = Path(tmpdirname) / Path(urlparse(dataset_url).path).name
            output_filename = Path(urlparse(expected_output_url).path).name
            output_file_path = Path(tmpdirname) / output_filename
            download_file(dataset_url, dataset_path)

            is_output_available = requests.head(expected_output_url).status_code == 200
            convert_jsonl_to_parquet(
                output_file_path=output_file_path,
                dataset_path=dataset_path,
                pydantic_cls=pydantic_cls,
                schema=schema,
                dtype_map=dtype_map,
            )

            if update_results:
                output_dir.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(output_file_path, output_dir / output_filename)
            elif is_output_available:
                expected_output_file_path = Path(tmpdirname) / "expected_output.parquet"
                download_file(expected_output_url, expected_output_file_path)
                assert (
                    output_file_path.read_bytes()
                    == expected_output_file_path.read_bytes()
                )
            else:
                raise RuntimeError("No expected output available")
