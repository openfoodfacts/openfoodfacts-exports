import shutil
import tempfile
from pathlib import Path

import requests
from openfoodfacts.utils import download_file

from openfoodfacts_exports.exports.parquet import convert_jsonl_to_parquet


class TestConvertJSONLToParquet:
    def test_convert_jsonl_to_parquet_integration(
        self, output_dir: Path, update_results: bool
    ):
        expected_output_url = "https://raw.githubusercontent.com/openfoodfacts/test-data/refs/heads/main/openfoodfacts-exports/tests/openfoodfacts-min.parquet"
        dataset_url = "https://raw.githubusercontent.com/openfoodfacts/test-data/refs/heads/main/openfoodfacts-exports/tests/openfoodfacts-products-min.jsonl.gz"

        with tempfile.TemporaryDirectory() as tmpdirname:
            dataset_path = Path(tmpdirname) / "openfoodfacts-products-min.jsonl.gz"
            output_filename = "openfoodfacts-min.parquet"
            output_file_path = Path(tmpdirname) / output_filename
            download_file(dataset_url, dataset_path)

            is_output_available = requests.head(expected_output_url).status_code == 200
            convert_jsonl_to_parquet(
                output_file_path=output_file_path, dataset_path=dataset_path
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
