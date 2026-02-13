import gzip
from pathlib import Path

import duckdb
import pytest

from openfoodfacts_exports.exports.csv.mobile import generate_mobile_app_dump


def test_generate_mobile_app_dump_file_not_found():
    with pytest.raises(FileNotFoundError):
        generate_mobile_app_dump(
            Path("/non/existent/path.parquet"), Path("/output/path.tsv.gz")
        )


def test_generate_mobile_app_dump_with_real_parquet(tmp_path: Path):
    parquet_path = tmp_path / "input.parquet"
    output_path = tmp_path / "output.tsv.gz"

    # Create a DuckDB connection and insert data
    con = duckdb.connect()
    con.execute(
        """CREATE TABLE test_table AS
            SELECT *
            FROM (VALUES 
                    ('1234567890123', 'Muesli aux fruits', '500 g',
                    'Carrefour,Carrefour bio', 'a', 4, 'a'),
                    ('1234567890124', 'Banana', '1', 'Chiquita', 'b', 3, 'b'),
                    ('1234567890125', 'Brown rice', '1 kg', 'Uncle Bens', 'c',
                    2, 'c')
                ) 
            AS t(code, product_name, quantity, brands, nutriscore_grade, nova_group,
            ecoscore_grade)
        """
    )

    # Export the table to a Parquet file
    con.execute(f"COPY test_table TO '{parquet_path}' (FORMAT 'parquet')")

    generate_mobile_app_dump(parquet_path, output_path)

    assert output_path.exists()
    with gzip.open(output_path, "rt") as f:
        lines = f.readlines()
        assert len(lines) == 4
        header = lines[0]
        assert "\t" in header
        fields = header.strip().split("\t")

        for field_name in (
            "code",
            "product_name",
            "quantity",
            "brands",
            "nutrition_grade_fr",
            "nova_group",
            "ecoscore_grade",
        ):
            assert field_name in fields

        assert lines[1].strip() == (
            "1234567890123\tMuesli aux fruits\t500 g\tCarrefour,Carrefour bio"
            "\ta\t4\ta"
        )
        assert lines[2].strip() == "1234567890124\tBanana\t1\tChiquita\tb\t3\tb"
        assert (
            lines[3].strip() == "1234567890125\tBrown rice\t1 kg\tUncle Bens\tc\t2\tc"
        )
