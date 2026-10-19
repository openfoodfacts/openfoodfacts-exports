from pathlib import Path

import pytest
from openfoodfacts import Flavor
from rq.job import Dependency

from openfoodfacts_exports import settings, tasks
from openfoodfacts_exports.types import ExportFlavor


@pytest.mark.parametrize("flavor", [ExportFlavor.off, ExportFlavor.obf])
def test_product_upload_depends_on_conversion(mocker, flavor):
    mocker.patch.object(tasks, "get_dataset", return_value=Path("input.jsonl"))
    jobs = [f"job-{index}" for index in range(18)]
    enqueue = mocker.patch.object(tasks.high_queue, "enqueue", side_effect=jobs)
    tasks.export_job(flavor)
    conversion, upload = enqueue.call_args_list[:2]
    assert conversion.args[0] is tasks.export_parquet
    assert conversion.args[3] == Flavor[flavor]
    assert upload.args[0] is tasks.upload_parquet
    assert upload.args[1] == conversion.args[2]
    assert upload.args[2] == "openfoodfacts/product-database"
    assert upload.kwargs["depends_on"] is jobs[0]
    assert upload.kwargs["retry"].max == 3
    if flavor == ExportFlavor.off:
        (
            plain_en,
            gzip_en_job,
            zstd_en_job,
            cleanup_en,
            gzip_en_upload,
            zstd_en_upload,
            plain_fr,
            gzip_fr_job,
            zstd_fr_job,
            cleanup_fr,
            gzip_fr_upload,
            zstd_fr_upload,
            mobile,
        ) = enqueue.call_args_list[2:]
        assert plain_en.args[0] is tasks.export_food_csv
        assert plain_en.args[1] == conversion.args[1]
        plain_path = plain_en.args[2]
        assert plain_path.parent == settings.DATASET_DIR
        assert plain_path.name.startswith("food-") and plain_path.suffix == ".csv"
        assert "depends_on" not in plain_en.kwargs
        for call, function, output_filename in (
            (
                gzip_en_job,
                tasks.compress_food_csv_gzip,
                "en.openfoodfacts.org.products.csv.gz",
            ),
            (
                zstd_en_job,
                tasks.compress_food_csv_zstd,
                "en.openfoodfacts.org.products.csv.zst",
            ),
        ):
            assert call.args == (
                function,
                plain_path,
                settings.DATASET_DIR / output_filename,
            )
            assert call.kwargs["depends_on"] is jobs[2]
        assert cleanup_en.args == (tasks.cleanup_food_csv, plain_path)
        dependency = cleanup_en.kwargs["depends_on"]
        assert isinstance(dependency, Dependency)
        assert dependency.dependencies == [jobs[3], jobs[4]]
        assert dependency.allow_failure is True
        for call, dependency, output_filename in (
            (gzip_en_upload, jobs[3], "en.openfoodfacts.org.products.csv.gz"),
            (zstd_en_upload, jobs[4], "en.openfoodfacts.org.products.csv.zst"),
        ):
            assert call.args == (
                tasks.upload_food_csv,
                settings.DATASET_DIR / output_filename,
            )
            assert call.kwargs["depends_on"] is dependency
            assert call.kwargs["retry"].max == 3
        assert mobile.args[0] is tasks.generate_push_mobile_app_dump
        assert mobile.kwargs["depends_on"] is jobs[0]
    else:
        assert enqueue.call_count == 2


def test_price_upload_depends_on_conversion(mocker):
    mocker.patch.object(tasks, "get_price_dataset", return_value=Path("input.jsonl"))
    enqueue = mocker.patch.object(tasks.high_queue, "enqueue")
    tasks.export_job(ExportFlavor.op)
    conversion, upload = enqueue.call_args_list
    assert conversion.args[0] is tasks.export_price_parquet
    assert upload.args[0] is tasks.upload_parquet
    assert upload.args[2] == "openfoodfacts/open-prices"
    assert upload.kwargs["depends_on"] is enqueue.return_value
    assert upload.kwargs["retry"].max == 3


def test_upload_retry_reuses_existing_file(tmp_path, monkeypatch, mocker):
    monkeypatch.setattr(settings, "ENABLE_HF_PUSH", 1)
    output = tmp_path / "food.parquet"
    output.write_bytes(b"existing export")
    upload = mocker.patch.object(
        tasks,
        "push_parquet_file_to_hf",
        side_effect=[ConnectionError, None],
    )
    convert = mocker.patch.object(tasks, "export_parquet")
    with pytest.raises(ConnectionError):
        tasks.upload_parquet(output, "openfoodfacts/product-database")
    tasks.upload_parquet(output, "openfoodfacts/product-database")
    assert upload.call_count == 2
    convert.assert_not_called()
    assert output.read_bytes() == b"existing export"


def test_upload_disabled(monkeypatch, mocker):
    monkeypatch.setattr(settings, "ENABLE_HF_PUSH", 0)
    upload = mocker.patch.object(tasks, "push_parquet_file_to_hf")
    tasks.upload_parquet(Path("missing.parquet"), "openfoodfacts/product-database")
    upload.assert_not_called()


def test_plain_csv_path_is_unique_per_run(mocker):
    mocker.patch.object(tasks, "get_dataset", return_value=Path("input.jsonl"))
    enqueue = mocker.patch.object(
        tasks.high_queue, "enqueue", side_effect=[f"job-{i}" for i in range(36)]
    )
    tasks.export_job(ExportFlavor.off)
    tasks.export_job(ExportFlavor.off)
    plain_paths = [
        call.args[2]
        for call in enqueue.call_args_list
        if call.args[0] is tasks.export_food_csv
    ]
    assert len(plain_paths) == 4
    assert len(set(plain_paths)) == 4
