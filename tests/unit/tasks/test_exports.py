from pathlib import Path

import pytest
from openfoodfacts import Flavor

from openfoodfacts_exports import settings, tasks
from openfoodfacts_exports.types import ExportFlavor


@pytest.mark.parametrize("flavor", [ExportFlavor.off, ExportFlavor.obf])
def test_product_upload_depends_on_conversion(mocker, flavor):
    mocker.patch.object(tasks, "get_dataset", return_value=Path("input.jsonl"))
    enqueue = mocker.patch.object(tasks.high_queue, "enqueue")
    tasks.export_job(flavor)
    conversion, upload = enqueue.call_args_list[:2]
    assert conversion.args[0] is tasks.export_parquet
    assert conversion.args[3] == Flavor[flavor]
    assert upload.args[0] is tasks.upload_parquet
    assert upload.args[1] == conversion.args[2]
    assert upload.args[2] == "openfoodfacts/product-database"
    assert upload.kwargs["depends_on"] is enqueue.return_value
    assert upload.kwargs["retry"].max == 3
    if flavor == ExportFlavor.off:
        mobile = enqueue.call_args_list[2]
        assert mobile.args[0] is tasks.generate_push_mobile_app_dump
        assert mobile.kwargs["depends_on"] is enqueue.return_value
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
