import gzip
import logging

import orjson

from openfoodfacts_exports.exports.historical_events import ChangeAction, HistoryEvent
from openfoodfacts_exports.tasks.historical_events import (
    HISTORICAL_EVENTS_DUMP_PATH,
    fetch_product_history,
    generate_historical_events_dump,
    iter_product_codes,
    publish_historical_events_dump,
)

_MODULE = "openfoodfacts_exports.tasks.historical_events"


def _make_history_event(code: str, rev_id: int, field: str) -> HistoryEvent:
    return HistoryEvent(
        id=f"{code}_{rev_id}",
        code=code,
        rev_id=rev_id,
        timestamp=1625097600 + rev_id,
        product_type="food",
        comment="Test comment",
        field=field,
        previous=None,
        current="value",
        action=ChangeAction.ADD,
    )


def _make_bucket_entry(mocker, object_name: str, is_dir: bool):
    return mocker.MagicMock(object_name=object_name, is_dir=is_dir)


def _read_dump(path) -> list[dict]:
    with gzip.open(path, "rb") as f:
        return [orjson.loads(line) for line in f.read().splitlines()]


class TestIterProductCodes:
    def test_yields_product_directories_only(self, mocker):
        mock_client = mocker.MagicMock()
        mock_client.list_objects.return_value = [
            _make_bucket_entry(mocker, "json/3017620422003/", is_dir=True),
            _make_bucket_entry(mocker, "json/README.txt", is_dir=False),
            _make_bucket_entry(mocker, "json/5449000000996/", is_dir=True),
        ]

        codes = list(iter_product_codes(mock_client))

        assert codes == ["3017620422003", "5449000000996"]
        kwargs = mock_client.list_objects.call_args.kwargs
        assert kwargs["bucket_name"] == "openfoodfacts-product-revisions"
        assert kwargs["prefix"] == "json/"


class TestFetchProductHistory:
    def test_returns_history_events(self, mocker):
        events = [_make_history_event("3017620422003", 1, "product_name")]
        mocker.patch(f"{_MODULE}.get_history_events", return_value=events)

        assert fetch_product_history("3017620422003", mocker.MagicMock()) == events

    def test_missing_history_file_returns_empty_list(self, mocker):
        mocker.patch(f"{_MODULE}.get_history_events", return_value=None)

        assert fetch_product_history("3017620422003", mocker.MagicMock()) == []

    def test_error_is_logged_and_returns_empty_list(self, mocker, caplog):
        mocker.patch(
            f"{_MODULE}.get_history_events", side_effect=ValueError("invalid line")
        )

        with caplog.at_level(logging.ERROR):
            history_events = fetch_product_history("3017620422003", mocker.MagicMock())

        assert history_events == []
        assert "3017620422003" in caplog.text


class TestGenerateHistoricalEventsDump:
    def test_concatenates_history_files_in_listing_order(self, tmp_path, mocker):
        events_by_code = {
            "3017620422003": [
                _make_history_event("3017620422003", 1, "code"),
                _make_history_event("3017620422003", 2, "product_name"),
            ],
            # A product directory without a history.jsonl file
            "5449000000996": None,
            "3274080005003": [_make_history_event("3274080005003", 4, "brands")],
        }
        mock_client = mocker.MagicMock()
        mock_client.list_objects.return_value = [
            _make_bucket_entry(mocker, f"json/{code}/", is_dir=True)
            for code in events_by_code
        ]
        mocker.patch(
            f"{_MODULE}.get_history_events",
            side_effect=lambda code, minio_client: events_by_code[code],
        )
        output_path = tmp_path / "openfoodfacts_historical_events.jsonl.gz"

        generate_historical_events_dump(mock_client, output_path)

        rows = _read_dump(output_path)
        assert [(row["code"], row["rev_id"]) for row in rows] == [
            ("3017620422003", 1),
            ("3017620422003", 2),
            ("3274080005003", 4),
        ]
        assert rows[0] == events_by_code["3017620422003"][0].model_dump(mode="json")

    def test_order_is_kept_across_batches(self, tmp_path, mocker):
        codes = [f"{i:08d}" for i in range(5)]
        mock_client = mocker.MagicMock()
        mock_client.list_objects.return_value = [
            _make_bucket_entry(mocker, f"json/{code}/", is_dir=True) for code in codes
        ]
        mocker.patch(
            f"{_MODULE}.get_history_events",
            side_effect=lambda code, minio_client: [
                _make_history_event(code, 1, "code")
            ],
        )
        mocker.patch(f"{_MODULE}.DOWNLOAD_BATCH_SIZE", 2)
        output_path = tmp_path / "openfoodfacts_historical_events.jsonl.gz"

        generate_historical_events_dump(mock_client, output_path)

        assert [row["code"] for row in _read_dump(output_path)] == codes

    def test_failing_product_is_skipped(self, tmp_path, mocker):
        def get_history_events(code, minio_client):
            if code == "5449000000996":
                raise ValueError("invalid line")
            return [_make_history_event(code, 1, "code")]

        mock_client = mocker.MagicMock()
        mock_client.list_objects.return_value = [
            _make_bucket_entry(mocker, "json/3017620422003/", is_dir=True),
            _make_bucket_entry(mocker, "json/5449000000996/", is_dir=True),
            _make_bucket_entry(mocker, "json/3274080005003/", is_dir=True),
        ]
        mocker.patch(f"{_MODULE}.get_history_events", side_effect=get_history_events)
        output_path = tmp_path / "openfoodfacts_historical_events.jsonl.gz"

        generate_historical_events_dump(mock_client, output_path)

        assert [row["code"] for row in _read_dump(output_path)] == [
            "3017620422003",
            "3274080005003",
        ]

    def test_empty_bucket_produces_empty_dump(self, tmp_path, mocker):
        mock_client = mocker.MagicMock()
        mock_client.list_objects.return_value = []
        output_path = tmp_path / "openfoodfacts_historical_events.jsonl.gz"

        generate_historical_events_dump(mock_client, output_path)

        assert _read_dump(output_path) == []


class TestPublishHistoricalEventsDump:
    def test_uploads_dump_when_s3_enabled(self, mocker):
        mock_client = mocker.MagicMock()
        mocker.patch(f"{_MODULE}.get_minio_client", return_value=mock_client)
        mock_generate = mocker.patch(f"{_MODULE}.generate_historical_events_dump")
        mocker.patch(f"{_MODULE}.settings.ENABLE_S3_PUSH", 1)

        publish_historical_events_dump()

        mock_generate.assert_called_once_with(mock_client, HISTORICAL_EVENTS_DUMP_PATH)
        mock_client.fput_object.assert_called_once()
        call = mock_client.fput_object.call_args
        assert call.args[0] == "openfoodfacts-ds"
        assert call.args[1] == "openfoodfacts_historical_events.jsonl.gz"
        assert call.kwargs["file_path"] == str(HISTORICAL_EVENTS_DUMP_PATH)

    def test_skips_upload_when_s3_disabled(self, mocker):
        mock_client = mocker.MagicMock()
        mocker.patch(f"{_MODULE}.get_minio_client", return_value=mock_client)
        mock_generate = mocker.patch(f"{_MODULE}.generate_historical_events_dump")
        mocker.patch(f"{_MODULE}.settings.ENABLE_S3_PUSH", 0)

        publish_historical_events_dump()

        mock_generate.assert_called_once()
        mock_client.fput_object.assert_not_called()
