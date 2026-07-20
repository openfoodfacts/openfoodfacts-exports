import datetime
import gzip
from types import SimpleNamespace

import orjson
from openfoodfacts import APIVersion

from openfoodfacts_exports.tasks.historical_events import (
    backfill_historical_events_to_file,
    generate_events_path,
    iter_backfill_events,
    store_revision_events,
)


def _make_product(products_dir, rel_dir, changes, snapshots):
    """Create a fake Product Opener product folder inside a products/ bundle."""
    product_dir = products_dir / rel_dir
    product_dir.mkdir(parents=True)
    (product_dir / "changes.json").write_bytes(orjson.dumps(changes))
    for rev, snapshot in snapshots.items():
        (product_dir / f"{rev}.json").write_bytes(orjson.dumps(snapshot))
    return product_dir


def _make_event(**overrides):
    """Build a minimal product update event with the attributes we read."""
    defaults = {
        "code": "7622210449283",
        "timestamp": datetime.datetime(
            2018, 10, 30, 20, 55, 33, tzinfo=datetime.timezone.utc
        ),
        "product_type": "food",
        "comment": "Updated brands",
        "diffs": {"fields": {"change": ["brands"]}},
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def test_generate_events_path():
    assert (
        generate_events_path(APIVersion.v2, "7622210449283", 252)
        == "historical_events/v2/7622210449283/252.jsonl"
    )


class TestStoreRevisionEvents:
    def test_stores_event_rows(self, mocker):
        """A field change is stored as a JSONL object in the revision bucket."""
        mock_client = mocker.MagicMock()
        event = _make_event()

        store_revision_events(
            mock_client,
            APIVersion.v2,
            event,
            previous_product={"rev": 251, "brands": "Nestlé"},
            current_product={"rev": 252, "brands": "Nestlé,Nescafé"},
        )

        mock_client.put_object.assert_called_once()
        kwargs = mock_client.put_object.call_args.kwargs
        assert kwargs["bucket_name"] == "openfoodfacts-product-revisions"
        assert kwargs["object_name"] == "historical_events/v2/7622210449283/252.jsonl"
        assert kwargs["content_type"] == "application/x-ndjson"
        body = kwargs["data"].read()
        assert kwargs["length"] == len(body)
        rows = [orjson.loads(line) for line in body.splitlines()]
        assert rows == [
            {
                "id": "7622210449283_252",
                "code": "7622210449283",
                "rev_id": 252,
                "timestamp": int(event.timestamp.timestamp()),
                "product_type": "food",
                "comment": "Updated brands",
                "field": "brands",
                "previous": "Nestlé",
                "current": "Nestlé,Nescafé",
                "action": "change",
            }
        ]

    def test_no_event_is_not_stored(self, mocker):
        """When the diff yields no field change, nothing is uploaded."""
        mock_client = mocker.MagicMock()
        event = _make_event(diffs=None)

        store_revision_events(
            mock_client,
            APIVersion.v2,
            event,
            previous_product=None,
            current_product={"rev": 252},
        )

        mock_client.put_object.assert_not_called()

    def test_missing_rev_is_not_stored(self, mocker):
        """A current product without a revision number is skipped."""
        mock_client = mocker.MagicMock()
        event = _make_event()

        store_revision_events(
            mock_client,
            APIVersion.v2,
            event,
            previous_product=None,
            current_product={"brands": "Nestlé"},
        )

        mock_client.put_object.assert_not_called()


class TestIterBackfillEvents:
    def test_add_then_change_across_two_revisions(self, tmp_path):
        """Each revision yields one row per changed field, with resolved values."""
        products_dir = tmp_path / "products"
        _make_product(
            products_dir,
            "200/000/000/1",
            changes=[
                {
                    "rev": 1,
                    "t": 100,
                    "comment": "created",
                    "diffs": {"fields": {"add": ["brands"]}},
                },
                {
                    "rev": 2,
                    "t": 200,
                    "comment": "update brands",
                    "diffs": {"fields": {"change": ["brands"]}},
                },
            ],
            snapshots={
                1: {"code": "2000000001", "product_type": "food", "brands": "Nestlé"},
                2: {
                    "code": "2000000001",
                    "product_type": "food",
                    "brands": "Nestlé,Nescafé",
                },
            },
        )

        events = list(iter_backfill_events(products_dir))

        assert events == [
            {
                "id": "2000000001_1",
                "code": "2000000001",
                "rev_id": 1,
                "timestamp": 100,
                "product_type": "food",
                "comment": "created",
                "field": "brands",
                "previous": None,
                "current": "Nestlé",
                "action": "add",
            },
            {
                "id": "2000000001_2",
                "code": "2000000001",
                "rev_id": 2,
                "timestamp": 200,
                "product_type": "food",
                "comment": "update brands",
                "field": "brands",
                "previous": "Nestlé",
                "current": "Nestlé,Nescafé",
                "action": "change",
            },
        ]

    def test_truncated_history_has_no_previous(self, tmp_path):
        """When the previous snapshot is missing, `previous` is left empty."""
        products_dir = tmp_path / "products"
        _make_product(
            products_dir,
            "200/000/000/2",
            changes=[
                {
                    "rev": 189,
                    "t": 300,
                    "comment": "first kept revision",
                    "diffs": {"fields": {"change": ["brands"]}},
                }
            ],
            snapshots={189: {"code": "2000000002", "brands": "Nestlé"}},
        )

        events = list(iter_backfill_events(products_dir))

        assert len(events) == 1
        assert events[0]["previous"] is None
        assert events[0]["current"] == "Nestlé"

    def test_contributor_field_values_are_stripped(self, tmp_path):
        """A changed contributor field carries no user id (stripped from snapshots)."""
        products_dir = tmp_path / "products"
        _make_product(
            products_dir,
            "200/000/000/3",
            changes=[
                {
                    "rev": 2,
                    "t": 400,
                    "comment": "edit",
                    "diffs": {"fields": {"change": ["last_editor"]}},
                }
            ],
            snapshots={
                1: {"code": "2000000003", "last_editor": "user_a"},
                2: {"code": "2000000003", "last_editor": "user_b"},
            },
        )

        events = list(iter_backfill_events(products_dir))

        assert len(events) == 1
        assert events[0]["field"] == "last_editor"
        assert events[0]["previous"] is None
        assert events[0]["current"] is None

    def test_missing_snapshot_is_skipped(self, tmp_path):
        """A change record without its snapshot is skipped, not fatal."""
        products_dir = tmp_path / "products"
        _make_product(
            products_dir,
            "200/000/000/4",
            changes=[
                {
                    "rev": 1,
                    "t": 500,
                    "comment": "created",
                    "diffs": {"fields": {"add": ["brands"]}},
                }
            ],
            snapshots={},
        )

        assert list(iter_backfill_events(products_dir)) == []


class TestBackfillHistoricalEventsToFile:
    def test_round_trip(self, tmp_path):
        """The written dump matches the generated event rows, one JSON per line."""
        products_dir = tmp_path / "products"
        _make_product(
            products_dir,
            "200/000/000/1",
            changes=[
                {
                    "rev": 1,
                    "t": 100,
                    "comment": "created",
                    "diffs": {"fields": {"add": ["brands"]}},
                }
            ],
            snapshots={1: {"code": "2000000001", "brands": "Nestlé"}},
        )
        output_path = tmp_path / "openfoodfacts_historical_events.jsonl.gz"

        backfill_historical_events_to_file(products_dir, output_path)

        with gzip.open(output_path, "rb") as fp:
            rows = [orjson.loads(line) for line in fp.read().splitlines()]
        assert rows == list(iter_backfill_events(products_dir))
        assert rows[0]["id"] == "2000000001_1"
