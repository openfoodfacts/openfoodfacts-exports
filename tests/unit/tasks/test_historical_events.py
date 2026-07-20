import datetime
from types import SimpleNamespace

import orjson
from openfoodfacts import APIVersion

from openfoodfacts_exports.tasks.historical_events import (
    generate_events_path,
    store_revision_events,
)


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
