import gzip

import orjson

from openfoodfacts_exports.exports.historical_events import (
    ChangeAction,
    FieldChange,
    RevisionInfo,
    flatten_diffs,
    generate_events,
    resolve_field_value,
    write_events_jsonl_gz,
)


class TestFlattenDiffs:
    def test_none_and_empty(self):
        """A missing or empty diff yields no field change."""
        assert flatten_diffs(None) == []
        assert flatten_diffs({}) == []

    def test_drops_the_category_split(self):
        """Changes from every category are merged into a single flat list."""
        diffs = {
            "fields": {"change": ["brands"]},
            "nutriments": {"change": ["energy"]},
        }
        assert flatten_diffs(diffs) == [
            FieldChange(field="brands", action=ChangeAction.CHANGE),
            FieldChange(field="energy", action=ChangeAction.CHANGE),
        ]

    def test_keeps_every_operation(self):
        """add, change and delete operations are all preserved."""
        diffs = {"fields": {"add": ["labels"], "change": ["brands"], "delete": ["url"]}}
        assert flatten_diffs(diffs) == [
            FieldChange(field="labels", action=ChangeAction.ADD),
            FieldChange(field="brands", action=ChangeAction.CHANGE),
            FieldChange(field="url", action=ChangeAction.DELETE),
        ]

    def test_ignores_unknown_operations_and_empty_lists(self):
        """Unknown operation keys and empty field lists are skipped."""
        diffs = {"fields": {"change": [], "unknown": ["brands"]}}
        assert flatten_diffs(diffs) == []

    def test_deduplicates_field_action_pairs(self):
        """The same (field, action) pair appearing twice is collapsed once."""
        diffs = {
            "fields": {"change": ["brands"]},
            "nutriments": {"change": ["brands"]},
        }
        assert flatten_diffs(diffs) == [
            FieldChange(field="brands", action=ChangeAction.CHANGE)
        ]


class TestResolveFieldValue:
    def test_top_level_field(self):
        assert resolve_field_value({"brands": "Nestlé"}, "brands") == "Nestlé"

    def test_nested_dotted_path(self):
        product = {"nutriments": {"energy": {"value": 42}}}
        assert resolve_field_value(product, "nutriments.energy.value") == 42

    def test_missing_field_returns_none(self):
        assert resolve_field_value({"brands": "Nestlé"}, "labels") is None

    def test_missing_nested_path_returns_none(self):
        assert resolve_field_value({"nutriments": {}}, "nutriments.energy") is None

    def test_none_product_returns_none(self):
        assert resolve_field_value(None, "brands") is None

    def test_non_dict_along_path_returns_none(self):
        assert resolve_field_value({"brands": "Nestlé"}, "brands.value") is None


class TestGenerateEvents:
    _REVISION = RevisionInfo(
        code="7622210449283",
        rev_id=252,
        timestamp=1540932933,
        product_type="food",
        comment="Updated brands",
    )

    def test_change_carries_previous_and_current(self):
        """A change reads the previous value and the current value."""
        events = generate_events(
            self._REVISION,
            diffs={"fields": {"change": ["brands"]}},
            previous_product={"brands": "Nestlé"},
            current_product={"brands": "Nestlé,Nescafé"},
        )
        assert events == [
            {
                "id": "7622210449283_252",
                "code": "7622210449283",
                "rev_id": 252,
                "timestamp": 1540932933,
                "product_type": "food",
                "comment": "Updated brands",
                "field": "brands",
                "previous": "Nestlé",
                "current": "Nestlé,Nescafé",
                "action": "change",
            }
        ]

    def test_add_has_no_previous_value(self):
        """An add leaves the previous value empty even if one is resolvable."""
        events = generate_events(
            self._REVISION,
            diffs={"fields": {"add": ["labels"]}},
            previous_product={"labels": "should be ignored"},
            current_product={"labels": "Organic"},
        )
        assert events[0]["previous"] is None
        assert events[0]["current"] == "Organic"
        assert events[0]["action"] == "add"

    def test_delete_has_no_current_value(self):
        """A delete leaves the current value empty even if one is resolvable."""
        events = generate_events(
            self._REVISION,
            diffs={"fields": {"delete": ["url"]}},
            previous_product={"url": "http://example.org"},
            current_product={"url": "should be ignored"},
        )
        assert events[0]["previous"] == "http://example.org"
        assert events[0]["current"] is None
        assert events[0]["action"] == "delete"

    def test_nested_field_path(self):
        """Nested dotted paths are resolved on both revisions."""
        events = generate_events(
            self._REVISION,
            diffs={"nutriments": {"change": ["nutriments.energy.value"]}},
            previous_product={"nutriments": {"energy": {"value": 42}}},
            current_product={"nutriments": {"energy": {"value": 50}}},
        )
        assert events[0]["previous"] == 42
        assert events[0]["current"] == 50

    def test_no_diff_yields_no_event(self):
        events = generate_events(
            self._REVISION,
            diffs=None,
            previous_product={"brands": "Nestlé"},
            current_product={"brands": "Nestlé"},
        )
        assert events == []


class TestWriteEventsJsonlGz:
    def test_round_trip(self, tmp_path):
        """Written rows can be read back as gzipped JSONL, one object per line."""
        events = [
            {"id": "1_1", "field": "brands", "action": "add"},
            {"id": "1_2", "field": "labels", "action": "change"},
        ]
        output_path = tmp_path / "off_historical_events.jsonl.gz"
        write_events_jsonl_gz(events, output_path)

        with gzip.open(output_path, "rb") as fp:
            lines = fp.read().splitlines()
        assert [orjson.loads(line) for line in lines] == events
