from pathlib import Path

import pytest

from openfoodfacts_exports.exports.parquet.common import push_parquet_file_to_hf


@pytest.mark.parametrize(
    "repo_id,filename,card_title",
    [
        ("openfoodfacts/open-prices", "prices.parquet", "# Open Prices"),
        (
            "openfoodfacts/product-database",
            "food.parquet",
            "# Open Food Facts Database",
        ),
        ("someone/custom-dataset", "custom.parquet", None),
    ],
)
def test_upload_commits_the_export_and_managed_card_together(
    tmp_path, mocker, repo_id, filename, card_title
):
    data_path = tmp_path / filename
    data_path.write_bytes(b"parquet fixture")
    hub = mocker.patch(
        "openfoodfacts_exports.exports.parquet.common.HfApi"
    ).return_value

    push_parquet_file_to_hf(data_path, repo_id, "preview", "Refresh export")

    hub.create_commit.assert_called_once()
    args = hub.create_commit.call_args.kwargs
    assert args["repo_id"] == repo_id
    assert args["revision"] == "preview"
    assert args["repo_type"] == "dataset"
    assert args["commit_message"] == "Refresh export"

    parquet, *cards = args["operations"]
    assert parquet.path_in_repo == filename
    assert Path(parquet.path_or_fileobj) == data_path
    if card_title is None:
        assert cards == []
    else:
        [card] = cards
        assert card.path_in_repo == "README.md"
        assert card_title in Path(card.path_or_fileobj).read_text(encoding="utf-8")
