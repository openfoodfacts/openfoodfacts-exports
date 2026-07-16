import json

import orjson
from openfoodfacts import APIVersion

from openfoodfacts_exports.tasks import revisions


class TestUploadRevision:
    _BARCODE = "3270160396337"
    _API_VERSION = APIVersion.v2

    def test_upload_revision(self, mocker):
        mock_client = mocker.MagicMock()
        product = {"rev": 12, "name": "Test Product"}
        revisions.upload_revision(
            mock_client,
            api_version=self._API_VERSION,
            barcode=self._BARCODE,
            product=product,
        )
        mock_client.put_object.assert_called_once()
        assert (
            mock_client.put_object.call_args.kwargs["bucket_name"]
            == "openfoodfacts-product-revisions"
        )
        assert (
            mock_client.put_object.call_args.kwargs["object_name"]
            == f"v2/{self._BARCODE}/12.json"
        )
        fp = mock_client.put_object.call_args.kwargs["data"]
        put_object_product = json.load(fp)
        assert put_object_product == product
        assert mock_client.put_object.call_args.kwargs["length"] == len(
            orjson.dumps(product)
        )
        assert (
            mock_client.put_object.call_args.kwargs["content_type"]
            == "application/json"
        )

    def test_upload_revision_set_as_latest(self, mocker):
        mock_client = mocker.MagicMock()
        product = {"rev": 12, "name": "Test Product"}
        revisions.upload_revision(
            mock_client,
            api_version=self._API_VERSION,
            barcode=self._BARCODE,
            product=product,
            set_as_latest=True,
        )
        assert mock_client.put_object.call_count == 2
        set_as_latest_call = mock_client.put_object.call_args_list[1]
        assert (
            set_as_latest_call.kwargs["bucket_name"]
            == "openfoodfacts-product-revisions"
        )
        assert (
            set_as_latest_call.kwargs["object_name"]
            == f"v2/{self._BARCODE}/latest.json"
        )
        fp = set_as_latest_call.kwargs["data"]
        put_object_product = json.load(fp)
        assert put_object_product == product
        assert set_as_latest_call.kwargs["length"] == len(orjson.dumps(product))
        assert set_as_latest_call.kwargs["content_type"] == "application/json"
