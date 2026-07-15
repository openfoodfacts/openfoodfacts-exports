import gzip
import json
from unittest.mock import MagicMock, patch

import orjson
from openfoodfacts import APIVersion, Environment, Flavor
from openfoodfacts.utils import AssetDownloadItem

from openfoodfacts_exports import tasks


class TestUploadNewImageToS3:
    @patch("openfoodfacts_exports.tasks.settings.ENABLE_S3_PUSH", False)
    def test_upload_new_image_to_s3_disabled(self, mocker):
        """When ENABLE_S3_PUSH is False, return early without calling S3/asset APIs."""
        mock_client = mocker.patch("openfoodfacts_exports.tasks.get_minio_client")
        mock_asset = mocker.patch("openfoodfacts_exports.tasks.get_asset_from_url")
        tasks.upload_new_image_to_s3("img1", "123", Flavor.off, Environment.org)
        mock_client.assert_not_called()
        mock_asset.assert_not_called()

    @patch("openfoodfacts_exports.tasks.settings.ENABLE_S3_PUSH", True)
    def test_upload_new_image_to_s3_success(self, mocker):
        """All assets return 200: 3 put_object calls (original, 400px, gzipped OCR)."""
        image_content = ("x" * 200).encode("utf-8")
        ocr_content = '{"ocr": "data"}'.encode("utf-8")

        def fake_get_asset_from_url(asset_url, **kwargs):
            is_ocr = asset_url.endswith(".json")
            content = ocr_content if is_ocr else image_content
            return AssetDownloadItem(
                url=asset_url, response=MagicMock(status_code=200, content=content)
            )

        mock_client = mocker.MagicMock()
        mocker.patch(
            "openfoodfacts_exports.tasks.get_minio_client", return_value=mock_client
        )
        mocker.patch(
            "openfoodfacts_exports.tasks.get_asset_from_url", fake_get_asset_from_url
        )
        tasks.upload_new_image_to_s3(
            image_id="1",
            barcode="3270160396337",
            flavor=Flavor.off,
            environment=Environment.org,
        )

        assert mock_client.put_object.call_count == 3

        calls = mock_client.put_object.call_args_list
        buckets = [c.kwargs["bucket_name"] for c in calls]
        assert all(b == "openfoodfacts-images" for b in buckets)

        object_names = [c.kwargs["object_name"] for c in calls]
        assert object_names == [
            "data/327/016/039/6337/1.jpg",
            "data/327/016/039/6337/1.400.jpg",
            "data/327/016/039/6337/1.json.gz",
        ]

        lengths = [c.kwargs["length"] for c in calls]
        assert lengths == [200, 200, len(gzip.compress(ocr_content))]

    @patch("openfoodfacts_exports.tasks.settings.ENABLE_S3_PUSH", True)
    def test_upload_new_image_to_s3_images_not_found(self, mocker):
        """Images return 404, OCR returns 200: only OCR is uploaded."""

        def fake_get_asset_from_url(asset_url, **kwargs):
            if asset_url.endswith(".jpg"):
                return AssetDownloadItem(
                    url=asset_url, response=MagicMock(status_code=404, content=b"")
                )
            return AssetDownloadItem(
                url=asset_url,
                response=MagicMock(
                    status_code=200, content='{"ocr": 1}'.encode("utf-8")
                ),
            )

        mock_client = mocker.MagicMock()
        mocker.patch(
            "openfoodfacts_exports.tasks.get_minio_client", return_value=mock_client
        )
        mocker.patch(
            "openfoodfacts_exports.tasks.get_asset_from_url",
            fake_get_asset_from_url,
        )
        tasks.upload_new_image_to_s3(
            image_id="1",
            barcode="3270160396337",
            flavor=Flavor.off,
            environment=Environment.org,
        )

        assert mock_client.put_object.call_count == 1
        calls = mock_client.put_object.call_args_list
        call = calls[0]
        assert call.kwargs["bucket_name"] == "openfoodfacts-images"
        assert call.kwargs["object_name"] == "data/327/016/039/6337/1.json.gz"

    @patch("openfoodfacts_exports.tasks.settings.ENABLE_S3_PUSH", True)
    def test_upload_new_image_to_s3_ocr_not_found(self, mocker):
        """Images return 200, OCR returns 404: 2 images are uploaded, no OCR upload."""
        image_content = b"image"

        def fake_get_asset_from_url(asset_url, **kwargs):
            if asset_url.endswith(".json"):
                return AssetDownloadItem(
                    url=asset_url, response=MagicMock(status_code=404, content=b"")
                )
            return AssetDownloadItem(
                url=asset_url,
                response=MagicMock(status_code=200, content=image_content),
            )

        mock_client = mocker.MagicMock()
        mocker.patch(
            "openfoodfacts_exports.tasks.get_minio_client", return_value=mock_client
        )
        mocker.patch(
            "openfoodfacts_exports.tasks.get_asset_from_url",
            fake_get_asset_from_url,
        )
        tasks.upload_new_image_to_s3(
            image_id="1",
            barcode="3270160396337",
            flavor=Flavor.off,
            environment=Environment.org,
        )

        assert mock_client.put_object.call_count == 2
        calls = mock_client.put_object.call_args_list
        assert calls[0].kwargs["object_name"] == "data/327/016/039/6337/1.jpg"
        assert calls[1].kwargs["object_name"] == "data/327/016/039/6337/1.400.jpg"

    @patch("openfoodfacts_exports.tasks.settings.ENABLE_S3_PUSH", True)
    def test_upload_new_image_to_s3_response_none(self, mocker):
        """When AssetDownloadItem.response is None (connection error), skip that
        asset."""

        def fake_get_asset_from_url(asset_url, **kwargs):
            return AssetDownloadItem(url=asset_url, response=None)

        mock_client = mocker.MagicMock()
        mocker.patch(
            "openfoodfacts_exports.tasks.get_minio_client", return_value=mock_client
        )
        mocker.patch(
            "openfoodfacts_exports.tasks.get_asset_from_url",
            fake_get_asset_from_url,
        )
        tasks.upload_new_image_to_s3(
            image_id="1",
            barcode="3270160396337",
            flavor=Flavor.off,
            environment=Environment.org,
        )

        # No uploads should happen since all responses are None
        mock_client.put_object.assert_not_called()


class TestUploadRevision:
    _BARCODE = "3270160396337"
    _API_VERSION = APIVersion.v2

    def test_upload_revision(self, mocker):
        mock_client = mocker.MagicMock()
        product = {"rev": 12, "name": "Test Product"}
        tasks.upload_revision(
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
        tasks.upload_revision(
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
