import json

import orjson
from openfoodfacts import APIVersion

from openfoodfacts_exports.exports.historical_events import ChangeAction, HistoryEvent
from openfoodfacts_exports.tasks import revisions
from openfoodfacts_exports.tasks.revisions import strip_product_from_user_ids


class FakeMinioClient:
    def put_object(
        self, bucket_name: str, object_name: str, data, length: int, content_type: str
    ):
        self.data_bytes = data.read()
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.content_type = content_type
        self.length = length


def test_upload_history_file():
    minio_client = FakeMinioClient()
    barcode = "5249429424921"
    rev_id = 10
    rev_id_2 = 11
    events = [
        HistoryEvent(
            id=f"{barcode}_{rev_id}",
            code=barcode,
            rev_id=rev_id,
            timestamp=1625097600,
            product_type="food",
            comment="Test comment",
            field="categories_tags",
            previous=["en:vegetables"],
            current=["en:fruits"],
            action=ChangeAction.CHANGE,
        ).model_dump(mode="json"),
        HistoryEvent(
            id=f"{barcode}_{rev_id_2}",
            code=barcode,
            rev_id=rev_id_2,
            timestamp=1625097900,
            product_type="food",
            comment="Add product name",
            field="product_name",
            previous=None,
            current="Pop corn with salt",
            action=ChangeAction.ADD,
        ).model_dump(mode="json"),
    ]
    revisions.upload_history_file(
        events=events, code=barcode, minio_client=minio_client
    )
    assert len(minio_client.data_bytes) == minio_client.length

    pushed_events = [
        orjson.loads(line) for line in minio_client.data_bytes.splitlines()
    ]
    assert pushed_events == events
    assert minio_client.content_type == "application/json"
    assert minio_client.bucket_name == "openfoodfacts-product-revisions"
    assert minio_client.object_name == f"raw/{barcode}/history.json"


class TestUploadRevision:
    _BARCODE = "3270160396337"
    _API_VERSION = APIVersion.v2

    def test_upload_revision(self, mocker):
        mock_client = mocker.MagicMock()
        product = {"rev": 12, "name": "Test Product"}
        revisions.upload_revision(
            mock_client,
            prefix=self._API_VERSION.value,
            code=self._BARCODE,
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
            prefix=self._API_VERSION.value,
            code=self._BARCODE,
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


def test_strip_product_from_user_ids():
    user_id = "user"
    product_legacy_image_schema = {
        "label_tags": ["en:organic"],
        "code": "3245968594852",
        "checkers_tags": [user_id],
        "correctors_tags": [user_id],
        "creator": user_id,
        "editors_tags": [user_id],
        "informers_tags": [user_id],
        "last_checker": user_id,
        "last_editor": user_id,
        "last_modified_by": user_id,
        "photographers_tags": [user_id],
        "images": {
            "1": {
                "sizes": {},
                "uploader": user_id,
                "uploaded_t": "1520424046",
            },
            "front_de": {
                "imgid": 1,
                "rev": 11,
            },
        },
    }
    assert strip_product_from_user_ids(product_legacy_image_schema) == {
        "label_tags": ["en:organic"],
        "code": "3245968594852",
        "images": {
            "1": {
                "sizes": {},
                "uploaded_t": "1520424046",
            },
            "front_de": {
                "imgid": 1,
                "rev": 11,
            },
        },
    }
    product_new_image_schema = product_legacy_image_schema.copy()
    product_new_image_schema["images"] = {
        "uploaded": {
            "1": {
                "sizes": {},
                "uploaded_t": "1520424046",
                "uploader": user_id,
            }
        },
        "selected": {
            "front": {
                "de": {
                    "imgid": 1,
                    "rev": 11,
                }
            }
        },
    }
    assert strip_product_from_user_ids(product_new_image_schema) == {
        "label_tags": ["en:organic"],
        "code": "3245968594852",
        "images": {
            "uploaded": {
                "1": {
                    "sizes": {},
                    "uploaded_t": "1520424046",
                }
            },
            "selected": {
                "front": {
                    "de": {
                        "imgid": 1,
                        "rev": 11,
                    }
                }
            },
        },
    }
