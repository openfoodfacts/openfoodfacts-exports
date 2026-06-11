import os
from pathlib import Path

PROJECT_DIR = Path(__file__).parent.parent

REDIS_HOST = os.getenv("REDIS_HOST", "redis")

DATASET_DIR_STR = os.getenv("DATASET_DIR", PROJECT_DIR / "datasets")
DATASET_DIR = Path(DATASET_DIR_STR)

if not DATASET_DIR.exists():
    raise FileNotFoundError(f"{str(DATASET_DIR_STR)} was not found.")


SENTRY_DSN = os.environ.get("SENTRY_DSN")
ENVIRONMENT = os.environ.get("ENVIRONMENT", "dev")

ENABLE_HF_PUSH = int(os.getenv("ENABLE_HF_PUSH", "0"))

ENABLE_S3_PUSH = int(os.getenv("ENABLE_S3_PUSH", "0"))

AWS_S3_DATASET_BUCKET = os.getenv("AWS_S3_DATASET_BUCKET", "openfoodfacts-ds")
AWS_S3_IMAGE_BUCKET = os.getenv("AWS_S3_IMAGE_BUCKET", "openfoodfacts-images")
AWS_S3_REVISION_BUCKET = os.getenv(
    "AWS_S3_REVISION_BUCKET", "openfoodfacts-product-revisions"
)

# Remote Redis where Product Opener publishes product updates in a stream
REDIS_UPDATE_HOST = os.environ.get("REDIS_UPDATE_HOST", "localhost")
REDIS_UPDATE_PORT = int(os.environ.get("REDIS_UPDATE_PORT", 6379))


# Name of the Redis stream where Product Opener publishes product updates
PRODUCT_UPDATE_STREAM_NAME = os.environ.get(
    "PRODUCT_UPDATE_STREAM_NAME", "product_updates"
)
REDIS_LATEST_ID_KEY = os.environ.get(
    "REDIS_LATEST_ID_KEY", "openfoodfacts_exports:product_updates:latest_id"
)

USER_AGENT = os.environ.get("USER_AGENT", "openfoodfacts-export")
