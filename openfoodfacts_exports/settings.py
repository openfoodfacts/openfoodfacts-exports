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
