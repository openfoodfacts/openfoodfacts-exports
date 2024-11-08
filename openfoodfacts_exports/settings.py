import os
from pathlib import Path

PROJECT_DIR = Path(__file__).parent.parent

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
