from redis import Redis

from openfoodfacts_exports import settings

redis_conn = Redis(host=settings.REDIS_HOST)
