import logging

from rq import Queue

from openfoodfacts_exports.workers.redis import redis_conn

logger = logging.getLogger(__name__)


high_queue = Queue("off-exports-high", connection=redis_conn)
low_queue = Queue("off-exports-low", connection=redis_conn)
