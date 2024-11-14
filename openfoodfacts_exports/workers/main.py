import sys

from rq import Worker

from openfoodfacts_exports.utils import init_sentry

from .redis import redis_conn

init_sentry()


def run(queues: list[str], burst: bool = False):
    try:
        w = Worker(queues=queues, connection=redis_conn)
        w.work(logging_level="INFO", burst=burst)
    except ConnectionError as e:
        print(e)
        sys.exit(1)
