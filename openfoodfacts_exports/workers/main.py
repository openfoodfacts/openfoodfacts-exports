import sys

from rq import Worker

from .redis import redis_conn


def run(queues: list[str], burst: bool = False):
    try:
        w = Worker(queues=queues, connection=redis_conn)
        w.work(logging_level="INFO", burst=burst)
    except ConnectionError as e:
        print(e)
        sys.exit(1)
