import pytz
from apscheduler.events import EVENT_JOB_ERROR
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.schedulers.blocking import BlockingScheduler
from openfoodfacts.utils import get_logger
from sentry_sdk import capture_exception

from openfoodfacts_exports.tasks import export_job
from openfoodfacts_exports.schemas import ExportFlavor
from openfoodfacts_exports.utils import init_sentry
from openfoodfacts_exports.workers.queues import high_queue

init_sentry()

logger = get_logger(__name__)


def exception_listener(event) -> None:
    if event.exception:
        capture_exception(event.exception)


def export_datasets() -> None:
    logger.info("Downloading dataset...")

    for flavor in (
        ExportFlavor.off,
        ExportFlavor.obf,
        ExportFlavor.opf,
        ExportFlavor.opff,
        ExportFlavor.op,
    ):
        high_queue.enqueue(export_job, flavor, job_timeout="1h", result_ttl=0)


# The scheduler is responsible for scheduling periodic work such as DB dump
# generation
def run() -> None:
    logger.info("Initializing scheduler")
    scheduler = BlockingScheduler(timezone=pytz.utc)
    scheduler.add_executor(ThreadPoolExecutor(10))
    scheduler.add_jobstore(MemoryJobStore())
    scheduler.add_job(export_datasets, "cron", hour=16, minute=0, max_instances=1)
    scheduler.add_listener(exception_listener, EVENT_JOB_ERROR)
    logger.info("Starting scheduler")
    scheduler.start()


if __name__ == "__main__":
    run()
