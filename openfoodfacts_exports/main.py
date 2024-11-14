import typer
from openfoodfacts import Flavor

app = typer.Typer()


@app.command()
def run_scheduler():
    """Run the scheduler, that will launch the export jobs once a day."""
    from openfoodfacts.utils import get_logger

    from openfoodfacts_exports.scheduler import run

    # configure root logger
    get_logger()
    run()


@app.command()
def run_worker(queues: list[str], burst: bool = False):
    """Run a worker for the given queues."""
    from openfoodfacts.utils import get_logger

    from openfoodfacts_exports.workers.main import run

    # configure root logger
    get_logger()
    run(queues, burst)


@app.command()
def launch_export(flavor: Flavor) -> None:
    """Launch an export job for a given flavor."""
    from openfoodfacts.utils import get_logger

    from openfoodfacts_exports.tasks import export_job
    from openfoodfacts_exports.utils import init_sentry

    # configure root logger
    get_logger()
    init_sentry()
    export_job(flavor)
