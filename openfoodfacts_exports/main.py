from pathlib import Path

import typer

from openfoodfacts_exports.types import ExportFlavor

app = typer.Typer()


@app.command()
def run_scheduler():
    """Run the scheduler, that will launch the export jobs once a day."""
    from openfoodfacts.utils import get_logger

    from openfoodfacts_exports.scheduler import run
    from openfoodfacts_exports.utils import init_sentry

    # configure root logger
    get_logger()
    init_sentry()

    run()


@app.command()
def run_update_listener():
    """Run the update listener, that listen for product updates and triggers appropriate
    actions."""
    from openfoodfacts.utils import get_logger

    from openfoodfacts_exports.update_listener import run_update_listener
    from openfoodfacts_exports.utils import init_sentry

    # configure root logger
    get_logger()
    init_sentry()
    run_update_listener()


@app.command()
def run_worker(queues: list[str], burst: bool = False):
    """Run a worker for the given queues."""
    from openfoodfacts.utils import get_logger

    from openfoodfacts_exports.utils import init_sentry
    from openfoodfacts_exports.workers.main import run

    # configure root logger
    get_logger()
    init_sentry()
    run(queues, burst)


@app.command()
def launch_export(flavor: ExportFlavor) -> None:
    """Launch an export job for a given flavor."""
    from openfoodfacts.utils import get_logger

    from openfoodfacts_exports.tasks import export_job
    from openfoodfacts_exports.utils import init_sentry

    # configure root logger
    get_logger()
    init_sentry()
    export_job(flavor)


@app.command()
def backfill_historical_events(products_dir: Path, output_path: Path) -> None:
    """Generate the historical events dump from a bundled products/ directory."""
    from openfoodfacts.utils import get_logger

    from openfoodfacts_exports.tasks.historical_events import (
        backfill_historical_events_to_file,
    )
    from openfoodfacts_exports.utils import init_sentry

    # configure root logger
    get_logger()
    init_sentry()
    backfill_historical_events_to_file(products_dir, output_path)
