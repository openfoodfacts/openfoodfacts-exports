from pathlib import Path
from typing import Annotated

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
def upload_all_revisions(
    product_type: Annotated[
        str,
        typer.Argument(
            help="Type of product to upload revisions for (e.g. 'food', 'beauty',...)"
        ),
    ],
    root_dir: Annotated[
        Path,
        typer.Argument(
            exists=True,
            file_okay=False,
            help="Directory containing the product data to upload (ex: "
            "`/rpool/off-backups/podata-nvme/products/`)",
        ),
    ],
    upload_history: Annotated[
        bool,
        typer.Option(help="Generate and upload the history.json file for each product"),
    ] = True,
    overwrite: Annotated[
        bool,
        typer.Option(help="Overwrite existing revisions"),
    ] = False,
    only_codes: Annotated[
        list[str] | None,
        typer.Option(help="Only upload revisions for a list of barcodes"),
    ] = None,
) -> None:
    """Upload all revisions of all products of a given type from a given directory."""
    from openfoodfacts.utils import get_logger

    from openfoodfacts_exports.tasks.revisions import upload_all_revisions
    from openfoodfacts_exports.utils import init_sentry

    # configure root logger
    get_logger()
    init_sentry()
    upload_all_revisions(
        product_type=product_type,
        root_dir=root_dir,
        upload_history=upload_history,
        overwrite=overwrite,
        only_codes=only_codes,
    )
