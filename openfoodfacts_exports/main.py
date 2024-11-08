import typer
from openfoodfacts import Flavor

app = typer.Typer()


@app.command()
def run_scheduler():
    from openfoodfacts.utils import get_logger

    from openfoodfacts_exports.scheduler import run

    # configure root logger
    get_logger()
    run()


@app.command()
def launch_export(flavor: Flavor) -> None:
    """Launch an export job for a given flavor."""
    from openfoodfacts.utils import get_logger

    from openfoodfacts_exports.tasks import export_job

    # configure root logger
    get_logger()
    export_job(flavor)
