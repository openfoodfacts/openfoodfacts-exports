import typer

app = typer.Typer()


@app.command()
def run_scheduler():
    from openfoodfacts.utils import get_logger

    from openfoodfacts_exports.scheduler import run

    # configure root logger
    get_logger()
    run()


@app.command()
def run_worker(
    queues: list[str] = typer.Argument(..., help="Names of the queues to listen to"),
    burst: bool = typer.Option(
        False, help="Run in burst mode (quit after all work is done)"
    ),
):
    """Launch a worker."""
    from openfoodfacts_exports.workers.main import run

    run(queues=queues, burst=burst)
