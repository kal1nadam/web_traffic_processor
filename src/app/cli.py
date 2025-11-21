import typer
from app.adapters.duckdb_backend import init_db, calc_attributions
from app.services.order_processing_service import import_orders
from app.adapters.repositories.db_repository import DbRepository

app = typer.Typer(help="CLI application for managing tasks.")

@app.command("run")
def run_cmd(name: str):
    """
    Run the main application.
    """
    typer.echo(f"App is running successfully! Hello {name}")

@app.command("init-db")
def init_db_cmd():
    print("Initializing database...")
    init_db()
    print("Database initialized successfully.")

@app.command("calc-attributions")
def calc_attributions_cmd():
    print("Calculating attributions...")
    calc_attributions()
    print("Attributions calculated successfully.")

@app.command("import-orders")
def import_orders_cmd():
    print("Importing orders...")
    import_orders()
    print("Orders imported successfully.")

@app.command("get-orders")
def get_orders_cmd(page: int, page_size: int):
    repo = DbRepository()

    orders = repo.get_orders(page, page_size)
    for order in orders:
        typer.echo(order)


if __name__ == "__main__":
    app()