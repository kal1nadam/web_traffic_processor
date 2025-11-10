import typer
from app.adapters.duckdb_backend import init_db, calc_attributions, get_connection_test, fetch_orders
from app.services.order_processing_service import process_orders

app = typer.Typer(help="CLI application for managing tasks.")

@app.command("run")
def run_cmd(name: str):
    """
    Run the main application.
    """
    typer.echo(f"App is running successfully! Hello {name}")

@app.command("init-db")
def init_db_cmd():
    init_db()


@app.command("calc-attributions")
def calc_attributions_cmd():
    calc_attributions()

@app.command("test")
def test_cmd():
    print("Testing...")

    process_orders()
    # con = get_connection_test()
    # typer.echo(con.execute("SELECT * FROM events LIMIT 10").fetchdf()) 
    # con.execute("DROP TABLE events")
    # con.execute("DROP TABLE purchase_last_click_attributions")
    # con.execute("DROP VIEW events_seed;")

    # orders = process_orders()

    # for order in orders[:20]:
    #     typer.echo(order)


if __name__ == "__main__":
    app()