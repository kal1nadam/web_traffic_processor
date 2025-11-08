import typer
from app.adapters.duckdb_backend import init_db, calc_purchase_attributions, get_connection_test

app = typer.Typer(help="CLI application for managing tasks.")

@app.command()
def run(name: str):
    """
    Run the main application.
    """
    typer.echo(f"App is running successfully! Hello {name}")

@app.command()
def test_db():
    init_db()

    # total = con.execute("SELECT COUNT(*) AS total FROM events").fetchone()[0]
    # total_attributions = con.execute("SELECT COUNT(*) AS total FROM last_click_attributions").fetchone()[0]
    # typer.echo(f"Total events in the database: {total}")
    # typer.echo(f"Total last click attributions in the database: {total_attributions}")
    
    # attributions = con.execute(
    #     """
    #     SELECT * FROM last_click_attributions LIMIT 100;
    #     """).fetchdf()
    
    # typer.echo(attributions)
    
@app.command()
def calc_attributions():
    calc_purchase_attributions()

@app.command()
def test():
    con = get_connection_test()
    typer.echo(con.execute("SELECT * FROM events LIMIT 10").fetchdf()) 
    # con.execute("DELETE FROM events")
    # con.execute("DELETE FROM purchase_last_click_attributions")

if __name__ == "__main__":
    app()