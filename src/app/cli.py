import typer
from app.adapters.duckdb_backend import init_in_memory_db

app = typer.Typer(help="CLI application for managing tasks.")

@app.command()
def run(name: str):
    """
    Run the main application.
    """
    typer.echo(f"App is running successfully! Hello {name}")

@app.command()
def test_db():
    con = init_in_memory_db()

    # typer.echo(con.execute("DESCRIBE events").fetchdf())

    # total = con.execute("SELECT COUNT(*) AS total FROM events").fetchone()[0]
    # total_attributions = con.execute("SELECT COUNT(*) AS total FROM last_click_attributions").fetchone()[0]
    # typer.echo(f"Total events in the database: {total}")
    # typer.echo(f"Total last click attributions in the database: {total_attributions}")
    
    attributions = con.execute(
        """
        SELECT * FROM last_click_attributions LIMIT 100;
        """).fetchdf()
    
    typer.echo(attributions)
    

if __name__ == "__main__":
    app()