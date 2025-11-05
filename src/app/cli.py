import typer

app = typer.Typer(help="CLI application for managing tasks.")

@app.command()
def run(name: str):
    """
    Run the main application.
    """
    typer.echo(f"App is running successfully! Hello {name}")

if __name__ == "__main__":
    app()