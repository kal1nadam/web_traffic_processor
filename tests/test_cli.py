from typer.testing import CliRunner
from app.cli import app

runner = CliRunner()

def test_run_command():
    result = runner.invoke(app, ["Adam"])
    assert result.exit_code == 0
    assert "App is running successfully! Hello Adam" in result.output