"""CLI smoke tests - verify commands don't crash and produce expected output."""

import re

from typer.testing import CliRunner

from khaos.cli import app

runner = CliRunner()


def strip_ansi(text: str) -> str:
    """Remove ANSI escape codes from text."""
    return re.sub(r"\x1b\[[0-9;]*m", "", text)


class TestHelpOutput:
    """Test --help works for all commands."""

    def test_main_help(self):
        """Main help displays usage info."""
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        output = strip_ansi(result.output).lower()
        assert "khaos" in output or "usage" in output

    def test_cluster_up_help(self):
        """cluster-up help displays options."""
        result = runner.invoke(app, ["cluster-up", "--help"])
        assert result.exit_code == 0
        assert "--mode" in strip_ansi(result.output)

    def test_cluster_down_help(self):
        """cluster-down help displays options."""
        result = runner.invoke(app, ["cluster-down", "--help"])
        assert result.exit_code == 0
        assert "--volumes" in strip_ansi(result.output)

    def test_cluster_status_help(self):
        """cluster-status help works."""
        result = runner.invoke(app, ["cluster-status", "--help"])
        assert result.exit_code == 0

    def test_list_help(self):
        """list help works."""
        result = runner.invoke(app, ["list", "--help"])
        assert result.exit_code == 0

    def test_validate_help(self):
        """validate help works."""
        result = runner.invoke(app, ["validate", "--help"])
        assert result.exit_code == 0

    def test_run_help(self):
        """run help displays all options."""
        result = runner.invoke(app, ["run", "--help"])
        assert result.exit_code == 0
        output = strip_ansi(result.output)
        assert "--duration" in output
        assert "--bootstrap-servers" in output
        assert "--no-consumers" in output

    def test_simulate_help(self):
        """simulate help displays security options."""
        result = runner.invoke(app, ["simulate", "--help"])
        assert result.exit_code == 0
        output = strip_ansi(result.output)
        assert "--security-protocol" in output
        assert "--sasl-mechanism" in output
        assert "--ssl-ca-location" in output


class TestListCommand:
    """Test the list command."""

    def test_list_shows_scenarios(self):
        """List command shows available scenarios."""
        result = runner.invoke(app, ["list"])
        assert result.exit_code == 0
        output = strip_ansi(result.output).lower()
        # Should show at least one built-in scenario
        assert "high-throughput" in output or "scenarios" in output


class TestValidateCommand:
    """Test the validate command."""

    def test_validate_all(self):
        """Validate all scenarios passes for built-in scenarios."""
        result = runner.invoke(app, ["validate"])
        # Should pass - all built-in scenarios should be valid
        assert result.exit_code == 0
        output = strip_ansi(result.output)
        assert "valid" in output.lower() or "✓" in output

    def test_validate_specific_scenario(self):
        """Validate a specific scenario."""
        result = runner.invoke(app, ["validate", "high-throughput"])
        assert result.exit_code == 0

    def test_validate_unknown_scenario_fails(self):
        """Validate unknown scenario fails with error."""
        result = runner.invoke(app, ["validate", "nonexistent-scenario-xyz"])
        assert result.exit_code == 1
        assert "not found" in strip_ansi(result.output).lower()


class TestRunCommand:
    """Test run command argument validation."""

    def test_run_no_scenario_shows_error(self):
        """Run without scenario shows helpful error."""
        result = runner.invoke(app, ["run"])
        assert result.exit_code == 1
        output = strip_ansi(result.output).lower()
        assert "specify" in output or "error" in output

    def test_run_unknown_scenario_fails(self):
        """Run with unknown scenario fails."""
        result = runner.invoke(app, ["run", "nonexistent-scenario-xyz"])
        assert result.exit_code == 1


class TestSimulateCommand:
    """Test simulate command argument validation."""

    def test_simulate_requires_bootstrap_servers(self):
        """Simulate without bootstrap servers shows error."""
        result = runner.invoke(app, ["simulate", "high-throughput"])
        assert result.exit_code == 1
        assert "bootstrap" in strip_ansi(result.output).lower()

    def test_simulate_no_scenario_shows_error(self):
        """Simulate without scenario shows error."""
        result = runner.invoke(app, ["simulate", "--bootstrap-servers", "localhost:9092"])
        assert result.exit_code == 1
        output = strip_ansi(result.output).lower()
        assert "specify" in output or "scenario" in output
