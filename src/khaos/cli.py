"""CLI entry point for khaos - Kafka chaos engineering toolkit."""

import asyncio
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from khaos.errors import KhaosConnectionError
from khaos.infrastructure import docker_manager

app = typer.Typer(
    name="khaos",
    help="Kafka chaos engineering toolkit - generate realistic workloads and failure scenarios",
    no_args_is_help=True,
)

console = Console()


@app.command("cluster-up")
def cluster_up() -> None:
    """Start the 3-broker Kafka cluster."""
    try:
        docker_manager.cluster_up()
    except RuntimeError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise typer.Exit(1)
    except TimeoutError as e:
        console.print(f"[bold red]Timeout:[/bold red] {e}")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[bold red]Unexpected error:[/bold red] {e}")
        raise typer.Exit(1)


@app.command("cluster-down")
def cluster_down(
    volumes: Annotated[
        bool,
        typer.Option("--volumes", "-v", help="Remove data volumes"),
    ] = False,
) -> None:
    """Stop the Kafka cluster."""
    try:
        docker_manager.cluster_down(remove_volumes=volumes)
    except RuntimeError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[bold red]Unexpected error:[/bold red] {e}")
        raise typer.Exit(1)


@app.command("cluster-status")
def cluster_status() -> None:
    """Show Kafka cluster status."""
    status = docker_manager.cluster_status()

    if not status:
        console.print("[yellow]No Kafka containers found. Run 'khaos cluster-up' first.[/yellow]")
        return

    table = Table(title="Kafka Cluster Status")
    table.add_column("Service", style="cyan")
    table.add_column("State", style="green")

    for service, state in sorted(status.items()):
        color = "green" if "running" in state.lower() else "red"
        table.add_row(service, f"[{color}]{state}[/{color}]")

    console.print(table)


@app.command("list")
def list_scenarios_cmd() -> None:
    """List available traffic scenarios."""
    from khaos.scenarios.loader import list_scenarios

    scenarios = list_scenarios()

    if not scenarios:
        console.print("[yellow]No scenarios found.[/yellow]")
        return

    table = Table(title="Available Scenarios")
    table.add_column("Name", style="cyan", no_wrap=True)
    table.add_column("Description", style="white")

    for name, description in sorted(scenarios.items()):
        table.add_row(name, description)

    console.print(table)


@app.command("validate")
def validate_scenario(
    scenarios: Annotated[
        list[str] | None,
        typer.Argument(help="Scenario name(s) to validate (validates all if none specified)"),
    ] = None,
) -> None:
    """Validate scenario YAML definitions."""
    from khaos.scenarios.loader import discover_scenarios
    from khaos.scenarios.validator import validate_scenario_file

    available = discover_scenarios()

    if scenarios:
        # Validate specific scenarios
        to_validate = {}
        for name in scenarios:
            if name not in available:
                console.print(f"[bold red]Error: Scenario '{name}' not found[/bold red]")
                raise typer.Exit(1)
            to_validate[name] = available[name]
    else:
        # Validate all scenarios
        to_validate = available

    all_valid = True
    for name, path in sorted(to_validate.items()):
        result = validate_scenario_file(path)

        if result.valid and not result.warnings:
            console.print(f"[green]✓[/green] {name}")
        elif result.valid and result.warnings:
            console.print(f"[yellow]⚠[/yellow] {name}")
            for warning in result.warnings:
                console.print(f"  [yellow]Warning[/yellow] {warning.path}: {warning.message}")
        else:
            console.print(f"[red]✗[/red] {name}")
            for error in result.errors:
                console.print(f"  [red]Error[/red] {error.path}: {error.message}")
            for warning in result.warnings:
                console.print(f"  [yellow]Warning[/yellow] {warning.path}: {warning.message}")
            all_valid = False

    if all_valid:
        console.print(f"\n[bold green]All {len(to_validate)} scenario(s) valid![/bold green]")
    else:
        console.print("\n[bold red]Validation failed[/bold red]")
        raise typer.Exit(1)


@app.command("run")
def run_scenario(
    scenarios: Annotated[
        list[str] | None,
        typer.Argument(help="Scenario name(s) to run"),
    ] = None,
    duration: Annotated[
        int,
        typer.Option("--duration", "-d", help="Duration in seconds (0 = run until Ctrl+C)"),
    ] = 0,
    keep_cluster: Annotated[
        bool,
        typer.Option("--keep-cluster", "-k", help="Keep Kafka cluster running after scenario ends"),
    ] = False,
    bootstrap_servers: Annotated[
        str,
        typer.Option("--bootstrap-servers", "-b", help="Kafka bootstrap servers"),
    ] = "127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094",
) -> None:
    """Run one or more traffic simulation scenarios."""
    from khaos.scenarios.executor import ScenarioExecutor
    from khaos.scenarios.loader import get_scenario

    if not scenarios:
        console.print("[bold red]Error: Please specify at least one scenario[/bold red]")
        console.print("Use 'khaos list' to see available scenarios")
        raise typer.Exit(1)

    # Auto-start cluster if not running
    if not docker_manager.is_cluster_running():
        console.print("[cyan]Starting Kafka cluster...[/cyan]")
        docker_manager.cluster_up()

    # Load all requested scenarios
    loaded_scenarios = []
    for scenario_name in scenarios:
        try:
            scenario = get_scenario(scenario_name)
            loaded_scenarios.append(scenario)
        except ValueError as e:
            console.print(f"[bold red]Error: {e}[/bold red]")
            raise typer.Exit(1)

    scenario_names = ", ".join(s.name for s in loaded_scenarios)
    console.print(f"[bold blue]Running scenario(s): {scenario_names}[/bold blue]")
    console.print(f"[dim]Duration: {duration}s | Bootstrap: {bootstrap_servers}[/dim]")

    try:
        executor = ScenarioExecutor(
            bootstrap_servers=bootstrap_servers,
            scenarios=loaded_scenarios,
        )
        result = asyncio.run(executor.execute(duration_seconds=duration))

        console.print("\n[bold green]Scenario completed![/bold green]")
        console.print(f"  Messages produced: {result.messages_produced:,}")
        console.print(f"  Messages consumed: {result.messages_consumed:,}")
        console.print(f"  Duration: {result.duration_seconds:.1f}s")
        if result.errors:
            console.print(f"  [red]Errors: {len(result.errors)}[/red]")
            for error in result.errors[:5]:
                console.print(f"    - {error}")

    except KeyboardInterrupt:
        console.print("\n[yellow]Scenario interrupted by user[/yellow]")
    except KhaosConnectionError as e:
        console.print(f"[bold red]Connection Error:[/bold red] {e}")
        raise typer.Exit(1)
    except RuntimeError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[bold red]Unexpected error:[/bold red] {e}")
        raise typer.Exit(1)
    finally:
        if not keep_cluster:
            console.print("[cyan]Stopping Kafka cluster...[/cyan]")
            docker_manager.cluster_down()
        else:
            console.print(
                "[cyan]Kafka cluster left running (use 'khaos cluster-down' to stop)[/cyan]"
            )


@app.command("simulate")
def simulate_external(
    scenarios: Annotated[
        list[str] | None,
        typer.Argument(help="Scenario name(s) to run"),
    ] = None,
    bootstrap_servers: Annotated[
        str | None,
        typer.Option("--bootstrap-servers", "-b", help="Kafka bootstrap servers (required)"),
    ] = None,
    duration: Annotated[
        int,
        typer.Option("--duration", "-d", help="Duration in seconds (0 = run until Ctrl+C)"),
    ] = 0,
    # Security Protocol
    security_protocol: Annotated[
        str,
        typer.Option(
            "--security-protocol",
            help="Security protocol: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL",
        ),
    ] = "PLAINTEXT",
    # SASL Options
    sasl_mechanism: Annotated[
        str | None,
        typer.Option(
            "--sasl-mechanism", help="SASL mechanism: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512"
        ),
    ] = None,
    sasl_username: Annotated[
        str | None,
        typer.Option("--sasl-username", help="SASL username"),
    ] = None,
    sasl_password: Annotated[
        str | None,
        typer.Option("--sasl-password", help="SASL password"),
    ] = None,
    # SSL/TLS Options
    ssl_ca_location: Annotated[
        str | None,
        typer.Option("--ssl-ca-location", help="Path to CA certificate file"),
    ] = None,
    ssl_cert_location: Annotated[
        str | None,
        typer.Option("--ssl-cert-location", help="Path to client certificate (mTLS)"),
    ] = None,
    ssl_key_location: Annotated[
        str | None,
        typer.Option("--ssl-key-location", help="Path to client private key (mTLS)"),
    ] = None,
    ssl_key_password: Annotated[
        str | None,
        typer.Option("--ssl-key-password", help="Password for encrypted private key"),
    ] = None,
    # Execution options
    skip_topic_creation: Annotated[
        bool,
        typer.Option("--skip-topic-creation", help="Skip topic creation (topics already exist)"),
    ] = False,
) -> None:
    """Run traffic simulation against an external Kafka cluster.

    Unlike 'run', this command does NOT manage Docker infrastructure.
    Broker incidents (stop_broker, start_broker) are automatically skipped.

    Examples:

        # Confluent Cloud
        khaos simulate high-throughput \\
            --bootstrap-servers pkc-xxx.confluent.cloud:9092 \\
            --security-protocol SASL_SSL \\
            --sasl-mechanism PLAIN \\
            --sasl-username <API_KEY> \\
            --sasl-password <API_SECRET>

        # Self-hosted with mTLS
        khaos simulate high-throughput \\
            --bootstrap-servers kafka.example.com:9093 \\
            --security-protocol SSL \\
            --ssl-ca-location /path/to/ca.pem \\
            --ssl-cert-location /path/to/client.pem \\
            --ssl-key-location /path/to/client.key
    """
    from khaos.models.cluster import ClusterConfig, SaslMechanism, SecurityProtocol
    from khaos.scenarios.external_executor import ExternalScenarioExecutor
    from khaos.scenarios.loader import get_scenario

    # Validate required parameters
    if not bootstrap_servers:
        console.print("[bold red]Error: --bootstrap-servers is required[/bold red]")
        raise typer.Exit(1)

    if not scenarios:
        console.print("[bold red]Error: Please specify at least one scenario[/bold red]")
        console.print("Use 'kafka-sim list' to see available scenarios")
        raise typer.Exit(1)

    # Build ClusterConfig
    try:
        cluster_config = ClusterConfig(
            bootstrap_servers=bootstrap_servers,
            security_protocol=SecurityProtocol(security_protocol),
            sasl_mechanism=SaslMechanism(sasl_mechanism) if sasl_mechanism else None,
            sasl_username=sasl_username,
            sasl_password=sasl_password,
            ssl_ca_location=ssl_ca_location,
            ssl_cert_location=ssl_cert_location,
            ssl_key_location=ssl_key_location,
            ssl_key_password=ssl_key_password,
        )
    except ValueError as e:
        console.print(f"[bold red]Configuration error: {e}[/bold red]")
        raise typer.Exit(1)

    # Load scenarios
    loaded_scenarios = []
    for scenario_name in scenarios:
        try:
            scenario = get_scenario(scenario_name)
            loaded_scenarios.append(scenario)
        except ValueError as e:
            console.print(f"[bold red]Error: {e}[/bold red]")
            raise typer.Exit(1)

    scenario_names = ", ".join(s.name for s in loaded_scenarios)
    console.print(f"[bold blue]Running scenario(s): {scenario_names}[/bold blue]")
    console.print(f"[dim]Duration: {duration}s | Bootstrap: {bootstrap_servers}[/dim]")
    console.print(f"[dim]Security: {security_protocol}[/dim]")

    try:
        executor = ExternalScenarioExecutor(
            cluster_config=cluster_config,
            scenarios=loaded_scenarios,
            skip_topic_creation=skip_topic_creation,
        )
        result = asyncio.run(executor.execute(duration_seconds=duration))

        console.print("\n[bold green]Scenario completed![/bold green]")
        console.print(f"  Messages produced: {result.messages_produced:,}")
        console.print(f"  Messages consumed: {result.messages_consumed:,}")
        console.print(f"  Duration: {result.duration_seconds:.1f}s")
        if result.errors:
            console.print(f"  [red]Errors: {len(result.errors)}[/red]")
            for error in result.errors[:5]:
                console.print(f"    - {error}")

    except KeyboardInterrupt:
        console.print("\n[yellow]Scenario interrupted by user[/yellow]")
    except KhaosConnectionError as e:
        console.print(f"[bold red]Connection Error:[/bold red] {e}")
        console.print("[dim]Check bootstrap servers and network connectivity[/dim]")
        raise typer.Exit(1)
    except RuntimeError as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[bold red]Unexpected error:[/bold red] {e}")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
