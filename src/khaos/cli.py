import asyncio
from importlib.metadata import version
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from khaos.errors import KhaosConnectionError
from khaos.executor import ExternalExecutor, LocalExecutor
from khaos.infrastructure.docker_manager import ClusterMode, DockerManager
from khaos.models.cluster import ClusterConfig, SaslMechanism, SecurityProtocol
from khaos.scenarios.loader import (
    discover_scenarios,
    get_scenario,
    list_scenarios_by_category,
)
from khaos.scenarios.scenario import SchemaRegistryConfig
from khaos.validators.scenario import validate_scenario_file

docker_manager = DockerManager()


def get_version() -> str:
    try:
        return version("khaos-cli")
    except Exception:
        return "dev"


app = typer.Typer(
    name="khaos",
    help="Kafka traffic generator for testing, learning, and chaos engineering",
    no_args_is_help=True,
)


def version_callback(value: bool) -> None:
    if value:
        print(f"khaos {get_version()}")
        raise typer.Exit()


@app.callback()
def main(
    version: Annotated[
        bool,
        typer.Option(
            "--version", "-v", callback=version_callback, is_eager=True, help="Show version"
        ),
    ] = False,
) -> None:
    pass


console = Console()


@app.command("cluster-up")
def cluster_up(
    mode: Annotated[
        ClusterMode,
        typer.Option("--mode", "-m", help="Cluster mode: kraft (default) or zookeeper"),
    ] = ClusterMode.KRAFT,
) -> None:
    try:
        docker_manager.cluster_up(mode=mode)
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
    status = docker_manager.cluster_status()

    if not status:
        console.print("[yellow]No Kafka containers found. Run 'khaos cluster-up' first.[/yellow]")
        return

    mode = docker_manager.get_active_mode()
    mode_label = "KRaft" if mode == ClusterMode.KRAFT else "ZooKeeper"

    table = Table(title=f"Kafka Cluster Status ({mode_label} mode)")
    table.add_column("Service", style="cyan")
    table.add_column("State", style="green")
    table.add_column("URL", style="blue")

    for service, info in sorted(status.items()):
        state = info["state"]
        url = info["url"]
        color = "green" if "running" in state.lower() else "red"
        table.add_row(service, f"[{color}]{state}[/{color}]", url)

    console.print(table)


@app.command("list")
def list_scenarios_cmd() -> None:
    categories = list_scenarios_by_category()

    if not categories:
        console.print("[yellow]No scenarios found.[/yellow]")
        return

    # Define category display order and descriptions
    category_info = {
        "traffic": ("Traffic Patterns", "Basic traffic generation scenarios"),
        "chaos": ("Chaos Engineering", "Fault injection and incident scenarios"),
        "flows": ("Event Flows", "Correlated multi-topic event flows"),
        "serialization": ("Serialization", "Avro and Protobuf format examples"),
    }

    table = Table(title="Available Scenarios")
    table.add_column("Scenario", style="cyan", no_wrap=True)
    table.add_column("Description", style="white")

    # Sort categories in preferred order
    sorted_categories = sorted(
        categories.keys(),
        key=lambda x: list(category_info.keys()).index(x) if x in category_info else 99,
    )

    for idx, category in enumerate(sorted_categories):
        scenarios = categories[category]
        display_name, category_desc = category_info.get(category, (category.title(), ""))

        if idx > 0:
            table.add_section()

        table.add_row(f"[bold magenta]{display_name}[/bold magenta]", f"[dim]{category_desc}[/dim]")

        for name, description in sorted(scenarios.items()):
            table.add_row(f"  {name}", description)

    console.print(table)
    console.print("\n[dim]Usage: khaos run <scenario>[/dim]")
    console.print("[dim]Example: khaos run traffic/high-throughput[/dim]")


@app.command("validate")
def validate_scenario(
    scenarios: Annotated[
        list[str] | None,
        typer.Argument(help="Scenario name(s) to validate (validates all if none specified)"),
    ] = None,
) -> None:
    available = discover_scenarios()

    if scenarios:
        to_validate = {}
        for name in scenarios:
            if name not in available:
                console.print(f"[bold red]Error: Scenario '{name}' not found[/bold red]")
                raise typer.Exit(1)
            to_validate[name] = available[name]
    else:
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
        str | None,
        typer.Option("--bootstrap-servers", "-b", help="Kafka bootstrap servers"),
    ] = None,
    mode: Annotated[
        ClusterMode,
        typer.Option("--mode", "-m", help="Cluster mode: kraft (default) or zookeeper"),
    ] = ClusterMode.KRAFT,
    no_consumers: Annotated[
        bool,
        typer.Option("--no-consumers", help="Disable built-in consumers (producer-only mode)"),
    ] = False,
) -> None:
    if not scenarios:
        console.print("[bold red]Error: Please specify at least one scenario[/bold red]")
        console.print("Use 'khaos list' to see available scenarios")
        raise typer.Exit(1)

    loaded_scenarios = []
    for scenario_name in scenarios:
        try:
            scenario = get_scenario(scenario_name)
            loaded_scenarios.append(scenario)
        except ValueError as e:
            console.print(f"[bold red]Error: {e}[/bold red]")
            raise typer.Exit(1)

    if not docker_manager.is_cluster_running():
        console.print("[cyan]Starting Kafka cluster...[/cyan]")
        docker_manager.cluster_up(mode=mode)

    if bootstrap_servers is None:
        bootstrap_servers = docker_manager.get_bootstrap_servers()

    scenario_names = ", ".join(s.name for s in loaded_scenarios)
    console.print(f"[bold blue]Running scenario(s): {scenario_names}[/bold blue]")
    mode_info = " | No consumers (producer-only)" if no_consumers else ""
    console.print(f"[dim]Duration: {duration}s | Bootstrap: {bootstrap_servers}{mode_info}[/dim]")

    if no_consumers:
        console.print("[cyan]Producer-only mode: connect your own consumers to consume data[/cyan]")

    try:
        executor = LocalExecutor(
            bootstrap_servers=bootstrap_servers,
            scenarios=loaded_scenarios,
            docker_manager=docker_manager,
            no_consumers=no_consumers,
        )
        result = asyncio.run(executor.start(duration_seconds=duration))

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
    security_protocol: Annotated[
        str,
        typer.Option(
            "--security-protocol",
            help="Security protocol: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL",
        ),
    ] = "PLAINTEXT",
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
    skip_topic_creation: Annotated[
        bool,
        typer.Option("--skip-topic-creation", help="Skip topic creation (topics already exist)"),
    ] = False,
    no_consumers: Annotated[
        bool,
        typer.Option("--no-consumers", help="Disable built-in consumers (producer-only mode)"),
    ] = False,
    schema_registry_url: Annotated[
        str | None,
        typer.Option("--schema-registry-url", help="Schema Registry URL for Avro/Protobuf schemas"),
    ] = None,
) -> None:
    if not bootstrap_servers:
        console.print("[bold red]Error: --bootstrap-servers is required[/bold red]")
        raise typer.Exit(1)

    if not scenarios:
        console.print("[bold red]Error: Please specify at least one scenario[/bold red]")
        console.print("Use 'kafka-sim list' to see available scenarios")
        raise typer.Exit(1)

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

    loaded_scenarios = []
    for scenario_name in scenarios:
        try:
            scenario = get_scenario(scenario_name)
            # Override schema registry URL if provided via CLI
            if schema_registry_url:
                scenario.schema_registry = SchemaRegistryConfig(url=schema_registry_url)
            loaded_scenarios.append(scenario)
        except ValueError as e:
            console.print(f"[bold red]Error: {e}[/bold red]")
            raise typer.Exit(1)

    scenario_names = ", ".join(s.name for s in loaded_scenarios)
    console.print(f"[bold blue]Running scenario(s): {scenario_names}[/bold blue]")
    mode_info = " | No consumers (producer-only)" if no_consumers else ""
    console.print(f"[dim]Duration: {duration}s | Bootstrap: {bootstrap_servers}{mode_info}[/dim]")
    console.print(f"[dim]Security: {security_protocol}[/dim]")
    if schema_registry_url:
        console.print(f"[dim]Schema Registry: {schema_registry_url}[/dim]")

    if no_consumers:
        console.print("[cyan]Producer-only mode: connect your own consumers to consume data[/cyan]")

    try:
        executor = ExternalExecutor(
            cluster_config=cluster_config,
            scenarios=loaded_scenarios,
            skip_topic_creation=skip_topic_creation,
            no_consumers=no_consumers,
        )
        result = asyncio.run(executor.start(duration_seconds=duration))

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
