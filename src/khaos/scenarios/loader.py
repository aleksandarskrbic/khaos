"""YAML scenario loader - discovers and parses scenario files."""

from pathlib import Path

import yaml

from khaos.scenarios.scenario import Scenario

SCENARIOS_DIR = Path(__file__).parent.parent.parent.parent / "scenarios"


def load_scenario(path: Path) -> Scenario:
    """Load a single scenario from a YAML file.

    Args:
        path: Path to the YAML file

    Returns:
        Scenario object
    """
    with path.open() as f:
        data = yaml.safe_load(f)
    return Scenario.from_dict(data)


def discover_scenarios(base_dir: Path | None = None) -> dict[str, Path]:
    """Discover all scenario YAML files in the scenarios directory.

    Args:
        base_dir: Base directory to search (defaults to built-in scenarios)

    Returns:
        Dict mapping scenario name to file path
    """
    if base_dir is None:
        base_dir = SCENARIOS_DIR

    if not base_dir.exists():
        return {}

    scenarios = {}

    for yaml_file in base_dir.rglob("*.yaml"):
        try:
            with yaml_file.open() as f:
                data = yaml.safe_load(f)
            if data and "name" in data:
                scenarios[data["name"]] = yaml_file
        except Exception:
            continue

    return scenarios


def load_all_scenarios(base_dir: Path | None = None) -> dict[str, Scenario]:
    """Load all scenarios from the scenarios directory.

    Args:
        base_dir: Base directory to search (defaults to built-in scenarios)

    Returns:
        Dict mapping scenario name to Scenario object
    """
    paths = discover_scenarios(base_dir)
    return {name: load_scenario(path) for name, path in paths.items()}


def get_scenario(name: str, base_dir: Path | None = None) -> Scenario:
    """Get a specific scenario by name.

    Args:
        name: Scenario name
        base_dir: Base directory to search (defaults to built-in scenarios)

    Returns:
        Scenario object

    Raises:
        ValueError: If scenario not found
    """
    paths = discover_scenarios(base_dir)

    if name not in paths:
        available = ", ".join(sorted(paths.keys()))
        raise ValueError(f"Unknown scenario: '{name}'. Available: {available}")

    return load_scenario(paths[name])


def list_scenarios(base_dir: Path | None = None) -> dict[str, str]:
    """List all available scenarios with descriptions.

    Args:
        base_dir: Base directory to search (defaults to built-in scenarios)

    Returns:
        Dict mapping scenario name to description
    """
    scenarios = load_all_scenarios(base_dir)
    return {name: scenario.description for name, scenario in scenarios.items()}
