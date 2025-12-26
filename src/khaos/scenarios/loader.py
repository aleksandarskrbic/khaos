from pathlib import Path

import yaml

from khaos.scenarios.scenario import Scenario

SCENARIOS_DIR = Path(__file__).parent.parent.parent.parent / "scenarios"


def load_scenario(path: Path) -> Scenario:
    with path.open() as f:
        data = yaml.safe_load(f)
    return Scenario.from_dict(data)


def discover_scenarios(base_dir: Path | None = None) -> dict[str, Path]:
    """Discover all scenarios and return a dict mapping path-based names to file paths.

    Names are based on relative path from scenarios dir, e.g.:
    - scenarios/traffic/high-throughput.yaml -> "traffic/high-throughput"
    - scenarios/chaos/broker-chaos.yaml -> "chaos/broker-chaos"
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
                # Use relative path from base_dir as the scenario key
                relative_path = yaml_file.relative_to(base_dir)
                # Remove .yaml extension and convert to string with forward slashes
                scenario_key = str(relative_path.with_suffix("")).replace("\\", "/")
                scenarios[scenario_key] = yaml_file
        except Exception:
            continue

    return scenarios


def load_all_scenarios(base_dir: Path | None = None) -> dict[str, Scenario]:
    paths = discover_scenarios(base_dir)
    return {name: load_scenario(path) for name, path in paths.items()}


def get_scenario(name: str, base_dir: Path | None = None) -> Scenario:
    """Get a scenario by name or file path.

    Args:
        name: Either a built-in scenario name (e.g., 'traffic/high-throughput')
              or a path to a custom scenario file (e.g., './my-scenario.yaml')
        base_dir: Base directory for built-in scenarios (default: scenarios/)

    Returns:
        Loaded Scenario object
    """
    # Check if it's a file path (has .yaml extension or looks like a path)
    if name.endswith(".yaml") or name.endswith(".yml"):
        path = Path(name)
        if not path.exists():
            raise ValueError(f"Scenario file not found: '{name}'")
        return load_scenario(path)

    # Check if it's an absolute or relative path without extension
    path = Path(name)
    if path.is_absolute() or name.startswith("./") or name.startswith("../"):
        # Try with .yaml extension
        yaml_path = path.with_suffix(".yaml")
        if yaml_path.exists():
            return load_scenario(yaml_path)
        # Try with .yml extension
        yml_path = path.with_suffix(".yml")
        if yml_path.exists():
            return load_scenario(yml_path)
        raise ValueError(f"Scenario file not found: '{name}' (tried .yaml and .yml)")

    # Look up in built-in scenarios
    paths = discover_scenarios(base_dir)

    if name not in paths:
        available = ", ".join(sorted(paths.keys()))
        raise ValueError(f"Unknown scenario: '{name}'. Available: {available}")

    return load_scenario(paths[name])


def list_scenarios(base_dir: Path | None = None) -> dict[str, str]:
    scenarios = load_all_scenarios(base_dir)
    return {name: scenario.description for name, scenario in scenarios.items()}


def list_scenarios_by_category(base_dir: Path | None = None) -> dict[str, dict[str, str]]:
    """List scenarios grouped by category (subdirectory).

    Returns:
        Dict mapping category name to dict of {scenario_name: description}
    """
    scenarios = load_all_scenarios(base_dir)
    categories: dict[str, dict[str, str]] = {}

    for name, scenario in scenarios.items():
        category = name.split("/", 1)[0] if "/" in name else "other"

        if category not in categories:
            categories[category] = {}
        categories[category][name] = scenario.description

    return categories
