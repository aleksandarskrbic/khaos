from pathlib import Path

import yaml

from khaos.scenarios.scenario import Scenario

SCENARIOS_DIR = Path(__file__).parent.parent.parent.parent / "scenarios"


def load_scenario(path: Path) -> Scenario:
    with path.open() as f:
        data = yaml.safe_load(f)
    return Scenario.from_dict(data)


def discover_scenarios(base_dir: Path | None = None) -> dict[str, Path]:
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
    paths = discover_scenarios(base_dir)
    return {name: load_scenario(path) for name, path in paths.items()}


def get_scenario(name: str, base_dir: Path | None = None) -> Scenario:
    paths = discover_scenarios(base_dir)

    if name not in paths:
        available = ", ".join(sorted(paths.keys()))
        raise ValueError(f"Unknown scenario: '{name}'. Available: {available}")

    return load_scenario(paths[name])


def list_scenarios(base_dir: Path | None = None) -> dict[str, str]:
    scenarios = load_all_scenarios(base_dir)
    return {name: scenario.description for name, scenario in scenarios.items()}
