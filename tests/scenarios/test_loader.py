"""Tests for scenario loader."""

from pathlib import Path

import pytest
import yaml

from khaos.scenarios.loader import (
    discover_scenarios,
    get_scenario,
    list_scenarios,
    load_scenario,
)
from khaos.scenarios.scenario import Scenario


def create_temp_scenario(data: dict, dir_path: Path, name: str = "test.yaml") -> Path:
    """Create a temporary scenario YAML file."""
    file_path = dir_path / name
    with file_path.open("w") as f:
        yaml.dump(data, f)
    return file_path


class TestLoadScenario:
    """Tests for load_scenario function."""

    def test_loads_valid_scenario(self, tmp_path):
        """Test loading a valid scenario file."""
        data = {
            "name": "test-scenario",
            "description": "A test scenario",
            "topics": [{"name": "test-topic", "partitions": 6}],
        }
        file_path = create_temp_scenario(data, tmp_path)

        scenario = load_scenario(file_path)

        assert isinstance(scenario, Scenario)
        assert scenario.name == "test-scenario"
        assert scenario.description == "A test scenario"
        assert len(scenario.topics) == 1

    def test_loads_scenario_with_incidents(self, tmp_path):
        """Test loading scenario with incidents."""
        data = {
            "name": "incident-scenario",
            "topics": [{"name": "events"}],
            "incidents": [{"type": "stop_broker", "at_seconds": 30, "broker": "kafka-1"}],
        }
        file_path = create_temp_scenario(data, tmp_path)

        scenario = load_scenario(file_path)

        assert len(scenario.incidents) == 1
        assert scenario.incidents[0].type == "stop_broker"

    def test_loads_scenario_with_incident_groups(self, tmp_path):
        """Test loading scenario with incident groups."""
        data = {
            "name": "group-scenario",
            "topics": [{"name": "events"}],
            "incidents": [
                {
                    "group": {
                        "repeat": 3,
                        "interval_seconds": 60,
                        "incidents": [
                            {"type": "stop_broker", "at_seconds": 0, "broker": "kafka-2"}
                        ],
                    }
                }
            ],
        }
        file_path = create_temp_scenario(data, tmp_path)

        scenario = load_scenario(file_path)

        assert len(scenario.incident_groups) == 1
        assert scenario.incident_groups[0].repeat == 3

    def test_file_not_found(self):
        """Test error when file doesn't exist."""
        with pytest.raises(FileNotFoundError):
            load_scenario(Path("/nonexistent/path.yaml"))

    def test_invalid_yaml(self, tmp_path):
        """Test error on invalid YAML."""
        file_path = tmp_path / "invalid.yaml"
        file_path.write_text("invalid: yaml: content: [")

        with pytest.raises(yaml.YAMLError):
            load_scenario(file_path)


class TestDiscoverScenarios:
    """Tests for discover_scenarios function."""

    def test_discovers_scenarios_in_directory(self, tmp_path):
        """Test discovering scenarios in directory."""
        create_temp_scenario(
            {"name": "scenario-a", "topics": [{"name": "t1"}]},
            tmp_path,
            "a.yaml",
        )
        create_temp_scenario(
            {"name": "scenario-b", "topics": [{"name": "t2"}]},
            tmp_path,
            "b.yaml",
        )

        scenarios = discover_scenarios(tmp_path)

        assert len(scenarios) == 2
        assert "scenario-a" in scenarios
        assert "scenario-b" in scenarios

    def test_discovers_nested_scenarios(self, tmp_path):
        """Test discovering scenarios in nested directories."""
        subdir = tmp_path / "traffic"
        subdir.mkdir()

        create_temp_scenario(
            {"name": "root-scenario", "topics": [{"name": "t1"}]},
            tmp_path,
            "root.yaml",
        )
        create_temp_scenario(
            {"name": "nested-scenario", "topics": [{"name": "t2"}]},
            subdir,
            "nested.yaml",
        )

        scenarios = discover_scenarios(tmp_path)

        assert len(scenarios) == 2
        assert "root-scenario" in scenarios
        assert "nested-scenario" in scenarios

    def test_skips_invalid_yaml(self, tmp_path):
        """Test that invalid YAML files are skipped."""
        create_temp_scenario(
            {"name": "valid", "topics": [{"name": "t1"}]},
            tmp_path,
            "valid.yaml",
        )

        invalid_file = tmp_path / "invalid.yaml"
        invalid_file.write_text("invalid: yaml: [")

        scenarios = discover_scenarios(tmp_path)

        assert len(scenarios) == 1
        assert "valid" in scenarios


class TestGetScenario:
    """Tests for get_scenario function."""

    def test_gets_existing_scenario(self, tmp_path):
        """Test getting an existing scenario."""
        create_temp_scenario(
            {"name": "my-scenario", "description": "Test", "topics": [{"name": "t1"}]},
            tmp_path,
        )

        scenario = get_scenario("my-scenario", tmp_path)

        assert scenario.name == "my-scenario"
        assert scenario.description == "Test"

    def test_raises_on_unknown_scenario(self, tmp_path):
        """Test error when scenario doesn't exist."""
        create_temp_scenario(
            {"name": "existing", "topics": [{"name": "t1"}]},
            tmp_path,
        )

        with pytest.raises(ValueError) as exc_info:
            get_scenario("nonexistent", tmp_path)

        assert "Unknown scenario" in str(exc_info.value)
        assert "nonexistent" in str(exc_info.value)

    def test_error_message_includes_available_scenarios(self, tmp_path):
        """Test that error message includes available scenarios."""
        create_temp_scenario(
            {"name": "available-one", "topics": [{"name": "t1"}]},
            tmp_path,
            "one.yaml",
        )
        create_temp_scenario(
            {"name": "available-two", "topics": [{"name": "t2"}]},
            tmp_path,
            "two.yaml",
        )

        with pytest.raises(ValueError) as exc_info:
            get_scenario("missing", tmp_path)

        error_msg = str(exc_info.value)
        assert "available-one" in error_msg
        assert "available-two" in error_msg


class TestListScenarios:
    """Tests for list_scenarios function."""

    def test_lists_scenarios_with_descriptions(self, tmp_path):
        """Test listing scenarios with their descriptions."""
        create_temp_scenario(
            {"name": "scenario-a", "description": "Description A", "topics": [{"name": "t1"}]},
            tmp_path,
            "a.yaml",
        )
        create_temp_scenario(
            {"name": "scenario-b", "description": "Description B", "topics": [{"name": "t2"}]},
            tmp_path,
            "b.yaml",
        )

        scenarios = list_scenarios(tmp_path)

        assert scenarios == {
            "scenario-a": "Description A",
            "scenario-b": "Description B",
        }

    def test_empty_description_defaults_to_empty_string(self, tmp_path):
        """Test scenario without description."""
        create_temp_scenario(
            {"name": "no-desc", "topics": [{"name": "t1"}]},
            tmp_path,
        )

        scenarios = list_scenarios(tmp_path)

        assert scenarios["no-desc"] == ""
