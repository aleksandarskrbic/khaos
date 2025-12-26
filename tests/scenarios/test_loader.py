from pathlib import Path

import pytest
import yaml

from khaos.scenarios.incidents import StopBrokerIncident
from khaos.scenarios.loader import (
    discover_scenarios,
    get_scenario,
    list_scenarios,
    load_scenario,
)
from khaos.scenarios.scenario import Scenario


def create_temp_scenario(data: dict, dir_path: Path, name: str = "test.yaml") -> Path:
    file_path = dir_path / name
    with file_path.open("w") as f:
        yaml.dump(data, f)
    return file_path


class TestLoadScenario:
    def test_loads_valid_scenario(self, tmp_path):
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
        data = {
            "name": "incident-scenario",
            "topics": [{"name": "events"}],
            "incidents": [{"type": "stop_broker", "at_seconds": 30, "broker": "kafka-1"}],
        }
        file_path = create_temp_scenario(data, tmp_path)

        scenario = load_scenario(file_path)

        assert len(scenario.incidents) == 1
        assert isinstance(scenario.incidents[0], StopBrokerIncident)
        assert scenario.incidents[0].broker == "kafka-1"

    def test_loads_scenario_with_incident_groups(self, tmp_path):
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
        with pytest.raises(FileNotFoundError):
            load_scenario(Path("/nonexistent/path.yaml"))

    def test_invalid_yaml(self, tmp_path):
        file_path = tmp_path / "invalid.yaml"
        file_path.write_text("invalid: yaml: content: [")

        with pytest.raises(yaml.YAMLError):
            load_scenario(file_path)


class TestDiscoverScenarios:
    def test_discovers_scenarios_in_directory(self, tmp_path):
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
        # Keys are now based on file path, not internal name
        assert "a" in scenarios
        assert "b" in scenarios

    def test_discovers_nested_scenarios(self, tmp_path):
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
        # Keys are now path-based: "root" and "traffic/nested"
        assert "root" in scenarios
        assert "traffic/nested" in scenarios

    def test_skips_invalid_yaml(self, tmp_path):
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
    def test_gets_existing_scenario(self, tmp_path):
        create_temp_scenario(
            {"name": "my-scenario", "description": "Test", "topics": [{"name": "t1"}]},
            tmp_path,
            "test.yaml",
        )

        # Now we use file path as key, not internal name
        scenario = get_scenario("test", tmp_path)

        assert scenario.name == "my-scenario"
        assert scenario.description == "Test"

    def test_raises_on_unknown_scenario(self, tmp_path):
        create_temp_scenario(
            {"name": "existing", "topics": [{"name": "t1"}]},
            tmp_path,
        )

        with pytest.raises(ValueError) as exc_info:
            get_scenario("nonexistent", tmp_path)

        assert "Unknown scenario" in str(exc_info.value)
        assert "nonexistent" in str(exc_info.value)

    def test_error_message_includes_available_scenarios(self, tmp_path):
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
        # Keys are now file paths without extension
        assert "one" in error_msg
        assert "two" in error_msg

    def test_gets_scenario_by_file_path(self, tmp_path):
        file_path = create_temp_scenario(
            {"name": "custom-scenario", "description": "Custom", "topics": [{"name": "t1"}]},
            tmp_path,
            "custom.yaml",
        )

        # Load by full file path
        scenario = get_scenario(str(file_path))

        assert scenario.name == "custom-scenario"
        assert scenario.description == "Custom"

    def test_gets_scenario_by_relative_path(self, tmp_path, monkeypatch):
        create_temp_scenario(
            {"name": "relative-scenario", "description": "Relative", "topics": [{"name": "t1"}]},
            tmp_path,
            "relative.yaml",
        )

        # Change to tmp_path directory
        monkeypatch.chdir(tmp_path)

        # Load by relative path
        scenario = get_scenario("./relative.yaml")

        assert scenario.name == "relative-scenario"

    def test_file_path_not_found_raises_error(self):
        with pytest.raises(ValueError) as exc_info:
            get_scenario("/nonexistent/path/scenario.yaml")

        assert "not found" in str(exc_info.value)


class TestListScenarios:
    def test_lists_scenarios_with_descriptions(self, tmp_path):
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

        # Keys are now file paths without extension
        assert scenarios == {
            "a": "Description A",
            "b": "Description B",
        }

    def test_empty_description_defaults_to_empty_string(self, tmp_path):
        create_temp_scenario(
            {"name": "no-desc", "topics": [{"name": "t1"}]},
            tmp_path,
            "test.yaml",
        )

        scenarios = list_scenarios(tmp_path)

        # Key is now file path without extension
        assert scenarios["test"] == ""
