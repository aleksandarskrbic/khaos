"""Tests for flow validator."""

import pytest

from khaos.schemas.flow_validator import FlowValidator


@pytest.fixture
def validator():
    return FlowValidator()


class TestFlowValidatorBasics:
    """Test basic flow validation."""

    def test_valid_minimal_flow(self, validator):
        """Minimal valid flow: name + 2 steps."""
        flows = [
            {
                "name": "test-flow",
                "steps": [
                    {"topic": "t1", "event_type": "created"},
                    {"topic": "t2", "event_type": "processed"},
                ],
            }
        ]
        result = validator.validate(flows)
        assert result.valid, f"Errors: {[e.message for e in result.errors]}"

    def test_valid_full_flow(self, validator):
        """Complete flow with all options."""
        flows = [
            {
                "name": "order-flow",
                "rate": 100,
                "correlation": {"type": "uuid"},
                "steps": [
                    {
                        "topic": "orders",
                        "event_type": "order_created",
                        "fields": [
                            {"name": "order_id", "type": "uuid"},
                            {"name": "amount", "type": "float", "min": 10.0, "max": 1000.0},
                        ],
                    },
                    {
                        "topic": "payments",
                        "event_type": "payment_initiated",
                        "delay_ms": 200,
                        "consumers": {"groups": 2, "per_group": 3, "delay_ms": 10},
                    },
                ],
            }
        ]
        result = validator.validate(flows)
        assert result.valid, f"Errors: {[e.message for e in result.errors]}"

    def test_flows_must_be_list(self, validator):
        """Flows must be a list, not a dict."""
        result = validator.validate({"name": "bad"})
        assert not result.valid
        assert any("must be a list" in e.message for e in result.errors)

    def test_flow_must_be_dict(self, validator):
        """Each flow must be a dict."""
        result = validator.validate(["not-a-dict"])
        assert not result.valid
        assert any("must be an object/dict" in e.message for e in result.errors)


class TestFlowRequiredFields:
    """Test required field validation."""

    def test_missing_name(self, validator):
        """Flow must have a name."""
        flows = [{"steps": [{"topic": "t", "event_type": "e"}]}]
        result = validator.validate(flows)
        assert not result.valid
        assert any("name" in e.path and "Missing" in e.message for e in result.errors)

    def test_name_must_be_string(self, validator):
        """Flow name must be a string."""
        flows = [{"name": 123, "steps": [{"topic": "t", "event_type": "e"}]}]
        result = validator.validate(flows)
        assert not result.valid
        assert any("must be a string" in e.message for e in result.errors)

    def test_missing_steps(self, validator):
        """Flow must have steps."""
        flows = [{"name": "test"}]
        result = validator.validate(flows)
        assert not result.valid
        assert any("steps" in e.path and "Missing" in e.message for e in result.errors)

    def test_steps_must_be_list(self, validator):
        """Steps must be a list."""
        flows = [{"name": "test", "steps": {"topic": "t"}}]
        result = validator.validate(flows)
        assert not result.valid
        assert any("must be a list" in e.message for e in result.errors)


class TestFlowRateValidation:
    """Test rate field validation."""

    def test_valid_rate(self, validator):
        """Valid positive rate."""
        flows = [
            {
                "name": "test",
                "rate": 50,
                "steps": [
                    {"topic": "t1", "event_type": "e1"},
                    {"topic": "t2", "event_type": "e2"},
                ],
            }
        ]
        result = validator.validate(flows)
        assert result.valid

    def test_rate_must_be_positive(self, validator):
        """Rate must be positive."""
        for rate in [0, -10]:
            flows = [
                {
                    "name": "test",
                    "rate": rate,
                    "steps": [
                        {"topic": "t", "event_type": "e"},
                        {"topic": "t2", "event_type": "e2"},
                    ],
                }
            ]
            result = validator.validate(flows)
            assert not result.valid
            assert any("positive" in e.message for e in result.errors)

    def test_rate_accepts_float(self, validator):
        """Rate can be a float (e.g., 0.5 flows per second)."""
        flows = [
            {
                "name": "test",
                "rate": 0.5,
                "steps": [
                    {"topic": "t1", "event_type": "e1"},
                    {"topic": "t2", "event_type": "e2"},
                ],
            }
        ]
        result = validator.validate(flows)
        assert result.valid


class TestCorrelationValidation:
    """Test correlation config validation."""

    def test_valid_uuid_correlation(self, validator):
        """UUID correlation type is valid."""
        flows = [
            {
                "name": "test",
                "correlation": {"type": "uuid"},
                "steps": [
                    {"topic": "t1", "event_type": "e1"},
                    {"topic": "t2", "event_type": "e2"},
                ],
            }
        ]
        result = validator.validate(flows)
        assert result.valid

    def test_valid_field_ref_correlation(self, validator):
        """Field ref correlation with field specified."""
        flows = [
            {
                "name": "test",
                "correlation": {"type": "field_ref", "field": "order_id"},
                "steps": [
                    {"topic": "t1", "event_type": "e1"},
                    {"topic": "t2", "event_type": "e2"},
                ],
            }
        ]
        result = validator.validate(flows)
        assert result.valid

    def test_invalid_correlation_type(self, validator):
        """Unknown correlation type fails."""
        flows = [
            {
                "name": "test",
                "correlation": {"type": "invalid"},
                "steps": [
                    {"topic": "t1", "event_type": "e1"},
                    {"topic": "t2", "event_type": "e2"},
                ],
            }
        ]
        result = validator.validate(flows)
        assert not result.valid
        assert any("Invalid correlation type" in e.message for e in result.errors)

    def test_field_ref_requires_field(self, validator):
        """Field ref type requires field to be specified."""
        flows = [
            {
                "name": "test",
                "correlation": {"type": "field_ref"},
                "steps": [
                    {"topic": "t1", "event_type": "e1"},
                    {"topic": "t2", "event_type": "e2"},
                ],
            }
        ]
        result = validator.validate(flows)
        assert not result.valid
        assert any("requires 'field'" in e.message for e in result.errors)

    def test_correlation_must_be_dict(self, validator):
        """Correlation must be a dict."""
        flows = [
            {
                "name": "test",
                "correlation": "uuid",
                "steps": [
                    {"topic": "t1", "event_type": "e1"},
                    {"topic": "t2", "event_type": "e2"},
                ],
            }
        ]
        result = validator.validate(flows)
        assert not result.valid
        assert any("must be an object/dict" in e.message for e in result.errors)


class TestStepValidation:
    """Test step validation."""

    def test_step_must_have_topic(self, validator):
        """Step must have topic."""
        flows = [
            {
                "name": "test",
                "steps": [{"event_type": "e1"}, {"topic": "t2", "event_type": "e2"}],
            }
        ]
        result = validator.validate(flows)
        assert not result.valid
        assert any("topic" in e.path for e in result.errors)

    def test_step_must_have_event_type(self, validator):
        """Step must have event_type."""
        flows = [
            {
                "name": "test",
                "steps": [{"topic": "t1"}, {"topic": "t2", "event_type": "e2"}],
            }
        ]
        result = validator.validate(flows)
        assert not result.valid
        assert any("event_type" in e.path for e in result.errors)

    def test_delay_ms_must_be_non_negative(self, validator):
        """delay_ms must be >= 0."""
        flows = [
            {
                "name": "test",
                "steps": [
                    {"topic": "t1", "event_type": "e1"},
                    {"topic": "t2", "event_type": "e2", "delay_ms": -100},
                ],
            }
        ]
        result = validator.validate(flows)
        assert not result.valid
        assert any("delay_ms" in e.path for e in result.errors)

    def test_first_step_delay_warning(self, validator):
        """Warning when first step has delay."""
        flows = [
            {
                "name": "test",
                "steps": [
                    {"topic": "t1", "event_type": "e1", "delay_ms": 100},
                    {"topic": "t2", "event_type": "e2"},
                ],
            }
        ]
        result = validator.validate(flows)
        assert result.valid
        assert any("First step has delay_ms" in w.message for w in result.warnings)


class TestStepConsumersValidation:
    """Test step consumer config validation."""

    def test_valid_consumers(self, validator):
        """Valid consumer config."""
        flows = [
            {
                "name": "test",
                "steps": [
                    {
                        "topic": "t1",
                        "event_type": "e1",
                        "consumers": {"groups": 2, "per_group": 3, "delay_ms": 10},
                    },
                    {"topic": "t2", "event_type": "e2"},
                ],
            }
        ]
        result = validator.validate(flows)
        assert result.valid

    def test_consumers_groups_must_be_positive(self, validator):
        """Consumer groups must be >= 1."""
        flows = [
            {
                "name": "test",
                "steps": [
                    {
                        "topic": "t1",
                        "event_type": "e1",
                        "consumers": {"groups": 0},
                    },
                    {"topic": "t2", "event_type": "e2"},
                ],
            }
        ]
        result = validator.validate(flows)
        assert not result.valid
        assert any("groups" in e.path and "positive" in e.message for e in result.errors)

    def test_consumers_per_group_must_be_positive(self, validator):
        """Consumer per_group must be >= 1."""
        flows = [
            {
                "name": "test",
                "steps": [
                    {
                        "topic": "t1",
                        "event_type": "e1",
                        "consumers": {"per_group": -1},
                    },
                    {"topic": "t2", "event_type": "e2"},
                ],
            }
        ]
        result = validator.validate(flows)
        assert not result.valid
        assert any("per_group" in e.path for e in result.errors)

    def test_consumers_delay_must_be_non_negative(self, validator):
        """Consumer delay_ms must be >= 0."""
        flows = [
            {
                "name": "test",
                "steps": [
                    {
                        "topic": "t1",
                        "event_type": "e1",
                        "consumers": {"delay_ms": -5},
                    },
                    {"topic": "t2", "event_type": "e2"},
                ],
            }
        ]
        result = validator.validate(flows)
        assert not result.valid
        assert any("delay_ms" in e.path for e in result.errors)

    def test_consumers_must_be_dict(self, validator):
        """Consumers must be a dict."""
        flows = [
            {
                "name": "test",
                "steps": [
                    {"topic": "t1", "event_type": "e1", "consumers": "invalid"},
                    {"topic": "t2", "event_type": "e2"},
                ],
            }
        ]
        result = validator.validate(flows)
        assert not result.valid
        assert any("must be an object/dict" in e.message for e in result.errors)


class TestFlowWarnings:
    """Test flow warnings."""

    def test_single_step_warning(self, validator):
        """Warning when flow has only one step."""
        flows = [{"name": "test", "steps": [{"topic": "t", "event_type": "e"}]}]
        result = validator.validate(flows)
        assert result.valid
        assert any("fewer than 2 steps" in w.message for w in result.warnings)


class TestMultipleFlowsValidation:
    """Test validation of multiple flows."""

    def test_validates_all_flows(self, validator):
        """All flows in the list are validated."""
        flows = [
            {
                "name": "flow-1",
                "steps": [
                    {"topic": "t1", "event_type": "e1"},
                    {"topic": "t2", "event_type": "e2"},
                ],
            },
            {
                "name": "flow-2",
                "steps": [
                    {"topic": "t3", "event_type": "e3"},
                    {"topic": "t4", "event_type": "e4"},
                ],
            },
        ]
        result = validator.validate(flows)
        assert result.valid

    def test_reports_errors_from_all_flows(self, validator):
        """Errors from multiple flows are all reported."""
        flows = [
            {"name": "flow-1"},  # missing steps
            {"steps": [{"topic": "t", "event_type": "e"}]},  # missing name
        ]
        result = validator.validate(flows)
        assert not result.valid
        assert len(result.errors) >= 2
        paths = [e.path for e in result.errors]
        assert any("flows[0]" in p for p in paths)
        assert any("flows[1]" in p for p in paths)
