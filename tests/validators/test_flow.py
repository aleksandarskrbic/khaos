import pytest

from khaos.validators.flow import FlowValidator


@pytest.fixture
def validator():
    return FlowValidator()


class TestFlowValidatorBasics:
    def test_valid_minimal_flow(self, validator):
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
        result = validator.validate({"name": "bad"})
        assert not result.valid
        assert any("must be a list" in e.message for e in result.errors)

    def test_flow_must_be_dict(self, validator):
        result = validator.validate(["not-a-dict"])
        assert not result.valid
        assert any("must be an object/dict" in e.message for e in result.errors)


class TestFlowRequiredFields:
    def test_missing_name(self, validator):
        flows = [{"steps": [{"topic": "t", "event_type": "e"}]}]
        result = validator.validate(flows)
        assert not result.valid
        assert any("name" in e.path and "Missing" in e.message for e in result.errors)

    def test_name_must_be_string(self, validator):
        flows = [{"name": 123, "steps": [{"topic": "t", "event_type": "e"}]}]
        result = validator.validate(flows)
        assert not result.valid
        assert any("must be a string" in e.message for e in result.errors)

    def test_missing_steps(self, validator):
        flows = [{"name": "test"}]
        result = validator.validate(flows)
        assert not result.valid
        assert any("steps" in e.path and "Missing" in e.message for e in result.errors)

    def test_steps_must_be_list(self, validator):
        flows = [{"name": "test", "steps": {"topic": "t"}}]
        result = validator.validate(flows)
        assert not result.valid
        assert any("must be a list" in e.message for e in result.errors)


class TestFlowRateValidation:
    def test_valid_rate(self, validator):
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
    def test_valid_uuid_correlation(self, validator):
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
    def test_step_must_have_topic(self, validator):
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
    def test_valid_consumers(self, validator):
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
    def test_single_step_warning(self, validator):
        flows = [{"name": "test", "steps": [{"topic": "t", "event_type": "e"}]}]
        result = validator.validate(flows)
        assert result.valid
        assert any("fewer than 2 steps" in w.message for w in result.warnings)


class TestMultipleFlowsValidation:
    def test_validates_all_flows(self, validator):
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
