from __future__ import annotations

from typing import Any

from khaos.validators.common import ValidationResult
from khaos.validators.schema import SchemaValidator

VALID_CORRELATION_TYPES = {"uuid", "field_ref"}


class FlowValidator:
    def __init__(self):
        self.schema_validator = SchemaValidator()

    def validate(self, flows: list[dict[str, Any]], base_path: str = "flows") -> ValidationResult:
        result = ValidationResult()

        if not isinstance(flows, list):
            result.add_error(base_path, "flows must be a list")
            return result

        for i, flow_def in enumerate(flows):
            path = f"{base_path}[{i}]"
            self._validate_flow(flow_def, path, result)

        return result

    def _validate_flow(self, flow_def: dict[str, Any], path: str, result: ValidationResult) -> None:
        if not isinstance(flow_def, dict):
            result.add_error(path, "Flow must be an object/dict")
            return

        if "name" not in flow_def:
            result.add_error(f"{path}.name", "Missing required field 'name'")
        elif not isinstance(flow_def["name"], str):
            result.add_error(f"{path}.name", "Field 'name' must be a string")

        if "rate" in flow_def:
            rate = flow_def["rate"]
            if not isinstance(rate, int | float) or rate <= 0:
                result.add_error(f"{path}.rate", "Field 'rate' must be a positive number")

        if "correlation" in flow_def:
            self._validate_correlation(flow_def["correlation"], f"{path}.correlation", result)

        if "steps" not in flow_def:
            result.add_error(f"{path}.steps", "Missing required field 'steps'")
            return

        steps = flow_def["steps"]
        if not isinstance(steps, list):
            result.add_error(f"{path}.steps", "Field 'steps' must be a list")
            return

        if len(steps) < 2:
            result.add_warning(
                f"{path}.steps",
                "Flow has fewer than 2 steps - consider using regular topics instead",
            )

        for i, step in enumerate(steps):
            self._validate_step(step, f"{path}.steps[{i}]", result, is_first=(i == 0))

    def _validate_correlation(
        self, correlation: dict[str, Any], path: str, result: ValidationResult
    ) -> None:
        if not isinstance(correlation, dict):
            result.add_error(path, "correlation must be an object/dict")
            return

        corr_type = correlation.get("type", "uuid")
        if corr_type not in VALID_CORRELATION_TYPES:
            result.add_error(
                f"{path}.type",
                f"Invalid correlation type '{corr_type}'. "
                f"Valid types: {', '.join(sorted(VALID_CORRELATION_TYPES))}",
            )

        if corr_type == "field_ref":
            if "field" not in correlation:
                result.add_error(
                    f"{path}.field",
                    "Correlation type 'field_ref' requires 'field' to be specified",
                )
            elif not isinstance(correlation["field"], str):
                result.add_error(f"{path}.field", "Field 'field' must be a string")

    def _validate_step(
        self,
        step: dict[str, Any],
        path: str,
        result: ValidationResult,
        is_first: bool = False,
    ) -> None:
        if not isinstance(step, dict):
            result.add_error(path, "Step must be an object/dict")
            return

        if "topic" not in step:
            result.add_error(f"{path}.topic", "Missing required field 'topic'")
        elif not isinstance(step["topic"], str):
            result.add_error(f"{path}.topic", "Field 'topic' must be a string")

        if "event_type" not in step:
            result.add_error(f"{path}.event_type", "Missing required field 'event_type'")
        elif not isinstance(step["event_type"], str):
            result.add_error(f"{path}.event_type", "Field 'event_type' must be a string")

        if "delay_ms" in step:
            delay = step["delay_ms"]
            if not isinstance(delay, int) or delay < 0:
                result.add_error(
                    f"{path}.delay_ms", "Field 'delay_ms' must be a non-negative integer"
                )
            if is_first and delay > 0:
                result.add_warning(
                    f"{path}.delay_ms",
                    "First step has delay_ms - delay applies before flow starts",
                )

        if "fields" in step:
            schema_result = self.schema_validator.validate(step["fields"], f"{path}.fields")
            for error in schema_result.errors:
                result.add_error(error.path, error.message)
            for warning in schema_result.warnings:
                result.add_warning(warning.path, warning.message)

        if "consumers" in step:
            self._validate_step_consumers(step["consumers"], f"{path}.consumers", result)

    def _validate_step_consumers(
        self, consumers: dict[str, Any], path: str, result: ValidationResult
    ) -> None:
        if not isinstance(consumers, dict):
            result.add_error(path, "consumers must be an object/dict")
            return

        if "groups" in consumers:
            groups = consumers["groups"]
            if not isinstance(groups, int) or groups < 1:
                result.add_error(f"{path}.groups", "Field 'groups' must be a positive integer")

        if "per_group" in consumers:
            per_group = consumers["per_group"]
            if not isinstance(per_group, int) or per_group < 1:
                result.add_error(
                    f"{path}.per_group", "Field 'per_group' must be a positive integer"
                )

        if "delay_ms" in consumers:
            delay = consumers["delay_ms"]
            if not isinstance(delay, int) or delay < 0:
                result.add_error(
                    f"{path}.delay_ms", "Field 'delay_ms' must be a non-negative integer"
                )
