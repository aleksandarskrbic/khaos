from __future__ import annotations

from typing import Any

from khaos.models.schema import VALID_FIELD_TYPES
from khaos.validators.common import ValidationResult


class SchemaValidator:
    def validate(self, fields: list[dict[str, Any]], base_path: str = "fields") -> ValidationResult:
        result = ValidationResult()

        if not isinstance(fields, list):
            result.add_error(base_path, "fields must be a list")
            return result

        for i, field_def in enumerate(fields):
            path = f"{base_path}[{i}]"
            self._validate_field(field_def, path, result)

        return result

    def _validate_field(
        self, field_def: dict[str, Any], path: str, result: ValidationResult
    ) -> None:
        if not isinstance(field_def, dict):
            result.add_error(path, "Field must be an object/dict")
            return

        if "name" not in field_def:
            result.add_error(f"{path}.name", "Missing required field 'name'")
        elif not isinstance(field_def["name"], str):
            result.add_error(f"{path}.name", "Field 'name' must be a string")

        if "type" not in field_def:
            result.add_error(f"{path}.type", "Missing required field 'type'")
            return

        field_type = field_def["type"]
        if not isinstance(field_type, str):
            result.add_error(f"{path}.type", "Field 'type' must be a string")
            return

        if field_type not in VALID_FIELD_TYPES:
            result.add_error(
                f"{path}.type",
                f"Unknown field type '{field_type}'. "
                f"Valid types: {', '.join(sorted(VALID_FIELD_TYPES))}",
            )
            return

        if field_type == "string":
            self._validate_string_field(field_def, path, result)
        elif field_type == "int":
            self._validate_int_field(field_def, path, result)
        elif field_type == "float":
            self._validate_float_field(field_def, path, result)
        elif field_type == "enum":
            self._validate_enum_field(field_def, path, result)
        elif field_type == "object":
            self._validate_object_field(field_def, path, result)
        elif field_type == "array":
            self._validate_array_field(field_def, path, result)
        elif field_type == "faker":
            self._validate_faker_field(field_def, path, result)

    def _validate_string_field(
        self, field_def: dict[str, Any], path: str, result: ValidationResult
    ) -> None:
        if "min_length" in field_def:
            if not isinstance(field_def["min_length"], int) or field_def["min_length"] < 0:
                result.add_error(
                    f"{path}.min_length", "Field 'min_length' must be a non-negative integer"
                )

        if "max_length" in field_def:
            if not isinstance(field_def["max_length"], int) or field_def["max_length"] < 1:
                result.add_error(
                    f"{path}.max_length", "Field 'max_length' must be a positive integer"
                )

        min_len = field_def.get("min_length", 0)
        max_len = field_def.get("max_length", 100)
        if isinstance(min_len, int) and isinstance(max_len, int) and min_len > max_len:
            result.add_error(f"{path}", "min_length cannot be greater than max_length")

        if "cardinality" in field_def:
            if not isinstance(field_def["cardinality"], int) or field_def["cardinality"] < 1:
                result.add_error(
                    f"{path}.cardinality", "Field 'cardinality' must be a positive integer"
                )

    def _validate_numeric_range(
        self, field_def: dict[str, Any], path: str, result: ValidationResult
    ) -> None:
        if "min" in field_def:
            if not isinstance(field_def["min"], int | float):
                result.add_error(f"{path}.min", "Field 'min' must be a number")

        if "max" in field_def:
            if not isinstance(field_def["max"], int | float):
                result.add_error(f"{path}.max", "Field 'max' must be a number")

        min_val = field_def.get("min")
        max_val = field_def.get("max")
        if (
            min_val is not None
            and max_val is not None
            and isinstance(min_val, int | float)
            and isinstance(max_val, int | float)
            and min_val > max_val
        ):
            result.add_error(f"{path}", "min cannot be greater than max")

    def _validate_int_field(
        self, field_def: dict[str, Any], path: str, result: ValidationResult
    ) -> None:
        self._validate_numeric_range(field_def, path, result)

        if "cardinality" in field_def:
            if not isinstance(field_def["cardinality"], int) or field_def["cardinality"] < 1:
                result.add_error(
                    f"{path}.cardinality", "Field 'cardinality' must be a positive integer"
                )

    def _validate_float_field(
        self, field_def: dict[str, Any], path: str, result: ValidationResult
    ) -> None:
        self._validate_numeric_range(field_def, path, result)

    def _validate_enum_field(
        self, field_def: dict[str, Any], path: str, result: ValidationResult
    ) -> None:
        if "values" not in field_def:
            result.add_error(f"{path}.values", "Enum field requires 'values' list")
            return

        values = field_def["values"]
        if not isinstance(values, list):
            result.add_error(f"{path}.values", "Field 'values' must be a list")
            return

        if len(values) == 0:
            result.add_error(f"{path}.values", "Enum must have at least one value")

        for i, val in enumerate(values):
            if not isinstance(val, str):
                result.add_error(f"{path}.values[{i}]", "Enum values must be strings")

    def _validate_object_field(
        self, field_def: dict[str, Any], path: str, result: ValidationResult
    ) -> None:
        if "fields" not in field_def:
            result.add_error(f"{path}.fields", "Object field requires 'fields' list")
            return

        fields = field_def["fields"]
        if not isinstance(fields, list):
            result.add_error(f"{path}.fields", "Field 'fields' must be a list")
            return

        if len(fields) == 0:
            result.add_error(f"{path}.fields", "Object must have at least one field")

        for i, nested_field in enumerate(fields):
            self._validate_field(nested_field, f"{path}.fields[{i}]", result)

    def _validate_array_field(
        self, field_def: dict[str, Any], path: str, result: ValidationResult
    ) -> None:
        if "items" not in field_def:
            result.add_error(f"{path}.items", "Array field requires 'items' schema")
            return

        items = field_def["items"]
        if not isinstance(items, dict):
            result.add_error(f"{path}.items", "Field 'items' must be an object/dict")
            return

        if "min_items" in field_def:
            if not isinstance(field_def["min_items"], int) or field_def["min_items"] < 0:
                result.add_error(
                    f"{path}.min_items", "Field 'min_items' must be a non-negative integer"
                )

        if "max_items" in field_def:
            if not isinstance(field_def["max_items"], int) or field_def["max_items"] < 1:
                result.add_error(
                    f"{path}.max_items", "Field 'max_items' must be a positive integer"
                )

        min_items = field_def.get("min_items", 1)
        max_items = field_def.get("max_items", 5)
        if isinstance(min_items, int) and isinstance(max_items, int) and min_items > max_items:
            result.add_error(f"{path}", "min_items cannot be greater than max_items")

        self._validate_array_item(items, f"{path}.items", result)

    def _validate_faker_field(
        self, field_def: dict[str, Any], path: str, result: ValidationResult
    ) -> None:
        if "provider" not in field_def:
            result.add_error(f"{path}.provider", "Faker field requires 'provider'")
            return

        provider = field_def["provider"]
        if not isinstance(provider, str):
            result.add_error(f"{path}.provider", "Field 'provider' must be a string")
            return

        # Validate locale if provided
        if "locale" in field_def and not isinstance(field_def["locale"], str):
            result.add_error(f"{path}.locale", "Field 'locale' must be a string")

    def _validate_array_item(
        self, item_def: dict[str, Any], path: str, result: ValidationResult
    ) -> None:
        if not isinstance(item_def, dict):
            result.add_error(path, "Array item must be an object/dict")
            return

        if "type" not in item_def:
            result.add_error(f"{path}.type", "Missing required field 'type'")
            return

        field_type = item_def["type"]
        if not isinstance(field_type, str):
            result.add_error(f"{path}.type", "Field 'type' must be a string")
            return

        if field_type not in VALID_FIELD_TYPES:
            result.add_error(
                f"{path}.type",
                f"Unknown field type '{field_type}'. "
                f"Valid types: {', '.join(sorted(VALID_FIELD_TYPES))}",
            )
            return

        if field_type == "string":
            self._validate_string_field(item_def, path, result)
        elif field_type == "int":
            self._validate_int_field(item_def, path, result)
        elif field_type == "float":
            self._validate_float_field(item_def, path, result)
        elif field_type == "enum":
            self._validate_enum_field(item_def, path, result)
        elif field_type == "object":
            self._validate_object_field(item_def, path, result)
        elif field_type == "array":
            self._validate_array_field(item_def, path, result)
