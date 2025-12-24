from dataclasses import dataclass, field


@dataclass
class ValidationError:
    path: str
    message: str


@dataclass
class ValidationResult:
    valid: bool = True
    errors: list[ValidationError] = field(default_factory=list)
    warnings: list[ValidationError] = field(default_factory=list)

    def add_error(self, path: str, message: str) -> None:
        self.errors.append(ValidationError(path, message))
        self.valid = False

    def add_warning(self, path: str, message: str) -> None:
        self.warnings.append(ValidationError(path, message))
