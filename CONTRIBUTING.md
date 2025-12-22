# Contributing to khaos

Thanks for your interest in contributing!

## Development Setup

```bash
git clone https://github.com/aleksandarskrbic/khaos.git
cd khaos
uv sync
uv run pre-commit install
```

## Before Submitting a PR

Run all checks:

```bash
./scripts/check.sh
```

This runs:
- `ruff check` - linting with auto-fix
- `ruff format` - code formatting
- `pytest` - tests
- `mypy` - type checking

## Code Style

- We use [ruff](https://github.com/astral-sh/ruff) for linting and formatting
- Type hints are required for public APIs
- Tests are required for new features

## Pull Request Process

1. Fork the repo and create your branch from `main`
2. Run `./scripts/check.sh` and ensure all checks pass
3. Update documentation if needed
4. Submit PR with a clear description of changes
