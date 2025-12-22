#!/usr/bin/env bash
set -e

# Auto-fix linting and formatting issues
uv run ruff check src tests --fix
uv run ruff format src tests

# Run tests and type checking
uv run pytest tests/
uv run mypy src/khaos --ignore-missing-imports

echo "All checks passed!"
