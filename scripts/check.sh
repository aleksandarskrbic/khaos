#!/usr/bin/env bash
set -e

# Parse arguments
RUN_INTEGRATION=false
while getopts "i" opt; do
    case $opt in
        i) RUN_INTEGRATION=true ;;
        *) echo "Usage: $0 [-i]" && exit 1 ;;
    esac
done

# Auto-fix linting and formatting issues
uv run ruff check src tests --fix
uv run ruff format src tests

# Run tests
if [ "$RUN_INTEGRATION" = true ]; then
    echo "Running all tests including integration tests..."
    uv run pytest tests/
else
    echo "Running unit tests (use -i to include integration tests)..."
    uv run pytest tests/ -m "not integration"
fi

# Type checking
uv run ty check src/khaos

echo "All checks passed!"
