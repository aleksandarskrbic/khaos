FROM python:3.11-slim

WORKDIR /app

# Install system dependencies for confluent-kafka
RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# Install uv for faster package installation
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy project files
COPY pyproject.toml uv.lock ./
COPY src/ src/
COPY scenarios/ scenarios/

# Install dependencies and the package
RUN uv sync --frozen --no-dev

# Set entrypoint to khaos CLI
ENTRYPOINT ["uv", "run", "khaos"]

# Default command (show help)
CMD ["--help"]
