# Use a Python image with uv pre-installed
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

# Set environment variables
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy
ENV PYTHONPATH="/app/src"

WORKDIR /app

# Install dependencies
# Copy lockfile and pyproject.toml first to leverage Docker cache
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev

# Copy application code
COPY src/ src/

# Install the project itself
RUN uv sync --frozen --no-dev

# Place executables in the environment at the front of the path
ENV PATH="/app/.venv/bin:$PATH"

# Expose the port for Dagster webserver
EXPOSE 3000

# Default command to run Dagster Dev (Webserver + Daemon)
CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000", "-m", "graph_rag_data_pipeline_1"]
