set positional-arguments

# List available development commands.
default:
    @just --list

# Install all locked development dependencies.
install:
    uv sync --locked --dev

# Check formatting, lint, and types.
check:
    uv run ruff format --check .
    uv run ruff check .
    uv run ty check

# Build the documentation and treat warnings as errors.
docs:
    uv run --group docs sphinx-build -W --keep-going -n -b html docs docs/_build/html

# Execute every Python example embedded in the documentation.
docs-examples:
    uv run --group docs-examples pytest -q docs/check_examples.py

# Check every marimo notebook without executing its workload.
notebooks-check:
    uv run --group notebooks marimo check examples/marimo/[0-9]*.py

# Run one notebook as an isolated, reproducible Python script.
notebook path:
    uv run --script "{{path}}"

# Execute every notebook against the configured object store.
notebooks-test:
    #!/usr/bin/env bash
    set -euo pipefail
    for notebook in examples/marimo/[0-9]*.py; do
        uv run --script "$notebook"
    done

# Rebuild the documentation while files change and serve it locally.
docs-serve:
    uv run --group docs sphinx-autobuild -W -n docs docs/_build/html

# Run the complete test suite against available services.
test *args:
    uv run pytest -v "$@"

# Run tests that do not require S3.
unit *args:
    uv run pytest -v -k "not s3" "$@"

# Run the complete test suite with a managed MinIO service.
integration *args:
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'docker compose down --volumes' EXIT
    docker compose up -d --wait
    uv run pytest -v "$@"

# Start the MinIO service used by benchmarks.
bench-up:
    docker compose up -d --wait

# Run the storage benchmark against MinIO.
bench *args: bench-up
    uv run python bench/bench_read_write.py "$@"

# Stop the MinIO service used by benchmarks.
bench-down:
    docker compose down --volumes
