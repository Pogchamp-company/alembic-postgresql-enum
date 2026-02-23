FROM python:latest

COPY ./pyproject.toml ./pyproject.toml
COPY ./uv.lock ./uv.lock
COPY ./README.md ./README.md
COPY --from=ghcr.io/astral-sh/uv:0.8.0 /uv /uvx /bin/
RUN uv sync --group matrix-2-0

COPY ./alembic_postgresql_enum ./alembic_postgresql_enum
COPY ./tests ./tests

WORKDIR ./tests

ENTRYPOINT uv run --no-python-downloads --no-sync pytest
