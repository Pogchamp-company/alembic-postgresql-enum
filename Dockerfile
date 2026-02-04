FROM python:latest

COPY ./alembic_postgresql_enum ./alembic_postgresql_enum
COPY ./tests ./tests
COPY ./pyproject.toml ./pyproject.toml
COPY ./uv.lock ./uv.lock
COPY ./README.md ./README.md

WORKDIR ./tests

COPY --from=ghcr.io/astral-sh/uv:0.8.0 /uv /uvx /bin/
RUN uv sync --group matrix-2-0

ENTRYPOINT uv run pytest
