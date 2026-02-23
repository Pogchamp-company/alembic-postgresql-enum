tests:
    docker compose up --build --exit-code-from run-tests
format:
    uv run black alembic_postgresql_enum/ tests/
