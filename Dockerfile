FROM python:latest

COPY ./alembic_postgresql_enum ./alembic_postgresql_enum
COPY ./tests ./tests

WORKDIR ./tests

RUN pip install -r requirements.txt

ENTRYPOINT pytest
