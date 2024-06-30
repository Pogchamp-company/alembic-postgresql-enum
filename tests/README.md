# How to run tests

# With `docker compose`

Just run:
```commandline
docker compose up --build --exit-code-from run-tests
```

# Manually

## Create database

Start postgres through docker compose:

## Env variables

### Using .env
```
pip install python-dotenv
touch .env
```
Set env variable DATABASE_URI in .env file

### Using command line
```
export DATABASE_URI=postgresql://username:password@localhost:5432/dbname
```

Install test requirements
```
pip install -R tests/requirements.txt
```

Run tests
```
pytest
```
