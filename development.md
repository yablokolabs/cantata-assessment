# Development

## Docker Compose

Start the local stack:

```bash
cd backend
docker compose up
```

Services come up on:

- **Backend API:** <http://localhost:8010>
- **Swagger UI:** <http://localhost:8010/docs>
- **Postgres:** `localhost:5442` (db: `cantata`, user: `cantata`, password: `cantata`)
- **Redis:** `localhost:6389`

> Host ports were changed from the original 8000/5432/6379 in commit `93d4b62`,
> which collided with another stack on the development machine. In-container
> ports are unchanged, so `DATABASE_URL` and `REDIS_URL` inside the compose
> network still read 5432/6379. Revert that single commit to restore the
> original host ports; nothing else depends on it.

The `api` and `worker` services share the same image. The `worker` runs the dramatiq broker against Redis.

## Migrations

Migrations run automatically on `api` container startup. To run them manually against a local Python environment:

```bash
cd backend
uv sync
uv run alembic upgrade head
```

To create a new migration:

```bash
uv run alembic revision --autogenerate -m "your description"
```

## Seeding

To populate the database with five in-flight pipelines:

```bash
CANTATA_API_BASE=http://localhost:8010 uv run python scripts/seed.py
```

Or from inside the container, where the default base URL is already correct:

```bash
docker compose exec api python scripts/seed.py
```

Note that this script only creates pipeline rows. The `FAKE_*_FAILURE_MODE`
variables its scenarios reference are read from `os.environ` inside the
**worker**, so the worker must be restarted with the variable set before a
scenario will actually trigger.

## Tooling

- Python 3.12, `uv` for dependency management.
- `ruff` for lint + format.
- `pyright` (basic mode) for type checking — not strict, but no errors should be introduced.

## Logging

`structlog` is wired up at INFO. Step transitions and dramatiq message events log to stdout. There is no Sentry or Logfire backend in this assessment scaffold — what's here is what an on-call engineer would see in a real terminal.
