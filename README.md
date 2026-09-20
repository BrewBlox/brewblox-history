# History Service

The history service is the gatekeeper for Brewblox databases. It writes data from history events, and offers REST interfaces for querying the Victoria and Redis databases.

## Development

The environment is managed with [uv](https://docs.astral.sh/uv/):

```sh
uv sync                                     # create .venv and install dependencies
uv run pytest                               # tests (need Docker for the eventbus, redis and victoria)
uv run ruff format --check --diff           # formatting, as checked by CI
uv run invoke --list                        # build and cleanup tasks
docker compose up                           # run the service with hot reload next to its dependencies
```

Pull the test images once before the first test run: `docker compose -f test/docker-compose.yml pull`.
