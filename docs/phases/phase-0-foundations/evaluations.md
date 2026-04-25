# Phase 0 Evaluations

## Current Workspace Status

Complete and locally validated.

## Objective

Verify that the skeleton is in place so later phases only add business logic.

## Checks

| ID | Check | Method |
| --- | --- | --- |
| P0-E01 | `uv run pulse --help` prints all subcommands | CLI smoke test |
| P0-E02 | `uv run pulse init-db` creates a fresh SQLite file with all tables | Integration test |
| P0-E03 | `agent/config.py` loads `products.yaml` through `pydantic-settings` and `.env` | Unit test |
| P0-E04 | `agent/storage.py` creates `products`, `reviews`, `review_embeddings`, `runs`, and `themes` | DB schema assertion |
| P0-E05 | `.env.example` contains placeholders only | File-content check |
| P0-E06 | `Dockerfile` and `docker-compose.yml` boot the agent container | Container smoke test |
| P0-E07 | CI runs lint and tests on an empty repo baseline | CI assertion |
| P0-E08 | structured logs include `run_id` context | Logging test |

## Acceptance

- the CLI exists and is discoverable
- the DB can be initialized from scratch
- config, storage, container, and CI scaffolding are ready
