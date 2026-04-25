# Weekly Product Review Pulse - Phase Plan

This implementation plan follows the current architecture and reflects the
actual code status in this workspace.

## Current Workspace Status

Audit date: 2026-04-25

| Phase | Code status | End-to-end status in this workspace |
| --- | --- | --- |
| Phase 0 - Foundations | Complete | Complete |
| Phase 1 - Ingestion | Complete | Complete |
| Phase 2 - Clustering | Complete | Complete |
| Phase 3 - Summarization | Complete | Complete |
| Phase 4 - Render | Complete | Complete |
| Phase 5 - Docs MCP | Complete | Complete |
| Phase 6 - Gmail MCP | Complete | Complete |
| Phase 7 - Orchestration | Complete | Complete |

Evidence for phases 5 to 7 now being complete here:

- the local `deliveries` table now contains one Docs delivery and one Gmail
  delivery
- `/api/completion` returns `overall_status = complete`
- the validated Groww flow has been observed through live MCP-backed delivery

Remaining scope caveat:

- the whole multi-product fleet is not fully configured yet because
  `products.yaml` still contains placeholder metadata for products other than
  Groww

## Cross-Cutting Operator Surface

The project now includes an internal operations layer in addition to the CLI:

- FastAPI control API
- Next.js operator dashboard

This surface exists to:

- expose readiness and completion status
- show recent runs and audit payloads
- trigger a single-product run
- trigger a weekly batch run

It is not the stakeholder-facing output surface. Google Docs remains the system
of record.

## Phase Layout

- `phase-0-foundations`
- `phase-1-ingestion`
- `phase-2-clustering`
- `phase-3-summarization`
- `phase-4-render`
- `phase-5-docs-mcp`
- `phase-6-gmail-mcp`
- `phase-7-orchestration`

## Phase 0 - Foundations And Scaffolding

**Goal:** establish the repo, config, storage, CLI, and deployment skeleton.

### Scope

- `pyproject.toml` using `uv`
- `agent/config.py` for settings and `products.yaml`
- SQLite storage and table creation
- Typer CLI under `agent/__main__.py`
- `Dockerfile`
- `docker-compose.yml`
- structured logging
- CI scaffolding

### Exit Criteria

- `pulse --help` shows the command set
- the database initializes cleanly
- lint and tests run in CI

### Current Status

Complete and locally validated.

## Phase 1 - Review Ingestion

**Goal:** reliably ingest the review window for each configured product.

### Scope

- App Store RSS ingestion
- Google Play ingestion
- unified review model
- PII scrubbing
- dedup and upsert into SQLite
- raw JSONL snapshot persistence
- `pulse ingest`

### Exit Criteria

- deterministic fixture replay
- meaningful review volume on live runs
- reruns avoid duplicate inserts

### Current Status

Complete and locally exercised. The workspace already contains persisted review
data from prior runs.

## Phase 2 - Embeddings And Clustering

**Goal:** convert raw reviews into coherent feedback clusters.

### Scope

- language and minimum-text filtering
- embedding provider abstraction
- embedding cache
- clustering and representative selection
- keyphrase extraction
- persisted cluster artifacts
- `pulse cluster`

### Exit Criteria

- stable clustering on repeat runs with the same input
- persisted embedding and cluster artifacts
- representative reviews are traceable

### Current Status

Complete and locally validated.

## Phase 3 - Theme Summarization

**Goal:** convert clusters into stakeholder-readable themes, quotes, and action
ideas.

### Scope

- summarization pipeline
- grounded quote validation
- structured theme artifacts
- action idea generation
- `pulse summarize`

### Exit Criteria

- malformed model output is rejected
- every kept quote maps back to stored review text
- theme artifacts persist successfully

### Current Status

Complete and locally validated.

## Phase 4 - Render

**Goal:** generate the one-page Docs report payload and Gmail teaser payload.

### Scope

- Docs-friendly render structure
- text and HTML teaser rendering
- stable section anchor generation
- render artifact persistence
- `pulse render`

### Exit Criteria

- render output is deterministic for the same run
- report contains themes, quotes, and action ideas
- teaser is concise and link-oriented

### Current Status

Complete and locally validated.

## Phase 5 - Google Docs MCP

**Goal:** append the weekly report to the product Google Doc through MCP only.

### Scope

- Docs MCP session management
- document lookup or creation
- idempotent section append
- heading ID and deep-link capture
- docs delivery persistence
- `pulse publish --target docs`

### Exit Criteria

- first publish appends the section
- repeat publish for the same run is a no-op
- real Docs MCP staging test passes
- delivery identifiers persist

### Current Status

Complete end to end in this workspace. A live Docs delivery has been recorded
through MCP.

## Phase 6 - Gmail MCP

**Goal:** create or send the stakeholder email through Gmail MCP after Docs
publish succeeds.

### Scope

- Gmail MCP session management
- draft create and update logic
- gated send behavior
- deterministic subject and idempotency key
- Gmail delivery persistence
- `pulse publish --target gmail`

### Exit Criteria

- draft mode is safe by default
- send mode is gated
- reruns do not duplicate stakeholder emails
- real Gmail MCP staging test passes

### Current Status

Complete end to end in this workspace. A live Gmail draft delivery has been
recorded through MCP.

## Phase 7 - Orchestration, Scheduling, And Hardening

**Goal:** run the full pulse safely, repeatedly, and observably.

### Scope

- full pipeline orchestration
- weekly batch execution
- file locks and resume behavior
- retry and recovery logic
- audit payload generation
- OpenTelemetry metrics and alerts
- FastAPI control API
- Next.js operator dashboard
- `pulse run`
- `pulse run-weekly`
- `pulse serve`

### Exit Criteria

- duplicate triggers do not duplicate Docs or Gmail outcomes
- reruns recover from partial publish failures
- operators can inspect status and trigger flows
- live end-to-end Docs plus Gmail run is observed

### Current Status

Complete end to end in this workspace. A live Docs plus Gmail MCP-backed run
has been observed.

## Handoff Rules

- Treat any fresh deployment environment as incomplete until it records its own
  live Docs and Gmail MCP evidence.
- Keep Google delivery behind MCP only.
- Keep the operator dashboard internal; do not treat it as the stakeholder
  report artifact.
- Run draft-only first, then enable Gmail send once Docs append and email
  content are validated.
