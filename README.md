# Weekly Product Review Pulse

AI agent for a weekly product review pulse. The system ingests App Store and
Google Play reviews for configured fintech products, clusters and summarizes
the feedback, renders a one-page report, and delivers to Google Docs and Gmail
through MCP only.

The repo now includes:

- Python backend and orchestration pipeline
- FastAPI control API
- Next.js operator dashboard
- phase-by-phase docs
- Render and Vercel deployment runbooks

## Start Here

- Architecture: `docs/architecture.md`
- Problem statement: `docs/problemStatement.md`
- Phase plan: `docs/implementation-plan.md`
- Completion audit: `docs/completion-audit.md`
- Local setup: `docs/quickstart.md`
- Deployment: `docs/deployment.md`

## Core Commands

- `uv run pulse init-db`
- `uv run pulse ingest`
- `uv run pulse cluster`
- `uv run pulse summarize`
- `uv run pulse render`
- `uv run pulse publish`
- `uv run pulse run`
- `uv run pulse run-weekly`
- `uv run pulse audit-run`
- `uv run pulse list-products`
- `uv run pulse serve`

## Operator Console

The operator dashboard lives under `frontend/` and talks to the backend API.
It now includes live service health, scheduler visibility, issue tracking, and
one-shot trigger controls in addition to run history and audit views.

Deployment shape:

- Render backend from the repository root using `render.yaml`
- Vercel frontend with Root Directory set to `frontend`

Local frontend development:

```powershell
Set-Location frontend
npm install
npm run dev
```

Local backend API:

```powershell
uv run pulse serve --host 127.0.0.1 --port 8000
```

One-command local startup from Git Bash or another Bash shell:

```bash
bash start.sh
```

## Current Status

As of the current workspace audit:

- phases 0 to 4 are implemented and locally validated
- phases 5 to 7 are implemented but still pending live Docs and Gmail MCP
  validation in this workspace

See `docs/completion-audit.md` for the exact evidence and remaining gaps.
