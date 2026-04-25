# Quickstart

This guide gets the backend, operator dashboard, and local pipeline running.

## 1. Prerequisites

Install these first:

- Python 3.12
- Node.js 20 or newer
- npm
- `uv` for Python dependency management

You will also need runtime configuration:

- real product metadata in `products.yaml`
- a Docs MCP server command
- a Gmail MCP server command
- Google Workspace auth configured inside those MCP servers
- optionally `OPENAI_API_KEY` if you switch away from heuristic summarization

## 2. Backend Configuration

1. Copy `.env.example` to `.env`.
2. Fill in the MCP values:
   - `PULSE_DOCS_MCP_COMMAND`
   - `PULSE_DOCS_MCP_ARGS`
   - `PULSE_GMAIL_MCP_COMMAND`
   - `PULSE_GMAIL_MCP_ARGS`
3. Keep `PULSE_CONFIRM_SEND=false` for the first live validation runs.
4. Replace placeholders in `products.yaml`:
   - App Store IDs
   - Google Play package names
   - Google Doc IDs, or leave them empty only if your Docs MCP flow will create
     the documents
   - stakeholder email addresses

## 3. Frontend Configuration

1. Copy `frontend/.env.example` to `frontend/.env.local`.
2. Set `NEXT_PUBLIC_API_BASE_URL=http://localhost:8000`.

## 4. Install Dependencies

Backend:

```powershell
uv sync --extra dev
```

Frontend:

```powershell
Set-Location frontend
npm install
Set-Location ..
```

## 5. Initialize Local State

Create the SQLite database and local data directories:

```powershell
uv run pulse init-db
```

## 6. Start The Backend API

Run the control API locally:

```powershell
uv run pulse serve --host 127.0.0.1 --port 8000
```

Verify it:

```powershell
Invoke-WebRequest http://127.0.0.1:8000/health
```

## 7. Start The Operator Dashboard

In a second terminal:

```powershell
Set-Location frontend
npm run dev
```

Then open:

- `http://localhost:3000`

You should see:

- live service and agent health
- scheduler status and next-run forecast
- warnings and errors tracker
- product fleet status
- recent runs and run audit detail
- one-shot controls to run a full flow, Docs-only flow, or Gmail-only flow
- a weekly batch trigger

## 8. Run The Pipeline

Single product through CLI:

```powershell
uv run pulse run --product groww --weeks 10 --target all
```

Weekly batch through CLI:

```powershell
uv run pulse run-weekly --weeks 10 --target all
```

Or trigger a run from the dashboard:

- `Run product flow`
- `Run Weekly Batch`

## 9. Validate Google Delivery Safely

Use this order for the first live test:

1. keep `PULSE_CONFIRM_SEND=false`
2. run a single product
3. confirm the Google Doc section was appended through Docs MCP
4. confirm the Gmail draft was created or updated through Gmail MCP
5. check the `deliveries` table or the dashboard audit payload
6. only then switch to `PULSE_CONFIRM_SEND=true` for real sends

## 10. What Still Needs To Come From You

If you want true end-to-end completion, the repo still needs more than an
OpenAI key. You also need to provide or configure:

- the real Docs MCP server command
- the real Gmail MCP server command
- Google Workspace auth in those MCP servers
- stakeholder email addresses
- Google Doc IDs or a confirmed auto-create strategy
- final values for the remaining placeholder products in `products.yaml`
