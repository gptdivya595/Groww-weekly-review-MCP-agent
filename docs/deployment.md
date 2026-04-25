# Deployment

This project now has two deployable surfaces:

- backend control API and orchestrator on Railway
- frontend operator console on Vercel

The stakeholder-facing artifact is still Google Docs plus Gmail, both delivered
through MCP.

## 1. Target Topology

- Railway service from the repository root for the Python backend
- Railway volume mounted at `/app/data` for SQLite, raw snapshots, and
  artifacts
- Vercel project rooted at `frontend` for the operator dashboard
- Docs MCP and Gmail MCP commands available inside the Railway runtime

Important:

The current MCP client launches server commands over stdio. That means the
configured Docs and Gmail MCP commands must be executable inside the Railway
container. If your chosen MCP servers depend on Node, Bun, or another runtime,
install that runtime in the backend image before going live.

## 2. Railway Backend

### 2.1 Create The Service

- Deploy the repository root to Railway
- Use the root `Dockerfile`
- Set the Railway start command to:

```text
pulse serve --host 0.0.0.0 --port $PORT
```

### 2.2 Attach Persistent Storage

Mount a Railway volume at:

```text
/app/data
```

Without a mounted volume, the backend loses SQLite state and artifacts on
redeploy or restart.

### 2.3 Configure Environment Variables

At minimum, set:

- `PULSE_DB_PATH=/app/data/pulse.sqlite`
- `PULSE_RAW_DATA_DIR=/app/data/raw`
- `PULSE_EMBEDDING_CACHE_DIR=/app/data/cache/embeddings`
- `PULSE_ARTIFACTS_DIR=/app/data/artifacts`
- `PULSE_LOCKS_DIR=/app/data/locks`
- `PULSE_API_CORS_ORIGINS=https://<your-vercel-domain>`
- `PULSE_DOCS_MCP_COMMAND=...`
- `PULSE_DOCS_MCP_ARGS=...`
- `PULSE_GMAIL_MCP_COMMAND=...`
- `PULSE_GMAIL_MCP_ARGS=...`
- `PULSE_SCHEDULER_ENABLED=true` if Railway cron or another scheduler will own recurring runs
- `PULSE_SCHEDULER_MODE=external-cron`
- `PULSE_SCHEDULER_ISO_WEEKDAY=1`
- `PULSE_SCHEDULER_HOUR=9`
- `PULSE_SCHEDULER_MINUTE=0`
- `PULSE_CONFIRM_SEND=false` for the first production validation
- `OPENAI_API_KEY=...` if you use the OpenAI summarization provider

Also make sure the backend has access to:

- the final `products.yaml`
- real stakeholder emails
- real Google Doc IDs, or an approved Doc creation strategy

### 2.4 Health Check

Use:

```text
/health
```

After deploy, verify:

- `GET /health`
- `GET /api/overview`

## 3. Vercel Frontend

### 3.1 Create The Project

- Create a Vercel project from the same repository
- Set the project root directory to `frontend`

### 3.2 Configure Frontend Environment

Set:

```text
NEXT_PUBLIC_API_BASE_URL=https://<your-railway-backend-domain>
```

Deploy once, then copy the Vercel production URL back into Railway as
`PULSE_API_CORS_ORIGINS`.

### 3.3 Verify The Console

After deploy, the dashboard should load and show:

- service and agent health
- scheduler forecast and next run
- warnings and errors tracker
- recent runs and run detail
- one-shot trigger controls
- weekly batch trigger controls

If the page loads but shows readiness warnings, that is expected until live MCP
configuration and delivery evidence exist.

## 4. First Production Validation

Use this rollout order:

1. deploy backend to Railway
2. deploy frontend to Vercel
3. keep Gmail send gated with `PULSE_CONFIRM_SEND=false`
4. run one product flow
5. confirm the Doc section was appended through Docs MCP
6. confirm the Gmail draft was created through Gmail MCP
7. confirm `deliveries` records now exist
8. only then enable `PULSE_CONFIRM_SEND=true`

## 5. Known Deployment Risks

- If Railway does not have the required MCP server runtime installed, Docs and
  Gmail publish will fail even though the Python app starts.
- If `/app/data` is not backed by a volume, SQLite and artifacts will be lost.
- If `products.yaml` still contains placeholders, readiness will stay degraded
  and some products will not publish correctly.
- Until one live Docs delivery and one live Gmail delivery succeed, phases 5 to
  7 remain pending live validation.
