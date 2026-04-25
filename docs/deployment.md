# Deployment

This project deploys as:

- backend API and scheduler host on Render
- frontend operator dashboard on Vercel

Google Docs and Gmail delivery still happens only through MCP commands executed
inside the backend runtime.

## 1. Files That Matter

- `render.yaml`: Render Blueprint for the backend service
- `Dockerfile.render`: backend image used by Render
- `frontend/vercel.json`: Vercel project configuration for the frontend

Important:

`frontend/vercel.json` is intentionally inside `frontend/`. In Vercel, set the
project Root Directory to `frontend` so that Vercel treats that folder as the
project root and uses the config in that directory.

## 2. Render Backend

### 2.1 Create The Service

1. Push the repository to GitHub.
2. In Render, create a new Blueprint from the repository root.
3. Let Render read `render.yaml`.

The backend service is already configured to:

- build from the repository root
- use `Dockerfile.render`
- mount a persistent disk at `/app/data`
- expose `/health` as the health check

### 2.2 Render Environment Variables

Render will prefill most non-secret values from `render.yaml`. You still need
to provide the secret or environment-specific values marked `sync: false`:

- `PULSE_API_CORS_ORIGINS=https://<your-vercel-domain>`
- `GOOGLE_CLIENT_ID=...`
- `GOOGLE_CLIENT_SECRET=...`
- `OPENAI_API_KEY=...`

These are also important for production readiness:

- `GOOGLE_MCP_PROFILE=pulse`
- `PULSE_CONFIRM_SEND=false` for the first live validation
- `PULSE_SCHEDULER_ENABLED=false` if you only want one-shot runs at first

### 2.3 Persistent Storage

The Render disk mounted at `/app/data` stores:

- SQLite state
- cached embeddings
- raw review snapshots
- generated artifacts
- Google MCP auth state stored under the persisted home/config directories

Without that disk, auth state and pipeline evidence will be lost on redeploy.

### 2.4 Complete Google MCP Auth

After the first successful Render deploy:

1. Open a Render shell for the backend service.
2. Run the Google MCP auth flow in that runtime.
3. Confirm the tokens are written under the persisted home/config paths used by
   the service.

Because the backend launches Docs and Gmail MCP tools over stdio, the backend
container itself must be able to execute the MCP commands and access the saved
auth state.

### 2.5 Verify The Backend

After deploy, verify:

- `GET https://<render-url>/health`
- `GET https://<render-url>/api/overview`

## 3. Vercel Frontend

### 3.1 Create The Project

1. Import the same GitHub repository into Vercel.
2. Set Root Directory to `frontend`.
3. Keep the frontend config file at `frontend/vercel.json`.

### 3.2 Frontend Environment Variable

Set:

```text
NEXT_PUBLIC_API_BASE_URL=https://<your-render-backend-domain>
```

Deploy the frontend once. Then copy the Vercel production URL back into Render
as `PULSE_API_CORS_ORIGINS`.

### 3.3 Verify The Dashboard

After deploy, the dashboard should load and show:

- service health
- agent and delivery health
- scheduler status and next run
- warnings and errors tracker
- recent runs
- one-shot trigger controls
- the `Run Weekly Pulse` action

## 4. Recommended First Deploy Order

1. Deploy backend on Render from `render.yaml`.
2. Verify `/health` and `/api/overview`.
3. Deploy frontend on Vercel with Root Directory `frontend`.
4. Copy the Vercel URL into Render as `PULSE_API_CORS_ORIGINS`.
5. Complete Google Docs MCP and Gmail MCP auth inside the Render runtime.
6. Keep `PULSE_CONFIRM_SEND=false`.
7. Trigger one full Groww flow.
8. Confirm the Google Doc section was appended through Docs MCP.
9. Confirm the Gmail draft or send happened through Gmail MCP.
10. Only then enable real sends.

## 5. Common Failure Points

- Render deployed without the persistent disk, so state and auth do not persist.
- Vercel was pointed at the repo root instead of `frontend`.
- `NEXT_PUBLIC_API_BASE_URL` still points to localhost.
- `PULSE_API_CORS_ORIGINS` does not include the final Vercel domain.
- Google MCP auth was completed locally instead of inside the persisted Render
  runtime.
- `products.yaml` still contains placeholder data for docs or recipients.
