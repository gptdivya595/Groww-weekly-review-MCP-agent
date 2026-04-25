# Deployment

This project now deploys as:

- backend API and scheduler host on Railway
- frontend operator dashboard on Vercel

Google Docs and Gmail delivery still happens only through MCP commands executed
inside the backend runtime.

## 1. Files That Matter

- `railway.json`: Railway config-as-code for the backend service
- `Dockerfile`: backend image used by Railway
- `frontend/vercel.json`: Vercel project configuration for the frontend

Important:

`frontend/vercel.json` is intentionally inside `frontend/`. In Vercel, set the
project Root Directory to `frontend` so that Vercel treats that folder as the
project root and uses the config in that directory.

## 2. Railway Backend

### 2.1 Create The Service

1. Push the repository to GitHub.
2. In Railway, create a new project from that repository.
3. Deploy the repository root as the backend service.

The backend service is already configured to:

- build from the repository root
- use the root `Dockerfile`
- expose `/health` as the health check through `railway.json`
- start the API with the required data directories and database initialization

### 2.2 Attach Persistent Storage

Attach one Railway volume to the backend service and mount it at:

```text
/app/data
```

This is required because the backend stores SQLite state, raw review snapshots,
artifacts, lock files, and Google MCP auth state under `/app/data`.

### 2.3 Railway Environment Variables

The Docker image already sets stable defaults for:

- `PULSE_DB_PATH`
- `PULSE_PRODUCTS_FILE`
- `PULSE_RAW_DATA_DIR`
- `PULSE_EMBEDDING_CACHE_DIR`
- `PULSE_ARTIFACTS_DIR`
- `PULSE_LOCKS_DIR`
- `PULSE_DOCS_MCP_COMMAND`
- `PULSE_DOCS_MCP_ARGS`
- `PULSE_GMAIL_MCP_COMMAND`
- `PULSE_GMAIL_MCP_ARGS`
- `GOOGLE_MCP_PROFILE`
- safe initial scheduler and send settings

You still need to set these in Railway:

- `PULSE_API_CORS_ORIGINS=https://<your-vercel-domain>`
- `GOOGLE_CLIENT_ID=...`
- `GOOGLE_CLIENT_SECRET=...`
- `OPENAI_API_KEY=...`

Recommended first-run settings:

- `PULSE_CONFIRM_SEND=false`
- `PULSE_SCHEDULER_ENABLED=false`

### 2.4 Complete Google MCP Auth

After the first successful Railway deploy:

1. Open a shell inside the Railway backend service.
2. Run the Google MCP auth flow in that runtime.
3. Confirm the auth files are stored under the persisted `/app/data/home`
   paths.

Because the backend launches Docs and Gmail MCP tools over stdio, the backend
container itself must be able to execute the MCP commands and reuse the saved
auth state.

### 2.5 Verify The Backend

After deploy, verify:

- `GET https://<railway-url>/health`
- `GET https://<railway-url>/api/overview`

## 3. Vercel Frontend

### 3.1 Create The Project

1. Import the same GitHub repository into Vercel.
2. Set Root Directory to `frontend`.
3. Keep the frontend config file at `frontend/vercel.json`.

### 3.2 Frontend Environment Variable

Set:

```text
NEXT_PUBLIC_API_BASE_URL=https://<your-railway-backend-domain>
```

Deploy the frontend once. Then copy the Vercel production URL back into Railway
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

1. Deploy backend on Railway from the repository root.
2. Attach the Railway volume at `/app/data`.
3. Verify `/health` and `/api/overview`.
4. Deploy frontend on Vercel with Root Directory `frontend`.
5. Copy the Vercel URL into Railway as `PULSE_API_CORS_ORIGINS`.
6. Complete Google Docs MCP and Gmail MCP auth inside the Railway runtime.
7. Keep `PULSE_CONFIRM_SEND=false`.
8. Trigger one full Groww flow.
9. Confirm the Google Doc section was appended through Docs MCP.
10. Confirm the Gmail draft or send happened through Gmail MCP.
11. Only then enable real sends.

## 5. Common Failure Points

- Railway deployed without the volume, so state and auth do not persist.
- Vercel was pointed at the repo root instead of `frontend`.
- `NEXT_PUBLIC_API_BASE_URL` still points to localhost.
- `PULSE_API_CORS_ORIGINS` does not include the final Vercel domain.
- Google MCP auth was completed locally instead of inside the persisted Railway
  runtime.
- `products.yaml` still contains placeholder data for docs or recipients.

## 6. Vercel Error: No FastAPI Entrypoint Found

If Vercel shows an error like:

```text
No fastapi entrypoint found. Add an 'app' script in pyproject.toml ...
```

that means Vercel is trying to deploy the repository root as a Python project
because it found the root `pyproject.toml`.

This repository should not be deployed to Vercel from the root.

Fix it this way:

1. Open the Vercel project.
2. Go to `Settings` -> `Build and Deployment`.
3. Edit `Root Directory`.
4. Set it to `frontend`.
5. Redeploy.

If you already created the wrong Vercel project, the simplest option is often
to delete that project and import the same GitHub repository again with Root
Directory set to `frontend` during setup.
