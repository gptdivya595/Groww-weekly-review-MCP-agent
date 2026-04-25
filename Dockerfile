FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    HOME=/app/data/home \
    XDG_CONFIG_HOME=/app/data/home/.config \
    NPM_CONFIG_CACHE=/app/data/home/.npm \
    PULSE_DB_PATH=/app/data/pulse.sqlite \
    PULSE_PRODUCTS_FILE=/app/products.yaml \
    PULSE_RAW_DATA_DIR=/app/data/raw \
    PULSE_EMBEDDING_CACHE_DIR=/app/data/cache/embeddings \
    PULSE_ARTIFACTS_DIR=/app/data/artifacts \
    PULSE_LOCKS_DIR=/app/data/locks \
    PULSE_LOG_LEVEL=INFO \
    PULSE_TIMEZONE=Asia/Kolkata \
    PULSE_CONFIRM_SEND=false \
    PULSE_SCHEDULER_ENABLED=false \
    PULSE_SCHEDULER_MODE=external-cron \
    PULSE_SCHEDULER_ISO_WEEKDAY=1 \
    PULSE_SCHEDULER_HOUR=9 \
    PULSE_SCHEDULER_MINUTE=0 \
    PULSE_DOCS_MCP_COMMAND=google-docs-mcp \
    PULSE_DOCS_MCP_ARGS= \
    PULSE_DOCS_MCP_TIMEOUT_SECONDS=40 \
    PULSE_DOCS_MCP_MESSAGE_MODE=jsonl \
    PULSE_DOCS_MCP_TOOL_GET_DOCUMENT=readDocument \
    PULSE_DOCS_MCP_TOOL_CREATE_DOCUMENT=createDocument \
    PULSE_DOCS_MCP_TOOL_APPEND_SECTION=appendMarkdown \
    PULSE_GMAIL_MCP_COMMAND=google-docs-mcp \
    PULSE_GMAIL_MCP_ARGS= \
    PULSE_GMAIL_MCP_TIMEOUT_SECONDS=40 \
    PULSE_GMAIL_MCP_MESSAGE_MODE=jsonl \
    PULSE_GMAIL_MCP_TOOL_CREATE_DRAFT=createDraft \
    PULSE_GMAIL_MCP_TOOL_UPDATE_DRAFT=updateDraft \
    PULSE_GMAIL_MCP_TOOL_SEND_DRAFT=sendDraft \
    GOOGLE_MCP_PROFILE=pulse

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates gnupg \
    && mkdir -p /etc/apt/keyrings \
    && curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key \
    | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg \
    && echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_20.x nodistro main" \
    > /etc/apt/sources.list.d/nodesource.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends nodejs \
    && npm install -g @a-bonus/google-docs-mcp@1.8.0 \
    && python -m pip install --no-cache-dir uv \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY . /app

RUN uv pip install --system -e .

CMD ["sh", "-c", "mkdir -p /app/data/raw /app/data/cache/embeddings /app/data/artifacts /app/data/locks /app/data/home /app/data/home/.config /app/data/home/.npm && pulse init-db && pulse serve --host 0.0.0.0 --port ${PORT:-8000}"]
