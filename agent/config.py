from __future__ import annotations

import base64
import json
import os
from pathlib import Path

import yaml
from pydantic import ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

from agent.pulse_types import ProductConfig


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="PULSE_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    db_path: Path = Path("data/pulse.sqlite")
    products_file: Path = Path("products.yaml")
    raw_data_dir: Path = Path("data/raw")
    embedding_cache_dir: Path = Path("data/cache/embeddings")
    artifacts_dir: Path = Path("data/artifacts")
    locks_dir: Path = Path("data/locks")
    api_cors_origins: str = "*"
    log_level: str = "INFO"
    timezone: str = "Asia/Kolkata"
    max_run_cost_usd: float = 5.0
    confirm_send: bool = False
    http_timeout_seconds: float = 15.0
    appstore_max_pages: int = 10
    playstore_page_size: int = 200
    playstore_max_pages: int = 5
    cluster_embedding_provider: str = "local-hash"
    cluster_embedding_model: str = "local-hash-v1"
    cluster_supported_languages: str = "en"
    cluster_min_text_chars: int = 20
    cluster_min_cluster_size: int = 5
    cluster_umap_n_neighbors: int = 15
    cluster_umap_min_dist: float = 0.0
    cluster_umap_components: int = 5
    cluster_random_state: int = 42
    cluster_keyphrases_top_n: int = 5
    cluster_representatives_per_cluster: int = 3
    cluster_noise_warning_ratio: float = 0.35
    cluster_hash_dimensions: int = 384
    summarization_provider: str = "heuristic"
    summarization_model: str = "gpt-4.1-mini"
    summarization_max_clusters: int = 5
    summarization_max_reviews_per_cluster: int = 8
    summarization_retry_attempts: int = 2
    summarization_max_output_tokens: int = 400
    summarization_low_coverage_threshold: int = 5
    render_max_themes: int = 3
    render_max_quotes: int = 3
    render_max_action_ideas: int = 3
    render_email_teaser_themes: int = 3
    mcp_protocol_version: str = "2024-11-05"
    docs_mcp_command: str | None = None
    docs_mcp_args: str = ""
    docs_mcp_cwd: Path | None = None
    docs_mcp_timeout_seconds: float = 20.0
    docs_mcp_message_mode: str = "content-length"
    docs_mcp_tool_get_document: str = "docs.get_document"
    docs_mcp_tool_create_document: str = "docs.create_document"
    docs_mcp_tool_append_section: str = "docs.append_section"
    docs_document_title_prefix: str = "Weekly Review Pulse"
    gmail_mcp_command: str | None = None
    gmail_mcp_args: str = ""
    gmail_mcp_cwd: Path | None = None
    gmail_mcp_timeout_seconds: float = 20.0
    gmail_mcp_message_mode: str = "content-length"
    gmail_mcp_tool_create_draft: str = "gmail.create_draft"
    gmail_mcp_tool_update_draft: str = "gmail.update_draft"
    gmail_mcp_tool_send_draft: str = "gmail.send_draft"
    scheduler_enabled: bool = False
    scheduler_mode: str = "external-cron"
    scheduler_iso_weekday: int = 1
    scheduler_hour: int = 9
    scheduler_minute: int = 0
    run_lock_stale_seconds: int = 21600
    otel_enabled: bool = False
    otel_service_name: str = "weekly-product-review-pulse"
    otel_traces_endpoint: str | None = None
    otel_metrics_endpoint: str | None = None
    otel_export_interval_seconds: float = 30.0
    otel_console_fallback: bool = True
    llm_input_cost_per_million_usd: float = 0.0
    llm_output_cost_per_million_usd: float = 0.0


def resolve_path(path: Path) -> Path:
    return path if path.is_absolute() else (Path.cwd() / path).resolve()


def hydrate_process_env_from_dotenv(env_file: Path | None = None) -> None:
    dotenv_path = env_file or Path(".env")
    if not dotenv_path.is_absolute():
        dotenv_path = (Path.cwd() / dotenv_path).resolve()
    if not dotenv_path.exists():
        return

    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, _, value = line.partition("=")
        normalized_key = key.strip()
        if not normalized_key:
            continue

        normalized_value = value.strip()
        if (
            len(normalized_value) >= 2
            and normalized_value[0] == normalized_value[-1]
            and normalized_value[0] in {'"', "'"}
        ):
            normalized_value = normalized_value[1:-1]
        os.environ.setdefault(normalized_key, normalized_value)


def get_settings() -> Settings:
    hydrate_process_env_from_dotenv()
    seed_google_mcp_token_from_env()
    raw = Settings()
    return raw.model_copy(
        update={
            "db_path": resolve_path(raw.db_path),
            "products_file": resolve_path(raw.products_file),
            "raw_data_dir": resolve_path(raw.raw_data_dir),
            "embedding_cache_dir": resolve_path(raw.embedding_cache_dir),
            "artifacts_dir": resolve_path(raw.artifacts_dir),
            "locks_dir": resolve_path(raw.locks_dir),
            "docs_mcp_cwd": resolve_path(raw.docs_mcp_cwd)
            if raw.docs_mcp_cwd is not None
            else None,
            "gmail_mcp_cwd": resolve_path(raw.gmail_mcp_cwd)
            if raw.gmail_mcp_cwd is not None
            else None,
        }
    )


def seed_google_mcp_token_from_env() -> None:
    raw_json = os.getenv("GOOGLE_MCP_TOKEN_JSON")
    raw_b64 = os.getenv("GOOGLE_MCP_TOKEN_B64")
    if not raw_json and not raw_b64:
        return

    if raw_json:
        token_payload = raw_json
    else:
        assert raw_b64 is not None
        token_payload = base64.b64decode(raw_b64).decode("utf-8")

    parsed = json.loads(token_payload)
    if not isinstance(parsed, dict):
        raise ValueError("GOOGLE_MCP_TOKEN_JSON must decode to a JSON object.")

    token_path = google_mcp_token_path()
    token_path.parent.mkdir(parents=True, exist_ok=True)
    normalized = json.dumps(parsed, indent=2, ensure_ascii=False) + "\n"
    token_path.write_text(normalized, encoding="utf-8")
    os.chmod(token_path, 0o600)


def google_mcp_token_path() -> Path:
    xdg_config_home = os.getenv("XDG_CONFIG_HOME")
    if xdg_config_home:
        config_dir = Path(xdg_config_home)
    else:
        home = os.getenv("HOME") or os.getenv("USERPROFILE")
        if not home:
            raise ValueError(
                "HOME, USERPROFILE, or XDG_CONFIG_HOME must be set for Google MCP token storage."
            )
        config_dir = Path(home) / ".config"

    profile = os.getenv("GOOGLE_MCP_PROFILE")
    token_dir = config_dir / "google-docs-mcp"
    if profile:
        token_dir = token_dir / profile
    return token_dir / "token.json"


def load_products(settings: Settings) -> list[ProductConfig]:
    products_path = settings.products_file
    if not products_path.exists():
        raise FileNotFoundError(f"Products file not found: {products_path}")

    payload = yaml.safe_load(products_path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("products.yaml must contain a list of products")

    products: list[ProductConfig] = []
    for index, item in enumerate(payload):
        try:
            products.append(ProductConfig.model_validate(item))
        except ValidationError as exc:
            raise ValueError(f"Invalid product entry at index {index}") from exc

    return products


def get_product_by_slug(slug: str, settings: Settings) -> ProductConfig:
    products = load_products(settings)
    for product in products:
        if product.slug == slug:
            return product
    raise KeyError(f"Unknown product slug: {slug}")
