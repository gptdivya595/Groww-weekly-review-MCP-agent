from __future__ import annotations

import os

from agent.config import get_settings


def test_get_settings_hydrates_non_pulse_env_vars_from_dotenv(
    tmp_path,
    monkeypatch,
) -> None:
    dotenv_path = tmp_path / ".env"
    dotenv_path.write_text(
        "\n".join(
            [
                "PULSE_LOG_LEVEL=DEBUG",
                "GOOGLE_CLIENT_ID=test-google-client",
                "OPENAI_API_KEY=test-openai-key",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("PULSE_LOG_LEVEL", raising=False)
    monkeypatch.delenv("GOOGLE_CLIENT_ID", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    settings = get_settings()

    assert settings.log_level == "DEBUG"
    assert os.environ["GOOGLE_CLIENT_ID"] == "test-google-client"
    assert os.environ["OPENAI_API_KEY"] == "test-openai-key"


def test_get_settings_preserves_existing_process_env(tmp_path, monkeypatch) -> None:
    dotenv_path = tmp_path / ".env"
    dotenv_path.write_text(
        "GOOGLE_CLIENT_ID=file-google-client\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GOOGLE_CLIENT_ID", "process-google-client")

    get_settings()

    assert os.environ["GOOGLE_CLIENT_ID"] == "process-google-client"
