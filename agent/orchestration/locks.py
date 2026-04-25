from __future__ import annotations

import json
import os
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path


class RunLockError(RuntimeError):
    """Raised when a per-product ISO-week run lock cannot be acquired."""


@dataclass(slots=True)
class ProductWeekRunLock:
    lock_path: Path
    product_slug: str
    iso_week: str
    stale_after_seconds: int

    def __enter__(self) -> ProductWeekRunLock:
        self.acquire()
        return self

    def __exit__(self, *_args: object) -> None:
        self.release()

    def acquire(self) -> None:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        self._clear_stale_lock_if_needed()
        try:
            descriptor = os.open(
                self.lock_path,
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            )
        except FileExistsError as exc:
            raise RunLockError(
                f"Another run is already active for {self.product_slug} {self.iso_week}."
            ) from exc

        payload = {
            "product_slug": self.product_slug,
            "iso_week": self.iso_week,
            "pid": os.getpid(),
            "acquired_at": datetime.now(UTC).isoformat(),
        }
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2, sort_keys=True)
        except Exception:
            with suppress(OSError):
                os.close(descriptor)
            self.release()
            raise

    def release(self) -> None:
        try:
            self.lock_path.unlink()
        except FileNotFoundError:
            return

    def _clear_stale_lock_if_needed(self) -> None:
        if not self.lock_path.exists():
            return

        acquired_at: datetime | None = None
        try:
            payload = json.loads(self.lock_path.read_text(encoding="utf-8"))
            raw_timestamp = payload.get("acquired_at")
            if isinstance(raw_timestamp, str):
                acquired_at = datetime.fromisoformat(raw_timestamp)
        except Exception:
            acquired_at = None

        if acquired_at is None:
            raise RunLockError(
                f"Run lock exists for {self.product_slug} {self.iso_week} and could not be parsed."
            )

        age = datetime.now(UTC) - acquired_at
        if age <= timedelta(seconds=self.stale_after_seconds):
            raise RunLockError(
                f"Another run is already active for {self.product_slug} {self.iso_week}."
            )

        self.lock_path.unlink(missing_ok=True)
