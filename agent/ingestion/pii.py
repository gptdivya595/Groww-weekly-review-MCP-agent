from __future__ import annotations

import re

EMAIL_PATTERN = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
AADHAAR_PATTERN = re.compile(r"\b\d{4}\s?\d{4}\s?\d{4}\b")
PHONE_PATTERN = re.compile(r"(?<!\w)(?:\+?\d[\d\s().-]{8,}\d)(?!\w)")


def _redact_phone(match: re.Match[str]) -> str:
    token = match.group(0)
    digits = re.sub(r"\D", "", token)
    if 10 <= len(digits) <= 14:
        return "[REDACTED_PHONE]"
    return token


def scrub_review_text(text: str | None) -> str | None:
    if text is None:
        return None

    scrubbed = EMAIL_PATTERN.sub("[REDACTED_EMAIL]", text)
    scrubbed = AADHAAR_PATTERN.sub("[REDACTED_AADHAAR]", scrubbed)
    scrubbed = PHONE_PATTERN.sub(_redact_phone, scrubbed)
    return scrubbed
