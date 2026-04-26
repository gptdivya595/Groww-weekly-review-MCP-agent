from __future__ import annotations

import csv
import io
import json
import re
from datetime import UTC, datetime
from hashlib import sha1

from pydantic import BaseModel, Field

from agent.ingestion.models import RawReview, ReviewSource

_HEADER_NORMALIZER = re.compile(r"[^a-z0-9]+")

_BODY_COLUMNS = (
    "body",
    "review",
    "review_text",
    "review_body",
    "content",
    "text",
    "comment",
    "message",
    "reviewcontent",
    "translated_content",
    "description",
)
_TITLE_COLUMNS = ("title", "headline", "review_title", "subject", "heading")
_RATING_COLUMNS = ("rating", "score", "stars", "star_rating", "review_rating")
_AUTHOR_COLUMNS = ("author", "author_alias", "user", "user_name", "username", "reviewer")
_CREATED_AT_COLUMNS = (
    "review_created_at",
    "created_at",
    "date",
    "review_date",
    "timestamp",
    "at",
    "time",
    "posted_at",
    "published_at",
    "submitted_at",
)
_UPDATED_AT_COLUMNS = (
    "review_updated_at",
    "updated_at",
    "edited_at",
    "last_modified_at",
    "modified_at",
)
_EXTERNAL_ID_COLUMNS = (
    "external_id",
    "review_id",
    "id",
    "reviewid",
    "store_review_id",
    "comment_id",
)
_SOURCE_COLUMNS = ("source", "platform", "store", "channel")
_LOCALE_COLUMNS = ("locale", "language", "lang", "country_code", "country")
_APP_VERSION_COLUMNS = ("app_version", "version", "appversion", "review_created_version")
_SOURCE_URL_COLUMNS = ("source_url", "url", "link", "review_url")

_DATETIME_FORMATS = (
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%d/%m/%Y",
    "%d-%m-%Y",
    "%m/%d/%Y",
    "%m-%d-%Y",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d %H:%M:%S",
    "%Y/%m/%d %H:%M",
    "%Y/%m/%d %H:%M:%S",
    "%d/%m/%Y %H:%M",
    "%d/%m/%Y %H:%M:%S",
    "%d-%m-%Y %H:%M",
    "%d-%m-%Y %H:%M:%S",
    "%m/%d/%Y %H:%M",
    "%m/%d/%Y %H:%M:%S",
    "%m-%d-%Y %H:%M",
    "%m-%d-%Y %H:%M:%S",
)


class ParsedCsvUpload(BaseModel):
    reviews: list[RawReview] = Field(default_factory=list)
    columns: list[str] = Field(default_factory=list)
    total_rows: int = 0
    accepted_rows: int = 0
    skipped_rows: int = 0
    derived_timestamp_rows: int = 0
    sample_errors: list[str] = Field(default_factory=list)


def parse_uploaded_reviews(
    csv_text: str,
    *,
    filename: str | None,
    fallback_review_time: datetime,
) -> ParsedCsvUpload:
    text = csv_text.lstrip("\ufeff").strip()
    if not text:
        raise ValueError("The uploaded CSV is empty.")

    sample = text[:4096]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
    except csv.Error:
        dialect = csv.excel

    reader = csv.DictReader(io.StringIO(text), dialect=dialect)
    if reader.fieldnames is None:
        raise ValueError("The uploaded CSV must include a header row.")

    columns = [field.strip() for field in reader.fieldnames if field and field.strip()]
    if not columns:
        raise ValueError("The uploaded CSV must include at least one named column.")

    result = ParsedCsvUpload(columns=columns)

    for row_number, row in enumerate(reader, start=2):
        result.total_rows += 1
        raw_row = _normalize_raw_row(row)
        if not any(raw_row.values()):
            result.skipped_rows += 1
            continue

        normalized_row = {_normalize_header(key): value for key, value in raw_row.items()}

        title = _pick_first(normalized_row, _TITLE_COLUMNS)
        body = _pick_first(normalized_row, _BODY_COLUMNS) or title
        if body is None:
            result.skipped_rows += 1
            _append_sample_error(
                result,
                f"Row {row_number} was skipped because no review text column was found.",
            )
            continue

        created_at = _parse_datetime(_pick_first(normalized_row, _CREATED_AT_COLUMNS))
        updated_at = _parse_datetime(_pick_first(normalized_row, _UPDATED_AT_COLUMNS))
        if created_at is None and updated_at is None:
            created_at = fallback_review_time
            updated_at = fallback_review_time
            result.derived_timestamp_rows += 1
        elif created_at is None:
            created_at = updated_at
        elif updated_at is None:
            updated_at = created_at

        external_id = _pick_first(normalized_row, _EXTERNAL_ID_COLUMNS)
        if external_id is None:
            external_id = _generated_external_id(normalized_row)

        result.reviews.append(
            RawReview(
                source=_coerce_source(_pick_first(normalized_row, _SOURCE_COLUMNS)),
                external_id=external_id,
                rating=_parse_rating(_pick_first(normalized_row, _RATING_COLUMNS)),
                title=title,
                body=body,
                author_alias=_pick_first(normalized_row, _AUTHOR_COLUMNS),
                review_created_at=created_at,
                review_updated_at=updated_at,
                locale=_pick_first(normalized_row, _LOCALE_COLUMNS),
                app_version=_pick_first(normalized_row, _APP_VERSION_COLUMNS),
                source_url=_pick_first(normalized_row, _SOURCE_URL_COLUMNS),
                raw_payload={
                    "filename": filename,
                    "csv_row_number": row_number,
                    "row": raw_row,
                },
            )
        )
        result.accepted_rows += 1

    if result.accepted_rows == 0:
        raise ValueError(
            "No usable review rows were found. Include a text column such as "
            "`body`, `content`, `review`, or `title`."
        )

    return result


def _normalize_raw_row(row: dict[str | None, str | None]) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for key, value in row.items():
        if key is None:
            continue
        stripped_key = key.strip()
        if not stripped_key:
            continue
        normalized[stripped_key] = str(value).strip() if value is not None else ""
    return normalized


def _normalize_header(value: str) -> str:
    normalized = _HEADER_NORMALIZER.sub("_", value.strip().lower()).strip("_")
    return normalized


def _pick_first(row: dict[str, str], candidates: tuple[str, ...]) -> str | None:
    for candidate in candidates:
        value = row.get(candidate)
        if value is None:
            continue
        stripped = value.strip()
        if stripped:
            return stripped
    return None


def _parse_rating(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        parsed = int(float(value))
    except ValueError:
        return None
    if 1 <= parsed <= 5:
        return parsed
    return None


def _parse_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None

    normalized = value.strip()
    if not normalized:
        return None

    if normalized.isdigit():
        if len(normalized) >= 13:
            return datetime.fromtimestamp(int(normalized) / 1000, tz=UTC)
        return datetime.fromtimestamp(int(normalized), tz=UTC)

    iso_candidate = normalized.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(iso_candidate)
    except ValueError:
        parsed = None

    if parsed is not None:
        return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)

    for pattern in _DATETIME_FORMATS:
        try:
            parsed = datetime.strptime(normalized, pattern)
        except ValueError:
            continue
        return parsed.replace(tzinfo=UTC)

    return None


def _coerce_source(value: str | None) -> ReviewSource:
    if value is None:
        return ReviewSource.CSV_UPLOAD

    normalized = value.strip().lower()
    if not normalized:
        return ReviewSource.CSV_UPLOAD

    collapsed = re.sub(r"[^a-z0-9]+", "", normalized)
    if any(token in collapsed for token in ("appstore", "apple", "itunes")):
        return ReviewSource.APPSTORE
    if any(token in collapsed for token in ("playstore", "googleplay", "google")):
        return ReviewSource.PLAYSTORE
    return ReviewSource.CSV_UPLOAD


def _generated_external_id(row: dict[str, str]) -> str:
    payload = json.dumps(row, sort_keys=True, ensure_ascii=False)
    digest = sha1(payload.encode("utf-8")).hexdigest()
    return f"csv-{digest[:20]}"


def _append_sample_error(result: ParsedCsvUpload, message: str) -> None:
    if len(result.sample_errors) >= 5:
        return
    result.sample_errors.append(message)
