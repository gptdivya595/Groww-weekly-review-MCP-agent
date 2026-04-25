from __future__ import annotations

import re

from agent.clustering.models import FilteredReviewSet, ReviewDocument

COMMON_ENGLISH_WORDS = {
    "app",
    "and",
    "but",
    "for",
    "good",
    "great",
    "help",
    "issue",
    "love",
    "not",
    "the",
    "this",
    "very",
    "with",
}
WORD_PATTERN = re.compile(r"[a-zA-Z]+")


def parse_supported_languages(raw_value: str) -> set[str]:
    return {value.strip().lower() for value in raw_value.split(",") if value.strip()}


def is_supported_language(review: ReviewDocument, supported_languages: set[str]) -> bool:
    text = review.text
    if not text:
        return False

    locale = (review.locale or "").strip().lower()
    if locale:
        language_token = locale.split("-", maxsplit=1)[0].split("_", maxsplit=1)[0]
        if language_token in supported_languages:
            return True
        if "-" in locale or "_" in locale:
            return False
        if len(language_token) == 2 and language_token not in supported_languages:
            return looks_english(text)

    return looks_english(text)


def looks_english(text: str) -> bool:
    words = WORD_PATTERN.findall(text.lower())
    if not words:
        return False

    english_hits = sum(1 for word in words if word in COMMON_ENGLISH_WORDS)
    ascii_ratio = sum(1 for char in text if ord(char) < 128) / max(len(text), 1)
    return english_hits >= 1 or ascii_ratio >= 0.95


def filter_reviews(
    reviews: list[ReviewDocument],
    *,
    supported_languages: set[str],
    min_text_chars: int,
) -> FilteredReviewSet:
    accepted: list[ReviewDocument] = []
    seen_body_hashes: set[str] = set()
    filtered_language = 0
    filtered_too_short = 0
    filtered_duplicate_body = 0

    for review in reviews:
        if not is_supported_language(review, supported_languages):
            filtered_language += 1
            continue

        if len(review.text) < min_text_chars:
            filtered_too_short += 1
            continue

        if review.body_hash and review.body_hash in seen_body_hashes:
            filtered_duplicate_body += 1
            continue

        if review.body_hash:
            seen_body_hashes.add(review.body_hash)
        accepted.append(review)

    return FilteredReviewSet(
        eligible_reviews=accepted,
        filtered_language=filtered_language,
        filtered_too_short=filtered_too_short,
        filtered_duplicate_body=filtered_duplicate_body,
    )
