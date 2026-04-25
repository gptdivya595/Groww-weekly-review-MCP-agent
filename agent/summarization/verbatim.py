from __future__ import annotations

import re

from agent.clustering.models import ReviewDocument
from agent.summarization.models import ValidatedQuote

_WHITESPACE_RE = re.compile(r"\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+")
_QUOTE_STRIP_RE = re.compile(r'^[\'"\u2018\u2019\u201c\u201d]+|[\'"\u2018\u2019\u201c\u201d]+$')
_PUNCT_SPACING_RE = re.compile(r"\s+([,.!?;:])")
_CANONICAL_TRANSLATION = str.maketrans(
    {
        "\u2018": "'",
        "\u2019": "'",
        "\u201c": '"',
        "\u201d": '"',
        "\u2013": "-",
        "\u2014": "-",
        "\u00a0": " ",
    }
)


def normalize_verbatim_text(text: str) -> str:
    canonical = text.translate(_CANONICAL_TRANSLATION)
    canonical = _WHITESPACE_RE.sub(" ", canonical)
    canonical = _PUNCT_SPACING_RE.sub(r"\1", canonical)
    return canonical.strip().lower()


def clean_quote_candidate(text: str | None) -> str | None:
    if text is None:
        return None
    stripped = _QUOTE_STRIP_RE.sub("", text.strip())
    stripped = _WHITESPACE_RE.sub(" ", stripped)
    return stripped or None


def split_review_sentences(text: str) -> list[str]:
    pieces = [segment.strip() for segment in _SENTENCE_SPLIT_RE.split(text) if segment.strip()]
    if pieces:
        return pieces
    fallback = text.strip()
    return [fallback] if fallback else []


def iter_quote_candidates(text: str) -> list[str]:
    candidates = [
        sentence
        for sentence in split_review_sentences(text)
        if 24 <= len(sentence.strip()) <= 240
    ]
    if candidates:
        return candidates

    stripped = text.strip()
    if not stripped:
        return []
    return [stripped[:240].strip()]


def validate_quote_candidate(
    candidate: str | None,
    reviews: list[ReviewDocument],
    *,
    preferred_review_id: str | None = None,
) -> ValidatedQuote | None:
    cleaned = clean_quote_candidate(candidate)
    if cleaned is None:
        return None

    ordered_reviews = sorted(
        reviews,
        key=lambda review: (review.review_id != preferred_review_id, review.review_id),
    )
    normalized_candidate = normalize_verbatim_text(cleaned)

    for review in ordered_reviews:
        matched = _match_quote_against_review(cleaned, normalized_candidate, review)
        if matched is not None:
            return ValidatedQuote(review_id=review.review_id, text=matched)
    return None


def _match_quote_against_review(
    cleaned_candidate: str,
    normalized_candidate: str,
    review: ReviewDocument,
) -> str | None:
    review_text = review.text
    if not review_text:
        return None

    direct_index = review_text.lower().find(cleaned_candidate.lower())
    if direct_index >= 0:
        return review_text[direct_index : direct_index + len(cleaned_candidate)].strip()

    sentences = split_review_sentences(review_text)
    for sentence in sentences:
        normalized_sentence = normalize_verbatim_text(sentence)
        if normalized_sentence == normalized_candidate:
            return sentence.strip()
        if normalized_candidate in normalized_sentence and len(normalized_candidate) >= 16:
            return sentence.strip()
    return None
