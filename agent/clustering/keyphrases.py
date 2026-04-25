from __future__ import annotations

from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS, TfidfVectorizer


def extract_keyphrases(texts: list[str], *, top_n: int) -> list[str]:
    cleaned = [text.strip() for text in texts if text.strip()]
    if not cleaned:
        return []

    if len(cleaned) == 1:
        return _fallback_phrases(cleaned[0], top_n=top_n)

    vectorizer = TfidfVectorizer(
        lowercase=True,
        ngram_range=(1, 2),
        stop_words="english",
        max_features=200,
    )
    matrix = vectorizer.fit_transform(cleaned)
    scores = matrix.mean(axis=0).A1
    features = vectorizer.get_feature_names_out()
    ranked = sorted(
        ((feature, float(score)) for feature, score in zip(features, scores, strict=True)),
        key=lambda item: item[1],
        reverse=True,
    )

    keyphrases: list[str] = []
    for feature, _ in ranked:
        if feature not in keyphrases:
            keyphrases.append(feature)
        if len(keyphrases) >= top_n:
            break

    if keyphrases:
        return keyphrases
    return _fallback_phrases(" ".join(cleaned), top_n=top_n)


def _fallback_phrases(text: str, *, top_n: int) -> list[str]:
    tokens = [
        token
        for token in text.lower().split()
        if token.isascii() and token.isalpha() and token not in ENGLISH_STOP_WORDS
    ]
    deduped: list[str] = []
    for token in tokens:
        if token not in deduped:
            deduped.append(token)
        if len(deduped) >= top_n:
            break
    return deduped
