from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from hashlib import sha1
from pathlib import Path
from typing import Any, Protocol, cast

import numpy as np
from sklearn.feature_extraction.text import HashingVectorizer

from agent.clustering.models import CacheRecord, EmbeddingStats
from agent.config import Settings


class EmbeddingProvider(Protocol):
    provider_name: str
    model_name: str

    def embed_texts(self, texts: list[str]) -> np.ndarray:
        ...


class HashingEmbeddingProvider:
    provider_name = "local-hash"

    def __init__(self, *, model_name: str, dimensions: int) -> None:
        self.model_name = model_name
        self.dimensions = dimensions
        self._vectorizer = HashingVectorizer(
            n_features=dimensions,
            alternate_sign=False,
            norm="l2",
            ngram_range=(1, 2),
        )

    def embed_texts(self, texts: list[str]) -> np.ndarray:
        matrix = self._vectorizer.transform(texts)
        dense = cast(np.ndarray[Any, Any], matrix.toarray())
        return dense.astype(np.float32)


class FastEmbedBgeEmbeddingProvider:
    provider_name = "local-bge"

    def __init__(self, *, model_name: str) -> None:
        self.model_name = model_name
        self._embedder: Any | None = None

    def embed_texts(self, texts: list[str]) -> np.ndarray:
        if self._embedder is None:
            try:
                from fastembed import TextEmbedding
            except ImportError as exc:  # pragma: no cover - dependency installed in real env
                raise RuntimeError("fastembed is required for the local-bge provider.") from exc
            self._embedder = TextEmbedding(model_name=self.model_name)

        vectors = [np.asarray(vector, dtype=np.float32) for vector in self._embedder.embed(texts)]
        return np.vstack(vectors)


class OpenAIEmbeddingProvider:
    provider_name = "openai"

    def __init__(self, *, model_name: str) -> None:
        self.model_name = model_name

    def embed_texts(self, texts: list[str]) -> np.ndarray:
        try:
            from openai import OpenAI
        except ImportError as exc:  # pragma: no cover - dependency installed in real env
            raise RuntimeError("openai is required for the openai embedding provider.") from exc

        client = OpenAI()
        response = client.embeddings.create(model=self.model_name, input=texts)
        vectors = [np.asarray(item.embedding, dtype=np.float32) for item in response.data]
        return np.vstack(vectors)


def build_embedding_provider(settings: Settings) -> EmbeddingProvider:
    provider = settings.cluster_embedding_provider.strip().lower()
    model_name = settings.cluster_embedding_model.strip()

    if provider == "local-hash":
        return HashingEmbeddingProvider(
            model_name=model_name,
            dimensions=settings.cluster_hash_dimensions,
        )
    if provider == "local-bge":
        model = model_name or "BAAI/bge-small-en-v1.5"
        return FastEmbedBgeEmbeddingProvider(model_name=model)
    if provider == "openai":
        model = model_name or "text-embedding-3-small"
        return OpenAIEmbeddingProvider(model_name=model)
    raise ValueError(f"Unsupported embedding provider: {settings.cluster_embedding_provider}")


class EmbeddingCache:
    def __init__(
        self,
        *,
        cache_dir: Path,
        provider_name: str,
        model_name: str,
    ) -> None:
        self.cache_dir = cache_dir
        self.provider_name = provider_name
        self.model_name = model_name

    def embed_texts(
        self,
        texts: list[str],
        provider: EmbeddingProvider,
    ) -> tuple[np.ndarray, EmbeddingStats]:
        stats = EmbeddingStats()
        vectors: list[np.ndarray | None] = [None] * len(texts)
        missing_indices: list[int] = []
        missing_texts: list[str] = []

        for index, text in enumerate(texts):
            record = self.read(text)
            if record is None:
                missing_indices.append(index)
                missing_texts.append(text)
                stats.cache_misses += 1
                continue

            stats.cache_hits += 1
            vectors[index] = np.asarray(record.vector, dtype=np.float32)

        if missing_texts:
            fresh_vectors = provider.embed_texts(missing_texts)
            for offset, index in enumerate(missing_indices):
                vector = np.asarray(fresh_vectors[offset], dtype=np.float32)
                vectors[index] = vector
                self.write(texts[index], vector)

        return np.vstack([vector for vector in vectors if vector is not None]), stats

    def read(self, text: str) -> CacheRecord | None:
        cache_path = self._cache_path(text)
        if not cache_path.exists():
            return None

        payload = json.loads(cache_path.read_text(encoding="utf-8"))
        return CacheRecord(
            text_hash=payload["text_hash"],
            provider=payload["provider"],
            model=payload["model"],
            vector=payload["vector"],
            created_at=datetime.fromisoformat(payload["created_at"]),
            cache_path=cache_path,
        )

    def write(self, text: str, vector: np.ndarray) -> None:
        cache_path = self._cache_path(text)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "text_hash": text_cache_key(text),
            "provider": self.provider_name,
            "model": self.model_name,
            "vector": vector.tolist(),
            "created_at": datetime.now(UTC).isoformat(),
        }
        cache_path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")

    def _cache_path(self, text: str) -> Path:
        model_slug = slugify(self.model_name)
        return self.cache_dir / self.provider_name / model_slug / f"{text_cache_key(text)}.json"


def text_cache_key(text: str) -> str:
    return sha1(text.encode()).hexdigest()


def slugify(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]+", "-", value).strip("-").lower()
