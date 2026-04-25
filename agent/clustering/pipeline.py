from __future__ import annotations

import statistics
import warnings
from collections import defaultdict
from typing import Any, cast

import hdbscan
import numpy as np
import umap
from sklearn.metrics import pairwise_distances

from agent.clustering.filters import filter_reviews, parse_supported_languages
from agent.clustering.keyphrases import extract_keyphrases
from agent.clustering.models import (
    ClusterArtifact,
    ClusteringResult,
    EmbeddingStats,
    PersistedCluster,
    PersistedEmbedding,
    ReviewDocument,
)
from agent.clustering.providers import EmbeddingCache, EmbeddingProvider, build_embedding_provider
from agent.config import Settings
from agent.logging import get_logger
from agent.pulse_types import StoredRunRecord
from agent.storage import Storage
from agent.telemetry import record_clusters_formed, start_span


def run_clustering_for_run(
    *,
    settings: Settings,
    storage: Storage,
    run_record: StoredRunRecord,
) -> ClusteringResult:
    provider = build_embedding_provider(settings)
    service = ClusteringService(settings=settings, storage=storage, provider=provider)
    return service.run(run_record=run_record)


class ClusteringService:
    def __init__(
        self,
        *,
        settings: Settings,
        storage: Storage,
        provider: EmbeddingProvider,
    ) -> None:
        self.settings = settings
        self.storage = storage
        self.provider = provider
        self.logger = get_logger("pulse.clustering")

    def run(self, *, run_record: StoredRunRecord) -> ClusteringResult:
        with start_span(
            "clustering.run",
            {
                "product_slug": run_record.product_slug,
                "iso_week": run_record.iso_week,
                "embedding_provider": self.provider.provider_name,
                "embedding_model": self.provider.model_name,
            },
        ):
            raw_reviews = self.storage.fetch_reviews_for_run(run_record)
            supported_languages = parse_supported_languages(
                self.settings.cluster_supported_languages
            )
            filtered = filter_reviews(
                raw_reviews,
                supported_languages=supported_languages,
                min_text_chars=self.settings.cluster_min_text_chars,
            )
            texts = [review.text for review in filtered.eligible_reviews]
            cache = EmbeddingCache(
                cache_dir=self.settings.embedding_cache_dir,
                provider_name=self.provider.provider_name,
                model_name=self.provider.model_name,
            )

            if len(filtered.eligible_reviews) < max(self.settings.cluster_min_cluster_size, 3):
                embeddings = np.empty((0, 0), dtype=np.float32)
                embedding_stats = EmbeddingStats()
                if texts:
                    embeddings, embedding_stats = cache.embed_texts(texts, self.provider)
                self.storage.replace_review_embeddings(
                    run_id=run_record.run_id,
                    provider_name=self.provider.provider_name,
                    model_name=self.provider.model_name,
                    embeddings=[
                        PersistedEmbedding(review_id=review.review_id, vector=embedding.tolist())
                        for review, embedding in zip(
                            filtered.eligible_reviews,
                            embeddings,
                            strict=True,
                        )
                    ],
                )
                self.storage.replace_clusters(run_id=run_record.run_id, clusters=[])
                result = ClusteringResult(
                    run_id=run_record.run_id,
                    product_slug=run_record.product_slug,
                    iso_week=run_record.iso_week,
                    embedding_provider=self.provider.provider_name,
                    embedding_model=self.provider.model_name,
                    total_reviews_window=len(raw_reviews),
                    eligible_reviews=len(filtered.eligible_reviews),
                    filtered_language=filtered.filtered_language,
                    filtered_too_short=filtered.filtered_too_short,
                    filtered_duplicate_body=filtered.filtered_duplicate_body,
                    cluster_count=0,
                    noise_count=len(filtered.eligible_reviews),
                    noise_ratio=1.0 if filtered.eligible_reviews else 0.0,
                    embedding_stats=embedding_stats,
                    clusters=[],
                    low_signal=True,
                    warning="Not enough eligible reviews to form stable clusters.",
                )
                record_clusters_formed(
                    count=result.cluster_count,
                    embedding_provider=result.embedding_provider,
                    embedding_model=result.embedding_model,
                )
                return result

            embeddings, embedding_stats = cache.embed_texts(texts, self.provider)
            reduced_embeddings = self._reduce_dimensions(embeddings)
            labels = self._cluster_embeddings(reduced_embeddings)
            noise_count = int(np.sum(labels == -1))
            artifacts = self._build_cluster_artifacts(
                run_id=run_record.run_id,
                reviews=filtered.eligible_reviews,
                embeddings=embeddings,
                labels=labels,
            )
            noise_ratio = noise_count / max(len(filtered.eligible_reviews), 1)
            warning = None
            if not artifacts:
                warning = "No dominant clusters emerged from the current review set."
            elif noise_ratio > self.settings.cluster_noise_warning_ratio:
                warning = (
                    f"Noise ratio {noise_ratio:.2f} exceeded the warning threshold "
                    f"{self.settings.cluster_noise_warning_ratio:.2f}."
                )

            self.storage.replace_review_embeddings(
                run_id=run_record.run_id,
                provider_name=self.provider.provider_name,
                model_name=self.provider.model_name,
                embeddings=[
                    PersistedEmbedding(review_id=review.review_id, vector=embedding.tolist())
                    for review, embedding in zip(filtered.eligible_reviews, embeddings, strict=True)
                ],
            )
            self.storage.replace_clusters(
                run_id=run_record.run_id,
                clusters=[
                    PersistedCluster(
                        cluster_id=artifact.cluster_id,
                        run_id=artifact.run_id,
                        label=artifact.label,
                        size=artifact.size,
                        review_ids=artifact.review_ids,
                        representative_review_ids=artifact.representative_review_ids,
                        keyphrases=artifact.keyphrases,
                        medoid_review_id=artifact.medoid_review_id,
                        average_rating=artifact.average_rating,
                        rating_stddev=artifact.rating_stddev,
                    )
                    for artifact in artifacts
                ],
            )

            result = ClusteringResult(
                run_id=run_record.run_id,
                product_slug=run_record.product_slug,
                iso_week=run_record.iso_week,
                embedding_provider=self.provider.provider_name,
                embedding_model=self.provider.model_name,
                total_reviews_window=len(raw_reviews),
                eligible_reviews=len(filtered.eligible_reviews),
                filtered_language=filtered.filtered_language,
                filtered_too_short=filtered.filtered_too_short,
                filtered_duplicate_body=filtered.filtered_duplicate_body,
                cluster_count=len(artifacts),
                noise_count=noise_count,
                noise_ratio=noise_ratio,
                embedding_stats=embedding_stats,
                clusters=artifacts,
                low_signal=not artifacts,
                warning=warning,
            )
            record_clusters_formed(
                count=result.cluster_count,
                embedding_provider=result.embedding_provider,
                embedding_model=result.embedding_model,
            )
            return result

    def _reduce_dimensions(self, embeddings: np.ndarray) -> np.ndarray:
        if len(embeddings) <= 5:
            return embeddings

        n_neighbors = min(self.settings.cluster_umap_n_neighbors, max(len(embeddings) - 1, 2))
        n_components = min(
            self.settings.cluster_umap_components,
            max(2, min(embeddings.shape[1], len(embeddings) - 1)),
        )
        reducer = umap.UMAP(
            n_neighbors=n_neighbors,
            min_dist=self.settings.cluster_umap_min_dist,
            n_components=n_components,
            metric="cosine",
            random_state=self.settings.cluster_random_state,
            transform_seed=self.settings.cluster_random_state,
        )
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="n_jobs value 1 overridden to 1 by setting random_state.*",
                category=UserWarning,
            )
            reduced = cast(np.ndarray[Any, Any], reducer.fit_transform(embeddings))
        return np.asarray(reduced, dtype=np.float32)

    def _cluster_embeddings(self, embeddings: np.ndarray) -> np.ndarray:
        min_cluster_size = min(
            self.settings.cluster_min_cluster_size,
            max(2, len(embeddings) // 2),
        )
        if min_cluster_size < 2:
            return np.full(shape=(len(embeddings),), fill_value=-1, dtype=int)

        clusterer = hdbscan.HDBSCAN(
            min_cluster_size=min_cluster_size,
            metric="euclidean",
            cluster_selection_method="eom",
            prediction_data=False,
        )
        return cast(np.ndarray[Any, Any], clusterer.fit_predict(embeddings))

    def _build_cluster_artifacts(
        self,
        *,
        run_id: str,
        reviews: list[ReviewDocument],
        embeddings: np.ndarray,
        labels: np.ndarray,
    ) -> list[ClusterArtifact]:
        grouped_indices: dict[int, list[int]] = defaultdict(list)
        for index, label in enumerate(labels.tolist()):
            if label >= 0:
                grouped_indices[int(label)].append(index)

        raw_artifacts: list[ClusterArtifact] = []
        for label, indices in grouped_indices.items():
            cluster_reviews = [reviews[index] for index in indices]
            cluster_vectors = embeddings[indices]
            medoid_review_id = self._select_medoid(cluster_reviews, cluster_vectors)
            keyphrases = extract_keyphrases(
                [review.text for review in cluster_reviews],
                top_n=self.settings.cluster_keyphrases_top_n,
            )
            representatives = self._select_representatives(cluster_reviews, medoid_review_id)
            ratings = [review.rating for review in cluster_reviews if review.rating is not None]
            average_rating = float(sum(ratings) / len(ratings)) if ratings else None
            rating_stddev = (
                float(statistics.pstdev(ratings))
                if len(ratings) >= 2
                else 0.0 if ratings else None
            )

            raw_artifacts.append(
                ClusterArtifact(
                    cluster_id="",
                    run_id=run_id,
                    label=label,
                    size=len(cluster_reviews),
                    review_ids=sorted(review.review_id for review in cluster_reviews),
                    representative_review_ids=representatives,
                    keyphrases=keyphrases,
                    medoid_review_id=medoid_review_id,
                    average_rating=average_rating,
                    rating_stddev=rating_stddev,
                )
            )

        raw_artifacts.sort(key=lambda artifact: (-artifact.size, artifact.medoid_review_id))
        finalized: list[ClusterArtifact] = []
        for index, artifact in enumerate(raw_artifacts, start=1):
            finalized.append(
                artifact.model_copy(
                    update={"cluster_id": f"{run_id}_cluster_{index:02d}"}
                )
            )
        return finalized

    @staticmethod
    def _select_medoid(reviews: list[ReviewDocument], vectors: np.ndarray) -> str:
        distances = cast(np.ndarray[Any, Any], pairwise_distances(vectors, metric="cosine"))
        medoid_index = int(np.argmin(distances.sum(axis=1)))
        return reviews[medoid_index].review_id

    def _select_representatives(
        self,
        reviews: list[ReviewDocument],
        medoid_review_id: str,
    ) -> list[str]:
        selected: list[str] = [medoid_review_id]
        rated_reviews = [review for review in reviews if review.rating is not None]

        if rated_reviews:
            lowest = min(rated_reviews, key=lambda review: (review.rating or 0, review.review_id))
            highest = max(rated_reviews, key=lambda review: (review.rating or 0, review.review_id))
            for review in (lowest, highest):
                if review.review_id not in selected:
                    selected.append(review.review_id)

        newest = max(
            reviews,
            key=lambda review: (
                _review_timestamp(review),
                review.review_id,
            ),
        )
        if newest.review_id not in selected:
            selected.append(newest.review_id)

        return selected[: self.settings.cluster_representatives_per_cluster]


def _review_timestamp(review: ReviewDocument) -> str:
    value = review.review_updated_at or review.review_created_at
    return value.isoformat() if value is not None else ""
