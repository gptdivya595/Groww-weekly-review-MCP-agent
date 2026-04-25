from __future__ import annotations

from agent.clustering.models import PersistedCluster, ReviewDocument
from agent.config import Settings
from agent.logging import get_logger
from agent.pulse_types import StoredRunRecord
from agent.storage import Storage
from agent.summarization.llm_client import (
    HeuristicSummarizationClient,
    SummarizationClient,
    SummarizationClientError,
    build_summarization_client,
    sanitize_action_ideas,
    sanitize_summary,
    sanitize_theme_name,
)
from agent.summarization.models import (
    ClusterEvidence,
    SummarizationResult,
    SummarizedTheme,
)
from agent.summarization.verbatim import validate_quote_candidate
from agent.telemetry import record_themes_generated, start_span


def run_summarization_for_run(
    *,
    settings: Settings,
    storage: Storage,
    run_record: StoredRunRecord,
) -> SummarizationResult:
    client = build_summarization_client(
        provider_name=settings.summarization_provider,
        model_name=settings.summarization_model,
        timeout_seconds=settings.http_timeout_seconds,
        max_output_tokens=settings.summarization_max_output_tokens,
    )
    service = SummarizationService(settings=settings, storage=storage, client=client)
    return service.run(run_record=run_record)


class SummarizationService:
    def __init__(
        self,
        *,
        settings: Settings,
        storage: Storage,
        client: SummarizationClient,
    ) -> None:
        self.settings = settings
        self.storage = storage
        self.client = client
        self.logger = get_logger("pulse.summarization")
        self._fallback_client = HeuristicSummarizationClient()

    def run(self, *, run_record: StoredRunRecord) -> SummarizationResult:
        with start_span(
            "summarization.run",
            {
                "product_slug": run_record.product_slug,
                "iso_week": run_record.iso_week,
                "provider": self.client.provider_name,
                "model": self.client.model_name,
            },
        ):
            stored_clusters = self.storage.fetch_clusters_for_run(run_record.run_id)
            if not stored_clusters:
                self.storage.replace_themes(run_id=run_record.run_id, themes=[])
                result = SummarizationResult(
                    run_id=run_record.run_id,
                    product_slug=run_record.product_slug,
                    iso_week=run_record.iso_week,
                    summarization_provider=self.client.provider_name,
                    summarization_model=self.client.model_name,
                    clusters_available=0,
                    clusters_summarized=0,
                    theme_count=0,
                    invalid_quote_count=0,
                    quote_omission_count=0,
                    retry_count=0,
                    fallback_count=0,
                    low_signal=True,
                    warning="No persisted clusters were available to summarize.",
                    themes=[],
                )
                record_themes_generated(
                    count=result.theme_count,
                    provider=result.summarization_provider,
                    model=result.summarization_model,
                )
                return result

            selected_clusters = stored_clusters[: self.settings.summarization_max_clusters]
            themes: list[SummarizedTheme] = []
            retry_count = 0
            fallback_count = 0
            invalid_quote_count = 0
            quote_omission_count = 0

            for cluster in selected_clusters:
                evidence = self._build_evidence(cluster)
                fallback_draft = self._fallback_client.summarize_cluster(evidence)

                candidate_draft = fallback_draft
                applied_provider = self.client.provider_name
                applied_model = self.client.model_name

                if self.client.provider_name != "heuristic":
                    attempts = max(1, self.settings.summarization_retry_attempts + 1)
                    last_error: str | None = None
                    for attempt in range(attempts):
                        try:
                            candidate_draft = self.client.summarize_cluster(evidence)
                            break
                        except SummarizationClientError as exc:
                            last_error = str(exc)
                            if attempt < attempts - 1:
                                retry_count += 1
                                continue
                            candidate_draft = fallback_draft
                            fallback_count += 1
                            applied_provider = self._fallback_client.provider_name
                            applied_model = self._fallback_client.model_name
                            self.logger.warning(
                                "summarization_fallback",
                                cluster_id=evidence.cluster_id,
                                error=last_error,
                            )

                validated_quote = None
                if candidate_draft.quote_text:
                    validated_quote = validate_quote_candidate(
                        candidate_draft.quote_text,
                        evidence.reviews,
                        preferred_review_id=candidate_draft.quote_review_id,
                    )
                    if validated_quote is None:
                        invalid_quote_count += 1
                        quote_omission_count += 1

                name = sanitize_theme_name(candidate_draft.name, evidence, fallback_draft.name)
                summary = sanitize_summary(
                    candidate_draft.summary,
                    evidence,
                    fallback_draft.summary,
                )
                action_ideas = sanitize_action_ideas(
                    candidate_draft.action_ideas,
                    evidence,
                    fallback_draft.action_ideas,
                )

                if evidence.size < self.settings.summarization_low_coverage_threshold and (
                    "Signal is limited" not in summary
                ):
                    summary = (
                        f"{summary} Signal is limited because this theme is based on only "
                        f"{evidence.size} reviews."
                    )

                themes.append(
                    SummarizedTheme(
                        theme_id=f"{evidence.cluster_id}_theme",
                        run_id=run_record.run_id,
                        cluster_id=evidence.cluster_id,
                        name=name,
                        summary=summary,
                        keyphrases=evidence.keyphrases,
                        medoid_review_id=evidence.medoid_review_id,
                        quote_review_id=validated_quote.review_id if validated_quote else None,
                        quote_text=validated_quote.text if validated_quote else None,
                        action_ideas=action_ideas,
                        representative_review_ids=evidence.representative_review_ids,
                        coverage_count=evidence.size,
                        average_rating=evidence.average_rating,
                        rating_stddev=evidence.rating_stddev,
                        model_provider=applied_provider,
                        model_name=applied_model,
                        low_coverage=evidence.size
                        < self.settings.summarization_low_coverage_threshold,
                    )
                )

            self.storage.replace_themes(run_id=run_record.run_id, themes=themes)

            warning = None
            low_signal = False
            if not themes:
                warning = "No themes were produced from the stored clusters."
                low_signal = True
            elif len(stored_clusters) > len(selected_clusters):
                warning = (
                    f"Summarized the top {len(selected_clusters)} clusters out of "
                    f"{len(stored_clusters)} available clusters."
                )

            result = SummarizationResult(
                run_id=run_record.run_id,
                product_slug=run_record.product_slug,
                iso_week=run_record.iso_week,
                summarization_provider=self.client.provider_name,
                summarization_model=self.client.model_name,
                clusters_available=len(stored_clusters),
                clusters_summarized=len(selected_clusters),
                theme_count=len(themes),
                invalid_quote_count=invalid_quote_count,
                quote_omission_count=quote_omission_count,
                retry_count=retry_count,
                fallback_count=fallback_count,
                low_signal=low_signal,
                warning=warning,
                themes=themes,
            )
            record_themes_generated(
                count=result.theme_count,
                provider=result.summarization_provider,
                model=result.summarization_model,
            )
            return result

    def _build_evidence(self, cluster: PersistedCluster) -> ClusterEvidence:
        cluster_reviews = self.storage.fetch_reviews_by_ids(cluster.review_ids)
        selected_reviews = _select_evidence_reviews(
            cluster_reviews,
            representative_review_ids=cluster.representative_review_ids,
            medoid_review_id=cluster.medoid_review_id,
            max_reviews=self.settings.summarization_max_reviews_per_cluster,
        )
        return ClusterEvidence(
            cluster_id=cluster.cluster_id,
            run_id=cluster.run_id,
            label=cluster.label,
            size=cluster.size,
            review_ids=cluster.review_ids,
            representative_review_ids=cluster.representative_review_ids,
            keyphrases=cluster.keyphrases,
            medoid_review_id=cluster.medoid_review_id,
            average_rating=cluster.average_rating,
            rating_stddev=cluster.rating_stddev,
            reviews=selected_reviews,
        )


def _select_evidence_reviews(
    reviews: list[ReviewDocument],
    *,
    representative_review_ids: list[str],
    medoid_review_id: str,
    max_reviews: int,
) -> list[ReviewDocument]:
    ordered = sorted(
        reviews,
        key=lambda review: (
            review.review_id not in representative_review_ids,
            review.review_id != medoid_review_id,
            review.rating if review.rating is not None else 99,
            review.review_id,
        ),
    )
    return ordered[:max_reviews]
