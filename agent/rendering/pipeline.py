from __future__ import annotations

import hashlib
import html
import json
import re
from pathlib import Path

from agent.config import Settings
from agent.logging import get_logger
from agent.pulse_types import ProductConfig, StoredRunRecord
from agent.rendering.models import (
    DOC_SECTION_URL_PLACEHOLDER,
    DocsBlock,
    DocsRequestTree,
    EmailTeaserPayload,
    RenderArtifact,
    RenderedTheme,
    RenderResult,
)
from agent.storage import Storage
from agent.summarization.models import SummarizedTheme

WHO_THIS_HELPS = [
    "Product: Prioritize roadmap decisions from recurring customer themes.",
    "Support: Spot repeating complaints and quality issues faster.",
    "Leadership: Get a fast health snapshot grounded in customer voice.",
]
REDACTION_MARKERS = (
    "[REDACTED_EMAIL]",
    "[REDACTED_PHONE]",
    "[REDACTED_AADHAAR]",
)


def run_render_for_run(
    *,
    settings: Settings,
    storage: Storage,
    run_record: StoredRunRecord,
    product: ProductConfig,
) -> RenderResult:
    service = RenderService(settings=settings, storage=storage)
    return service.run(run_record=run_record, product=product)


class RenderService:
    def __init__(self, *, settings: Settings, storage: Storage) -> None:
        self.settings = settings
        self.storage = storage
        self.logger = get_logger("pulse.rendering")

    def run(
        self,
        *,
        run_record: StoredRunRecord,
        product: ProductConfig,
    ) -> RenderResult:
        themes = self.storage.fetch_themes_for_run(run_record.run_id)
        rendered_themes, quote_lines, action_lines, low_signal = self._select_report_content(themes)

        anchor_key = _build_anchor_key(product.slug, run_record.iso_week)
        section_heading = f"{product.display_name} Weekly Review Pulse - {run_record.iso_week}"
        machine_key_line = f"Run key: {anchor_key}"
        period_label = (
            "Period: "
            f"{_format_date(run_record.lookback_start)} to {_format_date(run_record.week_end)} "
            f"({run_record.lookback_weeks}-week window)"
        )

        docs_request_tree = self._build_docs_request_tree(
            section_heading=section_heading,
            machine_key_line=machine_key_line,
            anchor_key=anchor_key,
            period_label=period_label,
            rendered_themes=rendered_themes,
            quote_lines=quote_lines,
            action_lines=action_lines,
            low_signal=low_signal,
        )
        email_teaser = self._build_email_teaser(
            product=product,
            iso_week=run_record.iso_week,
            period_label=period_label,
            rendered_themes=rendered_themes,
            action_lines=action_lines,
            low_signal=low_signal,
        )

        docs_payload_hash = _stable_hash(docs_request_tree.model_dump(mode="json"))
        email_payload_hash = _stable_hash(email_teaser.model_dump(mode="json"))
        artifact_base = RenderArtifact(
            run_id=run_record.run_id,
            product_slug=product.slug,
            product_display_name=product.display_name,
            iso_week=run_record.iso_week,
            lookback_weeks=run_record.lookback_weeks,
            section_heading=section_heading,
            anchor_key=anchor_key,
            machine_key_line=machine_key_line,
            period_label=period_label,
            available_theme_count=len(themes),
            rendered_theme_count=len(rendered_themes),
            top_themes=rendered_themes,
            quotes=quote_lines,
            action_ideas=action_lines,
            who_this_helps=WHO_THIS_HELPS,
            docs_request_tree=docs_request_tree,
            email_teaser=email_teaser,
            docs_payload_hash=docs_payload_hash,
            email_payload_hash=email_payload_hash,
            artifact_hash="",
        )
        artifact_hash = _stable_hash(
            artifact_base.model_dump(mode="json", exclude={"artifact_hash"})
        )
        artifact = artifact_base.model_copy(update={"artifact_hash": artifact_hash})

        artifact_path = self._artifact_path(product.slug, run_record.run_id)
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(
            json.dumps(artifact.model_dump(mode="json"), indent=2, sort_keys=True),
            encoding="utf-8",
        )

        warnings: list[str] = []
        if len(themes) > len(rendered_themes):
            warnings.append(
                "Rendered the top "
                f"{len(rendered_themes)} themes out of {len(themes)} available themes."
            )
        dropped_quotes = len(rendered_themes) - len(quote_lines)
        if dropped_quotes > 0:
            warnings.append(
                "Dropped "
                f"{dropped_quotes} quotes that were missing or contained redaction artifacts."
            )
        if low_signal:
            warnings.append(
                "Rendered a low-signal fallback report because no grounded themes "
                "were available."
            )

        warning = "; ".join(warnings) if warnings else None
        self.logger.info(
            "render_artifact_written",
            run_id=run_record.run_id,
            artifact_path=str(artifact_path),
            rendered_themes=len(rendered_themes),
            quote_count=len(quote_lines),
            action_count=len(action_lines),
            warning=warning,
        )

        return RenderResult(
            run_id=run_record.run_id,
            product_slug=product.slug,
            iso_week=run_record.iso_week,
            anchor_key=anchor_key,
            available_theme_count=len(themes),
            rendered_theme_count=len(rendered_themes),
            quote_count=len(quote_lines),
            action_count=len(action_lines),
            docs_block_count=len(docs_request_tree.blocks),
            artifact_path=artifact_path,
            docs_payload_hash=docs_payload_hash,
            email_payload_hash=email_payload_hash,
            artifact_hash=artifact_hash,
            low_signal=low_signal,
            warning=warning,
        )

    def _select_report_content(
        self,
        themes: list[SummarizedTheme],
    ) -> tuple[list[RenderedTheme], list[str], list[str], bool]:
        if not themes:
            return (
                [],
                [],
                ["Review the grouped evidence and manually triage this low-signal week."],
                True,
            )

        selected_themes = themes[: self.settings.render_max_themes]
        rendered_themes: list[RenderedTheme] = []
        quote_lines: list[str] = []
        action_lines: list[str] = []

        for source_theme in selected_themes:
            rendered_theme = RenderedTheme(
                theme_id=source_theme.theme_id,
                name=_clean_line(source_theme.name),
                summary=_limit_chars(_clean_line(source_theme.summary), 220),
                quote_text=self._sanitize_quote(source_theme.quote_text),
                quote_review_id=source_theme.quote_review_id,
                action_idea=_limit_chars(_clean_line(source_theme.action_ideas[0]), 180)
                if source_theme.action_ideas
                else None,
                coverage_count=source_theme.coverage_count,
                average_rating=source_theme.average_rating,
                low_coverage=source_theme.low_coverage,
            )
            rendered_themes.append(rendered_theme)

        for rendered_theme in rendered_themes:
            if rendered_theme.quote_text is None:
                continue
            quote_lines.append(f"\"{rendered_theme.quote_text}\" - {rendered_theme.name}")
            if len(quote_lines) >= self.settings.render_max_quotes:
                break

        seen_actions: set[str] = set()
        for rendered_theme in rendered_themes:
            if rendered_theme.action_idea is None:
                continue
            action_line = f"{rendered_theme.name}: {rendered_theme.action_idea}"
            action_key = action_line.casefold()
            if action_key in seen_actions:
                continue
            seen_actions.add(action_key)
            action_lines.append(action_line)
            if len(action_lines) >= self.settings.render_max_action_ideas:
                break

        if not action_lines:
            action_lines = [
                "Review the grouped evidence and manually triage the next steps for this week."
            ]

        return rendered_themes, quote_lines, action_lines, False

    def _build_docs_request_tree(
        self,
        *,
        section_heading: str,
        machine_key_line: str,
        anchor_key: str,
        period_label: str,
        rendered_themes: list[RenderedTheme],
        quote_lines: list[str],
        action_lines: list[str],
        low_signal: bool,
    ) -> DocsRequestTree:
        blocks: list[DocsBlock] = [
            DocsBlock(type="heading", level=1, text=section_heading),
            DocsBlock(type="paragraph", text=period_label),
        ]

        blocks.append(DocsBlock(type="heading", level=2, text="Top themes"))
        if rendered_themes:
            blocks.append(
                DocsBlock(
                    type="numbered_list",
                    items=[f"{theme.name} - {theme.summary}" for theme in rendered_themes],
                )
            )
        else:
            blocks.append(
                DocsBlock(
                    type="paragraph",
                    text="No grounded themes were available for this run.",
                )
            )

        blocks.append(DocsBlock(type="heading", level=2, text="Representative quotes"))
        if quote_lines:
            blocks.extend(DocsBlock(type="blockquote", text=quote) for quote in quote_lines)
        else:
            blocks.append(
                DocsBlock(
                    type="paragraph",
                    text="No publishable verbatim quotes were available after quote validation.",
                )
            )

        blocks.append(DocsBlock(type="heading", level=2, text="Action ideas"))
        blocks.append(DocsBlock(type="numbered_list", items=action_lines))

        blocks.append(DocsBlock(type="heading", level=2, text="Who this helps"))
        blocks.append(DocsBlock(type="bullet_list", items=WHO_THIS_HELPS))
        blocks.append(DocsBlock(type="paragraph", text=machine_key_line))

        if low_signal:
            blocks.append(
                DocsBlock(
                    type="paragraph",
                    text=(
                        "This is a low-signal report because no grounded summarized "
                        "themes were available."
                    ),
                )
            )

        markdown = _render_markdown(blocks)
        return DocsRequestTree(
            anchor_key=anchor_key,
            section_heading=section_heading,
            machine_key_line=machine_key_line,
            blocks=blocks,
            markdown=markdown,
        )

    def _build_email_teaser(
        self,
        *,
        product: ProductConfig,
        iso_week: str,
        period_label: str,
        rendered_themes: list[RenderedTheme],
        action_lines: list[str],
        low_signal: bool,
    ) -> EmailTeaserPayload:
        subject = f"{product.display_name} Weekly Review Pulse - {iso_week}"
        teaser_theme_lines = [
            theme.name
            for theme in rendered_themes[: self.settings.render_email_teaser_themes]
        ]
        if not teaser_theme_lines:
            teaser_theme_lines = ["No grounded themes were available for this run."]

        plain_lines = [
            subject,
            "",
            period_label,
            "",
            "Top themes",
        ]
        plain_lines.extend(f"- {line}" for line in teaser_theme_lines)
        plain_lines.extend(["", "Action ideas"])
        plain_lines.extend(
            f"- {line}" for line in action_lines[: self.settings.render_max_action_ideas]
        )
        if low_signal:
            plain_lines.extend(["", "Note: This week is low-signal and may need manual review."])
        plain_lines.extend(["", f"Read full report: {DOC_SECTION_URL_PLACEHOLDER}"])
        plain_text_template = "\n".join(plain_lines)

        html_lines = [
            "<html>",
            "  <body>",
            f"    <p><strong>{html.escape(subject)}</strong></p>",
            f"    <p>{html.escape(period_label)}</p>",
            "    <p><strong>Top themes</strong></p>",
            "    <ul>",
        ]
        for line in teaser_theme_lines:
            html_lines.append(f"      <li>{html.escape(line)}</li>")
        html_lines.extend(
            [
                "    </ul>",
                "    <p><strong>Action ideas</strong></p>",
                "    <ul>",
            ]
        )
        for line in action_lines[: self.settings.render_max_action_ideas]:
            html_lines.append(f"      <li>{html.escape(line)}</li>")
        html_lines.extend(["    </ul>"])
        if low_signal:
            html_lines.append(
                "    <p><em>This week is low-signal and may need manual review.</em></p>"
            )
        html_lines.extend(
            [
                f'    <p><a href="{DOC_SECTION_URL_PLACEHOLDER}">Read full report</a></p>',
                "  </body>",
                "</html>",
            ]
        )
        html_template = "\n".join(html_lines)

        return EmailTeaserPayload(
            subject=subject,
            plain_text_template=plain_text_template,
            html_template=html_template,
        )

    def _artifact_path(self, product_slug: str, run_id: str) -> Path:
        return self.settings.artifacts_dir / "render" / product_slug / f"{run_id}.json"

    @staticmethod
    def _sanitize_quote(quote_text: str | None) -> str | None:
        if quote_text is None:
            return None
        cleaned = _clean_line(quote_text)
        if not cleaned:
            return None
        if any(marker in cleaned.upper() for marker in REDACTION_MARKERS):
            return None
        return cleaned


def _build_anchor_key(product_slug: str, iso_week: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "-", f"{product_slug}-{iso_week}".lower()).strip("-")
    return f"pulse-{normalized}"


def _format_date(value: object) -> str:
    from datetime import datetime

    if not isinstance(value, datetime):
        raise TypeError("Expected datetime for render date formatting.")
    return value.strftime("%B %d, %Y")


def _clean_line(value: str) -> str:
    return " ".join(value.split()).strip()


def _limit_chars(value: str, limit: int) -> str:
    cleaned = _clean_line(value)
    if len(cleaned) <= limit:
        return cleaned
    truncated = cleaned[: limit - 3].rstrip(" ,;:-")
    return f"{truncated}..."


def _render_markdown(blocks: list[DocsBlock]) -> str:
    lines: list[str] = []
    for block in blocks:
        if block.type == "heading":
            lines.append(f"{'#' * (block.level or 1)} {block.text}")
        elif block.type == "paragraph":
            lines.append(block.text or "")
        elif block.type == "numbered_list":
            lines.extend(f"{index}. {item}" for index, item in enumerate(block.items, start=1))
        elif block.type == "bullet_list":
            lines.extend(f"- {item}" for item in block.items)
        elif block.type == "blockquote":
            lines.append(f"> {block.text}")
        lines.append("")
    return "\n".join(lines).strip() + "\n"


def _stable_json_dumps(payload: object) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _stable_hash(payload: object) -> str:
    return hashlib.sha256(_stable_json_dumps(payload).encode("utf-8")).hexdigest()
