from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Final
from xml.etree import ElementTree

import httpx

from agent.ingestion.models import RawReview, ReviewSource
from agent.logging import get_logger

APPSTORE_XML_URL: Final[str] = (
    "https://itunes.apple.com/{country}/rss/customerreviews/"
    "page={page}/id={app_id}/sortby=mostrecent/xml"
)
ATOM_NS: Final[str] = "http://www.w3.org/2005/Atom"
ITUNES_NS: Final[str] = "http://itunes.apple.com/rss"
XML_NAMESPACES: Final[dict[str, str]] = {"atom": ATOM_NS, "im": ITUNES_NS}

AppStorePageFetcher = Callable[[str, str, int], str]


class AppStoreReviewClient:
    def __init__(
        self,
        *,
        timeout_seconds: float = 15.0,
        max_pages: int = 10,
        fetch_page: AppStorePageFetcher | None = None,
    ) -> None:
        self.timeout_seconds = timeout_seconds
        self.max_pages = max_pages
        self.fetch_page = fetch_page or self._default_fetch_page
        self.logger = get_logger("pulse.ingestion.appstore")

    def fetch_reviews(
        self,
        *,
        app_id: str,
        country: str,
        lookback_start: datetime,
        week_end: datetime,
    ) -> list[RawReview]:
        reviews: list[RawReview] = []

        for page in range(1, self.max_pages + 1):
            xml_text = self.fetch_page(app_id, country, page)
            page_reviews = self.parse_feed(xml_text, app_id=app_id, country=country)

            if not page_reviews:
                break

            page_has_recent_reviews = False
            for review in page_reviews:
                review_time = review.review_updated_at or review.review_created_at
                if review_time is None:
                    continue

                if review_time >= lookback_start:
                    page_has_recent_reviews = True

                if lookback_start <= review_time <= week_end:
                    reviews.append(review)

            if not page_has_recent_reviews:
                break

        return reviews

    def _default_fetch_page(self, app_id: str, country: str, page: int) -> str:
        url = APPSTORE_XML_URL.format(country=country.lower(), page=page, app_id=app_id)
        response = httpx.get(
            url,
            follow_redirects=True,
            headers={"user-agent": "WeeklyProductReviewPulse/0.1"},
            timeout=self.timeout_seconds,
        )
        if response.status_code == 404:
            return ""
        response.raise_for_status()
        return response.text

    def parse_feed(
        self,
        xml_text: str,
        *,
        app_id: str,
        country: str,
    ) -> list[RawReview]:
        if not xml_text.strip():
            return []

        root = ElementTree.fromstring(xml_text)
        reviews: list[RawReview] = []

        for entry in root.findall("atom:entry", XML_NAMESPACES):
            rating_text = self._find_text(entry, "im:rating")
            external_id = self._find_text(entry, "atom:id")
            if rating_text is None or external_id is None:
                continue

            try:
                rating = int(rating_text)
            except ValueError:
                self.logger.warning(
                    "appstore_review_skipped",
                    reason="invalid_rating",
                    external_id=external_id,
                )
                continue

            updated_at = self._parse_datetime(self._find_text(entry, "atom:updated"))
            text_body = self._find_text_content(entry)
            source_url = self._find_link_href(entry, "related")
            if source_url is None:
                source_url = f"https://apps.apple.com/{country.lower()}/app/id{app_id}"

            raw_payload = {
                "entry_xml": ElementTree.tostring(entry, encoding="unicode"),
                "country": country.lower(),
                "app_id": app_id,
            }

            reviews.append(
                RawReview(
                    source=ReviewSource.APPSTORE,
                    external_id=external_id,
                    rating=rating,
                    title=self._find_text(entry, "atom:title"),
                    body=text_body,
                    author_alias=self._find_text(entry, "atom:author/atom:name"),
                    review_created_at=updated_at,
                    review_updated_at=updated_at,
                    locale=country.lower(),
                    app_version=self._find_text(entry, "im:version"),
                    source_url=source_url,
                    raw_payload=raw_payload,
                )
            )

        return reviews

    @staticmethod
    def _find_text(element: ElementTree.Element, path: str) -> str | None:
        node = element.find(path, XML_NAMESPACES)
        if node is None or node.text is None:
            return None
        text = node.text.strip()
        return text or None

    @staticmethod
    def _find_text_content(element: ElementTree.Element) -> str | None:
        for content in element.findall("atom:content", XML_NAMESPACES):
            content_type = content.attrib.get("type")
            if content_type == "text" and content.text:
                text = content.text.strip()
                if text:
                    return text
        return None

    @staticmethod
    def _find_link_href(element: ElementTree.Element, rel: str) -> str | None:
        for link in element.findall("atom:link", XML_NAMESPACES):
            href = link.attrib.get("href")
            if link.attrib.get("rel") == rel and href:
                return href
        return None

    @staticmethod
    def _parse_datetime(value: str | None) -> datetime | None:
        if value is None:
            return None
        return datetime.fromisoformat(value)
