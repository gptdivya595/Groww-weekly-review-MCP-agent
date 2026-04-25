from __future__ import annotations

import re
from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from agent.pulse_types import RunWindow

ISO_WEEK_PATTERN = re.compile(r"^(?P<year>\d{4})-W(?P<week>0[1-9]|[1-4]\d|5[0-3])$")


def current_time(tz_name: str) -> datetime:
    return datetime.now(ZoneInfo(tz_name))


def current_iso_week(tz_name: str) -> str:
    now = current_time(tz_name)
    iso_year, iso_week, _ = now.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"


def generate_run_id(now: datetime | None = None) -> str:
    timestamp = (now or datetime.now(UTC)).strftime("%Y%m%d_%H%M%S_%f")
    return f"run_{timestamp}"


def resolve_iso_week_window(
    iso_week: str,
    lookback_weeks: int,
    tz_name: str,
) -> RunWindow:
    match = ISO_WEEK_PATTERN.match(iso_week)
    if match is None:
        raise ValueError(f"Invalid ISO week: {iso_week}")

    year = int(match.group("year"))
    week = int(match.group("week"))
    tz = ZoneInfo(tz_name)

    week_start_date = date.fromisocalendar(year, week, 1)
    week_end_date = date.fromisocalendar(year, week, 7)

    week_start = datetime.combine(week_start_date, time.min, tzinfo=tz)
    week_end = datetime.combine(week_end_date, time.max, tzinfo=tz)
    lookback_start = week_start - timedelta(weeks=max(lookback_weeks - 1, 0))

    return RunWindow(
        iso_week=iso_week,
        week_start=week_start,
        week_end=week_end,
        lookback_start=lookback_start,
        lookback_weeks=lookback_weeks,
    )


def next_weekly_schedule_time(
    *,
    tz_name: str,
    iso_weekday: int,
    hour: int,
    minute: int,
    now: datetime | None = None,
) -> datetime:
    if not 1 <= iso_weekday <= 7:
        raise ValueError("iso_weekday must be between 1 and 7.")
    if not 0 <= hour <= 23:
        raise ValueError("hour must be between 0 and 23.")
    if not 0 <= minute <= 59:
        raise ValueError("minute must be between 0 and 59.")

    tz = ZoneInfo(tz_name)
    current = now.astimezone(tz) if now is not None else datetime.now(tz)
    scheduled_today = current.replace(hour=hour, minute=minute, second=0, microsecond=0)
    days_ahead = iso_weekday - current.isoweekday()
    if days_ahead < 0 or (days_ahead == 0 and scheduled_today <= current):
        days_ahead += 7

    next_date = current.date() + timedelta(days=days_ahead)
    return datetime.combine(next_date, time(hour, minute), tzinfo=tz)
