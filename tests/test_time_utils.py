from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from agent.time_utils import next_weekly_schedule_time


def test_next_weekly_schedule_time_uses_same_day_when_slot_is_ahead() -> None:
    now = datetime(2026, 4, 24, 8, 30, tzinfo=ZoneInfo("Asia/Kolkata"))

    next_run = next_weekly_schedule_time(
        tz_name="Asia/Kolkata",
        iso_weekday=5,
        hour=9,
        minute=0,
        now=now,
    )

    assert next_run == datetime(2026, 4, 24, 9, 0, tzinfo=ZoneInfo("Asia/Kolkata"))


def test_next_weekly_schedule_time_rolls_forward_when_slot_has_passed() -> None:
    now = datetime(2026, 4, 24, 10, 0, tzinfo=ZoneInfo("Asia/Kolkata"))

    next_run = next_weekly_schedule_time(
        tz_name="Asia/Kolkata",
        iso_weekday=5,
        hour=9,
        minute=0,
        now=now,
    )

    assert next_run == datetime(2026, 5, 1, 9, 0, tzinfo=ZoneInfo("Asia/Kolkata"))
