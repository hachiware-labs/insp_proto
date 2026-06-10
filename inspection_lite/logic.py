from __future__ import annotations

from dataclasses import replace
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from .models import (
    SCHEDULE_DAILY_TIME,
    SCHEDULE_INTERVAL,
    STATUS_CANCELED,
    STATUS_DONE,
    STATUS_FAILED,
    ScheduleTask,
)


def parse_iso_datetime(value: str, tz_name: str = "Asia/Tokyo") -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo(tz_name))
    return parsed


def compute_next_run_at(
    task: ScheduleTask, now: datetime | None = None, tz_name: str = "Asia/Tokyo"
) -> str | None:
    if not task.enabled:
        return None

    zone = ZoneInfo(tz_name)
    current = now.astimezone(zone) if now else datetime.now(zone)

    if task.schedule_type == SCHEDULE_DAILY_TIME:
        if not task.time:
            raise ValueError("daily_time schedule requires time")
        hour_text, minute_text = task.time.split(":", maxsplit=1)
        target = time(hour=int(hour_text), minute=int(minute_text), tzinfo=zone)
        next_run = datetime.combine(current.date(), target)
        if next_run <= current:
            next_run = next_run + timedelta(days=1)
        return next_run.isoformat(timespec="seconds")

    if task.schedule_type == SCHEDULE_INTERVAL:
        if not task.interval_seconds or task.interval_seconds <= 0:
            raise ValueError("interval schedule requires a positive interval_seconds")
        base = (
            parse_iso_datetime(task.last_enqueued_at, tz_name)
            if task.last_enqueued_at
            else current
        )
        next_run = base + timedelta(seconds=task.interval_seconds)
        while next_run <= current:
            next_run = next_run + timedelta(seconds=task.interval_seconds)
        return next_run.astimezone(zone).isoformat(timespec="seconds")

    raise ValueError(f"unknown schedule_type: {task.schedule_type}")


def with_next_run(
    task: ScheduleTask, now: datetime | None = None, tz_name: str = "Asia/Tokyo"
) -> ScheduleTask:
    return replace(task, next_run_at=compute_next_run_at(task, now, tz_name))


def is_task_due(task: ScheduleTask, now: datetime | None = None, tz_name: str = "Asia/Tokyo") -> bool:
    if not task.enabled or not task.next_run_at:
        return False
    zone = ZoneInfo(tz_name)
    current = now.astimezone(zone) if now else datetime.now(zone)
    return parse_iso_datetime(task.next_run_at, tz_name) <= current


def final_status_from_exit_code(exit_code: int) -> str:
    if exit_code == 0:
        return STATUS_DONE
    if exit_code == 130:
        return STATUS_CANCELED
    return STATUS_FAILED


def heartbeat_display_status(timestamp: str, now: datetime | None = None, stale_after_sec: int = 30) -> str:
    current = now or datetime.now(ZoneInfo("UTC"))
    seen_at = parse_iso_datetime(timestamp).astimezone(current.tzinfo)
    age = current - seen_at
    return "unknown" if age > timedelta(seconds=stale_after_sec) else "alive"
