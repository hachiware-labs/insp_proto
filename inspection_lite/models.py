from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
import json
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo


STATUS_QUEUED = "queued"
STATUS_RUNNING = "running"
STATUS_DONE = "done"
STATUS_FAILED = "failed"
STATUS_CANCEL_REQUESTED = "cancel_requested"
STATUS_CANCELED = "canceled"
TERMINAL_STATUSES = {STATUS_DONE, STATUS_FAILED, STATUS_CANCELED}

SOURCE_MANUAL = "manual"
SOURCE_SCHEDULE = "schedule"

SCHEDULE_DAILY_TIME = "daily_time"
SCHEDULE_INTERVAL = "interval"


def now_iso(tz_name: str = "Asia/Tokyo") -> str:
    return datetime.now(ZoneInfo(tz_name)).isoformat(timespec="seconds")


def make_job_id(prefix: str = "job", tz_name: str = "Asia/Tokyo") -> str:
    stamp = datetime.now(ZoneInfo(tz_name)).strftime("%Y%m%d-%H%M%S")
    return f"{prefix}-{stamp}-{uuid4().hex[:8]}"


def make_task_id(name: str | None = None) -> str:
    if name:
        cleaned = "".join(ch if ch.isalnum() else "-" for ch in name.lower()).strip("-")
        cleaned = "-".join(part for part in cleaned.split("-") if part)
        if cleaned:
            return f"task-{cleaned}-{uuid4().hex[:6]}"
    return f"task-{uuid4().hex[:12]}"


def parse_payload_json(raw: str | None) -> dict[str, Any]:
    if raw is None or raw.strip() == "":
        return {}
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("payload JSON must be an object")
    return value


@dataclass
class Job:
    job_id: str
    source: str
    process: str
    lot: str
    recipe: str
    runner_command: str
    payload: dict[str, Any]
    created_at: str

    @classmethod
    def create(
        cls,
        *,
        source: str,
        process: str,
        lot: str,
        recipe: str,
        runner_command: str,
        payload: dict[str, Any] | None = None,
        tz_name: str = "Asia/Tokyo",
    ) -> "Job":
        return cls(
            job_id=make_job_id(tz_name=tz_name),
            source=source,
            process=process,
            lot=lot,
            recipe=recipe,
            runner_command=runner_command,
            payload=payload or {},
            created_at=now_iso(tz_name),
        )

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Job":
        return cls(
            job_id=str(data["job_id"]),
            source=str(data.get("source", SOURCE_MANUAL)),
            process=str(data.get("process", "")),
            lot=str(data.get("lot", "")),
            recipe=str(data.get("recipe", "")),
            runner_command=str(data.get("runner_command", "")),
            payload=dict(data.get("payload") or {}),
            created_at=str(data.get("created_at", "")),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class JobState:
    job_id: str
    status: str
    visible: bool
    server_id: str | None
    worker_id: str | None
    created_at: str
    started_at: str | None = None
    finished_at: str | None = None
    exit_code: int | None = None
    message: str | None = None
    source: str | None = None
    process: str | None = None
    lot: str | None = None
    recipe: str | None = None

    @classmethod
    def queued(cls, job: Job) -> "JobState":
        return cls(
            job_id=job.job_id,
            status=STATUS_QUEUED,
            visible=True,
            server_id=None,
            worker_id=None,
            created_at=job.created_at,
            source=job.source,
            process=job.process,
            lot=job.lot,
            recipe=job.recipe,
        )

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "JobState":
        return cls(
            job_id=str(data["job_id"]),
            status=str(data["status"]),
            visible=bool(data.get("visible", True)),
            server_id=data.get("server_id"),
            worker_id=data.get("worker_id"),
            created_at=str(data.get("created_at", "")),
            started_at=data.get("started_at"),
            finished_at=data.get("finished_at"),
            exit_code=data.get("exit_code"),
            message=data.get("message"),
            source=data.get("source"),
            process=data.get("process"),
            lot=data.get("lot"),
            recipe=data.get("recipe"),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class CancelRequest:
    job_id: str
    requested_at: str
    requested_by: str = "ui"
    reason: str = "user_requested"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class Heartbeat:
    server_id: str
    role: str
    hostname: str
    timestamp: str
    max_jobs: int
    current_jobs: int
    running_job_id: str | None
    status: str = "alive"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ScheduleTask:
    task_id: str
    name: str
    enabled: bool
    schedule_type: str
    time: str | None
    interval_seconds: int | None
    runner_command: str
    process: str
    lot: str
    recipe: str
    payload: dict[str, Any]
    last_enqueued_at: str | None
    next_run_at: str | None
    created_at: str
    updated_at: str

    @classmethod
    def create(
        cls,
        *,
        name: str,
        enabled: bool,
        schedule_type: str,
        time: str | None,
        interval_seconds: int | None,
        runner_command: str,
        process: str,
        lot: str,
        recipe: str,
        payload: dict[str, Any] | None,
        tz_name: str = "Asia/Tokyo",
    ) -> "ScheduleTask":
        timestamp = now_iso(tz_name)
        return cls(
            task_id=make_task_id(name),
            name=name,
            enabled=enabled,
            schedule_type=schedule_type,
            time=time,
            interval_seconds=interval_seconds,
            runner_command=runner_command,
            process=process,
            lot=lot,
            recipe=recipe,
            payload=payload or {},
            last_enqueued_at=None,
            next_run_at=None,
            created_at=timestamp,
            updated_at=timestamp,
        )

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ScheduleTask":
        return cls(
            task_id=str(data["task_id"]),
            name=str(data["name"]),
            enabled=bool(data.get("enabled", True)),
            schedule_type=str(data.get("schedule_type", SCHEDULE_DAILY_TIME)),
            time=data.get("time"),
            interval_seconds=data.get("interval_seconds"),
            runner_command=str(data.get("runner_command", "")),
            process=str(data.get("process", "")),
            lot=str(data.get("lot", "")),
            recipe=str(data.get("recipe", "")),
            payload=dict(data.get("payload") or {}),
            last_enqueued_at=data.get("last_enqueued_at"),
            next_run_at=data.get("next_run_at"),
            created_at=str(data.get("created_at", "")),
            updated_at=str(data.get("updated_at", "")),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
