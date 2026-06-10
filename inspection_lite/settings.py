from __future__ import annotations

from dataclasses import dataclass
import os
import socket


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return int(value)


@dataclass(frozen=True)
class Settings:
    server_id: str
    role: str
    ui_enabled: bool
    nats_url: str
    worker_max_jobs: int
    default_runner_cmd: str
    scheduler_enabled: bool
    scheduler_timezone: str
    scheduler_interval_sec: int
    heartbeat_interval_sec: int
    heartbeat_api_host: str
    heartbeat_api_port: int
    ui_host: str
    ui_port: int
    stream_name: str = "INSPECTION_JOBS"
    job_subject: str = "inspection.jobs"
    consumer_name: str = "inspection-workers"
    job_state_bucket: str = "INSPECTION_JOB_STATE"
    cancel_bucket: str = "INSPECTION_CANCEL"
    heartbeat_bucket: str = "INSPECTION_HEARTBEAT"
    schedule_bucket: str = "INSPECTION_SCHEDULE_TASKS"

    @classmethod
    def from_env(cls) -> "Settings":
        hostname = socket.gethostname()
        return cls(
            server_id=os.getenv("SERVER_ID", hostname),
            role=os.getenv("INSPECTION_ROLE", "worker"),
            ui_enabled=_env_bool("UI_ENABLED", False),
            nats_url=os.getenv("NATS_URL", "nats://127.0.0.1:4222"),
            worker_max_jobs=_env_int("WORKER_MAX_JOBS", 1),
            default_runner_cmd=os.getenv(
                "INSPECTION_RUNNER_CMD", "python -m inspection_lite.runner"
            ),
            scheduler_enabled=_env_bool("SCHEDULER_ENABLED", False),
            scheduler_timezone=os.getenv("SCHEDULER_TIMEZONE", "Asia/Tokyo"),
            scheduler_interval_sec=_env_int("SCHEDULER_INTERVAL_SEC", 10),
            heartbeat_interval_sec=_env_int("HEARTBEAT_INTERVAL_SEC", 10),
            heartbeat_api_host=os.getenv("HEARTBEAT_API_HOST", "0.0.0.0"),
            heartbeat_api_port=_env_int("HEARTBEAT_API_PORT", 5001),
            ui_host=os.getenv("UI_HOST", "0.0.0.0"),
            ui_port=_env_int("UI_PORT", 5000),
        )
