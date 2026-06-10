from __future__ import annotations

import argparse
import asyncio
from collections import deque
from dataclasses import dataclass, replace
import os
from pathlib import Path
import shlex
import socket
import sys
import tempfile

from .logic import final_status_from_exit_code
from .models import (
    CancelRequest,
    Heartbeat,
    Job,
    JobState,
    STATUS_CANCEL_REQUESTED,
    STATUS_CANCELED,
    STATUS_QUEUED,
    STATUS_RUNNING,
    now_iso,
)
from .nats_store import InspectionNats
from .settings import Settings


@dataclass
class RunnerResult:
    exit_code: int
    stderr_tail: str = ""
    message: str | None = None


class Worker:
    def __init__(self, settings: Settings, once: bool = False):
        self.settings = settings
        self.once = once
        self.running_job_id: str | None = None
        self.worker_id = f"{settings.server_id}-{os.getpid()}"

    async def run(self) -> None:
        async with InspectionNats(self.settings) as store:
            while True:
                await self.write_heartbeat(store)
                if self.running_job_id is None:
                    handled = await self.try_run_one(store)
                    if self.once:
                        return
                    if not handled:
                        await asyncio.sleep(1)
                else:
                    await asyncio.sleep(1)

    async def write_heartbeat(self, store: InspectionNats) -> None:
        heartbeat = Heartbeat(
            server_id=self.settings.server_id,
            role=self.settings.role,
            hostname=socket.gethostname(),
            timestamp=now_iso(self.settings.scheduler_timezone),
            max_jobs=self.settings.worker_max_jobs,
            current_jobs=1 if self.running_job_id else 0,
            running_job_id=self.running_job_id,
        )
        await store.put_json(
            self.settings.heartbeat_bucket,
            self.settings.server_id,
            heartbeat.to_dict(),
        )

    async def try_run_one(self, store: InspectionNats) -> bool:
        fetched = await store.fetch_one_job(timeout=1.0)
        if fetched is None:
            return False

        message, job = fetched
        state_data = await store.get_json(self.settings.job_state_bucket, job.job_id)
        if state_data is None:
            await message.ack()
            return True

        state = JobState.from_dict(state_data)
        if state.status != STATUS_QUEUED or not state.visible:
            await message.ack()
            return True

        running = replace(
            state,
            status=STATUS_RUNNING,
            server_id=self.settings.server_id,
            worker_id=self.worker_id,
            started_at=now_iso(self.settings.scheduler_timezone),
            finished_at=None,
            exit_code=None,
            message=None,
        )
        await store.put_json(
            self.settings.job_state_bucket,
            job.job_id,
            running.to_dict(),
        )
        await message.ack()

        self.running_job_id = job.job_id
        try:
            await self.write_heartbeat(store)
            try:
                result = await self.run_process(store, job)
            except Exception as exc:
                message = f"worker failed to start runner: {exc!r}"
                print(f"job_id={job.job_id} {message}", file=sys.stderr, flush=True)
                result = RunnerResult(exit_code=1, message=message)

            final_status = final_status_from_exit_code(result.exit_code)
            message = runner_message(final_status, result)
            if message:
                print(
                    f"job_id={job.job_id} runner status={final_status}: {message}",
                    file=sys.stderr,
                    flush=True,
                )
            latest_data = await store.get_json(self.settings.job_state_bucket, job.job_id)
            latest = JobState.from_dict(latest_data or running.to_dict())
            finished = replace(
                latest,
                status=final_status,
                server_id=self.settings.server_id,
                worker_id=self.worker_id,
                finished_at=now_iso(self.settings.scheduler_timezone),
                exit_code=result.exit_code,
                message=message,
            )
            await store.put_json(
                self.settings.job_state_bucket,
                job.job_id,
                finished.to_dict(),
            )
        finally:
            self.running_job_id = None
            await self.write_heartbeat(store)
        return True

    async def run_process(self, store: InspectionNats, job: Job) -> RunnerResult:
        with tempfile.TemporaryDirectory(prefix="inspection-job-") as temp_dir:
            temp_path = Path(temp_dir)
            job_path = temp_path / f"{job.job_id}.json"
            cancel_path = temp_path / f"{job.job_id}.cancel"
            job_path.write_text(
                __import__("json").dumps(job.to_dict(), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

            command = split_command(job.runner_command)
            command.append(str(job_path))
            env = os.environ.copy()
            env["INSPECTION_CANCEL_FILE"] = str(cancel_path)
            env["INSPECTION_JOB_ID"] = job.job_id

            process = await asyncio.create_subprocess_exec(
                *command,
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stderr_tail: deque[str] = deque(maxlen=20)
            stdout_task = asyncio.create_task(
                forward_stream(process.stdout, sys.stdout, None)
            )
            stderr_task = asyncio.create_task(
                forward_stream(process.stderr, sys.stderr, stderr_tail)
            )
            cancel_notified = False
            last_heartbeat = 0.0
            while True:
                try:
                    exit_code = await asyncio.wait_for(process.wait(), timeout=1.0)
                    await asyncio.gather(stdout_task, stderr_task)
                    return RunnerResult(
                        exit_code=exit_code,
                        stderr_tail="".join(stderr_tail).strip(),
                    )
                except asyncio.TimeoutError:
                    loop_time = asyncio.get_running_loop().time()
                    if loop_time - last_heartbeat >= self.settings.heartbeat_interval_sec:
                        await self.write_heartbeat(store)
                        last_heartbeat = loop_time
                    state_data = await store.get_json(
                        self.settings.job_state_bucket, job.job_id
                    )
                    state = JobState.from_dict(state_data) if state_data else None
                    cancel_data = await store.get_json(
                        self.settings.cancel_bucket, job.job_id
                    )
                    if (
                        not cancel_notified
                        and (
                            cancel_data is not None
                            or (state and state.status == STATUS_CANCEL_REQUESTED)
                        )
                    ):
                        cancel_path.write_text("cancel_requested\n", encoding="utf-8")
                        cancel_notified = True


async def forward_stream(reader, writer, tail: deque[str] | None) -> None:
    while True:
        chunk = await reader.readline()
        if not chunk:
            return
        text = chunk.decode(errors="replace")
        writer.write(text)
        writer.flush()
        if tail is not None:
            tail.append(text)


def runner_message(final_status: str, result: RunnerResult) -> str | None:
    if result.message:
        return result.message
    if final_status == STATUS_RUNNING:
        return None
    if result.exit_code == 0:
        return None
    base = f"runner exited with code {result.exit_code}"
    if result.stderr_tail:
        return f"{base}; stderr tail: {result.stderr_tail}"
    return base


def split_command(command: str) -> list[str]:
    if not command.strip():
        raise ValueError("runner_command is required")
    return shlex.split(command, posix=os.name != "nt")


async def cancel_job(store: InspectionNats, settings: Settings, job_id: str) -> JobState | None:
    state_data = await store.get_json(settings.job_state_bucket, job_id)
    if state_data is None:
        return None
    state = JobState.from_dict(state_data)
    if state.status == STATUS_QUEUED:
        updated = replace(
            state,
            status=STATUS_CANCELED,
            visible=False,
            finished_at=now_iso(settings.scheduler_timezone),
            exit_code=130,
            message="canceled before start",
        )
        await store.put_json(settings.job_state_bucket, job_id, updated.to_dict())
        return updated
    if state.status == STATUS_RUNNING:
        updated = replace(
            state,
            status=STATUS_CANCEL_REQUESTED,
            message="cancel requested",
        )
        request = CancelRequest(
            job_id=job_id,
            requested_at=now_iso(settings.scheduler_timezone),
        )
        await store.put_json(settings.cancel_bucket, job_id, request.to_dict())
        await store.put_json(settings.job_state_bucket, job_id, updated.to_dict())
        return updated
    return state


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="process at most one message")
    args = parser.parse_args(argv)
    settings = Settings.from_env()
    try:
        asyncio.run(Worker(settings, once=args.once).run())
    except KeyboardInterrupt:
        sys.exit(130)


if __name__ == "__main__":
    main()
