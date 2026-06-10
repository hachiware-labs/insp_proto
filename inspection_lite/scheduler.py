from __future__ import annotations

import argparse
import asyncio
from dataclasses import replace
from datetime import datetime
import sys
from zoneinfo import ZoneInfo

from .logic import compute_next_run_at, is_task_due
from .models import Job, SOURCE_SCHEDULE, ScheduleTask, now_iso
from .nats_store import InspectionNats
from .settings import Settings


async def enqueue_from_task(
    store: InspectionNats, settings: Settings, task: ScheduleTask
) -> Job:
    job = Job.create(
        source=SOURCE_SCHEDULE,
        process=task.process or "scheduled",
        lot=task.lot or task.name,
        recipe=task.recipe or task.name,
        runner_command=task.runner_command or settings.default_runner_cmd,
        payload={
            **task.payload,
            "schedule_task_id": task.task_id,
            "schedule_task_name": task.name,
        },
        tz_name=settings.scheduler_timezone,
    )
    await store.create_job(job)
    return job


async def run_due_tasks(store: InspectionNats, settings: Settings) -> int:
    now = datetime.now(ZoneInfo(settings.scheduler_timezone))
    count = 0
    rows = await store.list_json(settings.schedule_bucket)
    for row in rows:
        task = ScheduleTask.from_dict(row)
        if task.enabled and task.next_run_at is None:
            task = replace(
                task,
                next_run_at=compute_next_run_at(task, now, settings.scheduler_timezone),
                updated_at=now_iso(settings.scheduler_timezone),
            )
            await store.put_json(settings.schedule_bucket, task.task_id, task.to_dict())

        if not is_task_due(task, now, settings.scheduler_timezone):
            continue

        await enqueue_from_task(store, settings, task)
        updated = replace(
            task,
            last_enqueued_at=now_iso(settings.scheduler_timezone),
            next_run_at=compute_next_run_at(
                replace(task, last_enqueued_at=now_iso(settings.scheduler_timezone)),
                now,
                settings.scheduler_timezone,
            ),
            updated_at=now_iso(settings.scheduler_timezone),
        )
        await store.put_json(settings.schedule_bucket, updated.task_id, updated.to_dict())
        count += 1
    return count


async def scheduler_loop(settings: Settings, once: bool = False) -> None:
    async with InspectionNats(settings) as store:
        while True:
            await run_due_tasks(store, settings)
            if once:
                return
            await asyncio.sleep(settings.scheduler_interval_sec)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="run one scheduler tick")
    args = parser.parse_args(argv)
    settings = Settings.from_env()
    try:
        asyncio.run(scheduler_loop(settings, once=args.once))
    except KeyboardInterrupt:
        sys.exit(130)


if __name__ == "__main__":
    main()
