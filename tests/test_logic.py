from __future__ import annotations

from dataclasses import replace
from datetime import datetime
import unittest
from zoneinfo import ZoneInfo

from inspection_lite.logic import (
    compute_next_run_at,
    final_status_from_exit_code,
    is_task_due,
)
from inspection_lite.models import (
    Job,
    JobState,
    SCHEDULE_DAILY_TIME,
    SCHEDULE_INTERVAL,
    SOURCE_MANUAL,
    STATUS_CANCELED,
    STATUS_DONE,
    STATUS_FAILED,
    ScheduleTask,
    parse_payload_json,
)
from inspection_lite.worker import RunnerResult, runner_message


def task(**overrides):
    base = ScheduleTask(
        task_id="task-1",
        name="check",
        enabled=True,
        schedule_type=SCHEDULE_DAILY_TIME,
        time="08:30",
        interval_seconds=None,
        runner_command="python -m inspection_lite.runner",
        process="maintenance",
        lot="daily",
        recipe="camera-check",
        payload={},
        last_enqueued_at=None,
        next_run_at=None,
        created_at="2026-06-10T10:00:00+09:00",
        updated_at="2026-06-10T10:00:00+09:00",
    )
    return replace(base, **overrides)


class LogicTests(unittest.TestCase):
    def test_parse_payload_json_requires_object(self):
        self.assertEqual(parse_payload_json(""), {})
        self.assertEqual(parse_payload_json('{"a": 1}'), {"a": 1})
        with self.assertRaises(ValueError):
            parse_payload_json("[1, 2]")

    def test_daily_time_next_run_moves_to_tomorrow_after_time(self):
        now = datetime(2026, 6, 10, 10, 0, tzinfo=ZoneInfo("Asia/Tokyo"))
        self.assertEqual(
            compute_next_run_at(task(), now, "Asia/Tokyo"),
            "2026-06-11T08:30:00+09:00",
        )

    def test_interval_next_run_uses_last_enqueue(self):
        now = datetime(2026, 6, 10, 10, 0, tzinfo=ZoneInfo("Asia/Tokyo"))
        scheduled = task(
            schedule_type=SCHEDULE_INTERVAL,
            time=None,
            interval_seconds=3600,
            last_enqueued_at="2026-06-10T08:30:00+09:00",
        )
        self.assertEqual(
            compute_next_run_at(scheduled, now, "Asia/Tokyo"),
            "2026-06-10T10:30:00+09:00",
        )

    def test_due_task(self):
        now = datetime(2026, 6, 10, 10, 0, tzinfo=ZoneInfo("Asia/Tokyo"))
        self.assertTrue(
            is_task_due(task(next_run_at="2026-06-10T09:59:59+09:00"), now, "Asia/Tokyo")
        )
        self.assertFalse(
            is_task_due(task(next_run_at="2026-06-10T10:00:01+09:00"), now, "Asia/Tokyo")
        )

    def test_final_status_from_exit_code(self):
        self.assertEqual(final_status_from_exit_code(0), STATUS_DONE)
        self.assertEqual(final_status_from_exit_code(130), STATUS_CANCELED)
        self.assertEqual(final_status_from_exit_code(1), STATUS_FAILED)

    def test_job_state_keeps_list_metadata(self):
        job = Job(
            job_id="job-1",
            source=SOURCE_MANUAL,
            process="inspection-process-a",
            lot="LOT-001",
            recipe="model-v1",
            runner_command="python -m inspection_lite.runner",
            payload={},
            created_at="2026-06-10T10:00:00+09:00",
        )
        state = JobState.queued(job)
        self.assertEqual(state.process, "inspection-process-a")
        self.assertEqual(state.lot, "LOT-001")
        self.assertEqual(state.recipe, "model-v1")

    def test_runner_failure_message_includes_stderr_tail(self):
        message = runner_message(
            STATUS_FAILED,
            RunnerResult(exit_code=1, stderr_tail="RuntimeError: boom"),
        )
        self.assertEqual(
            message,
            "runner exited with code 1; stderr tail: RuntimeError: boom",
        )


if __name__ == "__main__":
    unittest.main()
