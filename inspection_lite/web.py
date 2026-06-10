from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime
import html
import json
from typing import Any
from zoneinfo import ZoneInfo

from flask import Flask, jsonify, redirect, render_template_string, request, url_for

from .logic import compute_next_run_at, heartbeat_display_status
from .models import (
    Job,
    SOURCE_MANUAL,
    SCHEDULE_DAILY_TIME,
    SCHEDULE_INTERVAL,
    STATUS_CANCEL_REQUESTED,
    STATUS_CANCELED,
    STATUS_DONE,
    STATUS_FAILED,
    STATUS_QUEUED,
    STATUS_RUNNING,
    ScheduleTask,
    now_iso,
    parse_payload_json,
)
from .nats_store import InspectionNats
from .scheduler import enqueue_from_task
from .settings import Settings
from .worker import cancel_job


def run_async(coro):
    return asyncio.run(coro)


async def with_store(settings: Settings, func):
    async with InspectionNats(settings) as store:
        return await func(store)


def create_app(settings: Settings | None = None) -> Flask:
    settings = settings or Settings.from_env()
    app = Flask(__name__)

    @app.get("/")
    def index():
        status_filter = request.args.get("status", "")
        edit_task_id = request.args.get("edit_task")
        snapshot = run_async(load_snapshot(settings, status_filter, edit_task_id))
        return render_template_string(INDEX_TEMPLATE, **snapshot)

    @app.post("/api/jobs")
    def create_job():
        try:
            data = request_values()
            payload = parse_payload_value(data.get("payload"))
            job = Job.create(
                source=SOURCE_MANUAL,
                process=str(data["process"]).strip(),
                lot=str(data["lot"]).strip(),
                recipe=str(data["recipe"]).strip(),
                runner_command=str(data.get("runner_command", "")).strip()
                or settings.default_runner_cmd,
                payload=payload,
                tz_name=settings.scheduler_timezone,
            )
            run_async(with_store(settings, lambda store: store.create_job(job)))
        except Exception as exc:
            return error_response(exc)
        if request.is_json:
            return jsonify(job.to_dict()), 201
        return redirect(url_for("index"))

    @app.get("/api/jobs")
    def api_jobs():
        return jsonify(run_async(load_jobs(settings, include_hidden=True)))

    @app.post("/api/jobs/<job_id>/cancel")
    def api_cancel_job(job_id: str):
        async def _cancel(store):
            return await cancel_job(store, settings, job_id)

        state = run_async(with_store(settings, _cancel))
        if request.accept_mimetypes.best == "application/json":
            return jsonify(state.to_dict() if state else None)
        return redirect(url_for("index"))

    @app.post("/api/schedule-tasks")
    def api_create_task():
        try:
            task = build_task_from_form(settings)
            next_run = compute_next_run_at(task, tz_name=settings.scheduler_timezone)
            task = replace(task, next_run_at=next_run)
            run_async(
                with_store(
                    settings,
                    lambda store: store.put_json(
                        settings.schedule_bucket, task.task_id, task.to_dict()
                    ),
                )
            )
        except Exception as exc:
            return error_response(exc)
        if request.is_json:
            return jsonify(task.to_dict()), 201
        return redirect(url_for("index") + "#schedule")

    @app.post("/api/schedule-tasks/<task_id>/edit")
    def api_edit_task(task_id: str):
        try:
            async def _edit(store):
                data = await store.get_json(settings.schedule_bucket, task_id)
                if data is None:
                    raise ValueError(f"schedule task not found: {task_id}")
                existing = ScheduleTask.from_dict(data)
                task = build_task_from_form(settings, existing=existing)
                task = replace(
                    task,
                    next_run_at=compute_next_run_at(
                        task, tz_name=settings.scheduler_timezone
                    )
                    if task.enabled
                    else None,
                )
                await store.put_json(settings.schedule_bucket, task_id, task.to_dict())
                return task

            run_async(with_store(settings, _edit))
        except Exception as exc:
            return error_response(exc)
        if request.is_json:
            return jsonify({"task_id": task_id})
        return redirect(url_for("index") + "#schedule")

    @app.post("/api/schedule-tasks/<task_id>/delete")
    def api_delete_task(task_id: str):
        run_async(
            with_store(
                settings,
                lambda store: store.delete_key(settings.schedule_bucket, task_id),
            )
        )
        return redirect(url_for("index") + "#schedule")

    @app.post("/api/schedule-tasks/<task_id>/toggle")
    def api_toggle_task(task_id: str):
        async def _toggle(store):
            data = await store.get_json(settings.schedule_bucket, task_id)
            if data is None:
                return None
            task = ScheduleTask.from_dict(data)
            task = replace(
                task,
                enabled=not task.enabled,
                updated_at=now_iso(settings.scheduler_timezone),
            )
            task = replace(
                task,
                next_run_at=compute_next_run_at(task, tz_name=settings.scheduler_timezone)
                if task.enabled
                else None,
            )
            await store.put_json(settings.schedule_bucket, task_id, task.to_dict())
            return task

        run_async(with_store(settings, _toggle))
        return redirect(url_for("index") + "#schedule")

    @app.post("/api/schedule-tasks/<task_id>/run-now")
    def api_run_task_now(task_id: str):
        async def _run(store):
            data = await store.get_json(settings.schedule_bucket, task_id)
            if data is None:
                return None
            task = ScheduleTask.from_dict(data)
            return await enqueue_from_task(store, settings, task)

        run_async(with_store(settings, _run))
        return redirect(url_for("index") + "#schedule")

    return app


async def load_snapshot(
    settings: Settings, status_filter: str = "", edit_task_id: str | None = None
) -> dict[str, Any]:
    async with InspectionNats(settings) as store:
        jobs = await load_jobs_from_store(settings, store, include_hidden=False)
        tasks = await load_tasks_from_store(settings, store)
        heartbeats = await load_heartbeats_from_store(settings, store)
    if status_filter:
        jobs = [job for job in jobs if job.get("status") == status_filter]
    edit_task = None
    if edit_task_id:
        for task in tasks:
            if task.get("task_id") == edit_task_id:
                edit_task = task
                break
    return {
        "settings": settings,
        "jobs": jobs,
        "tasks": tasks,
        "heartbeats": heartbeats,
        "status_filter": status_filter,
        "status_options": [
            STATUS_QUEUED,
            STATUS_RUNNING,
            STATUS_DONE,
            STATUS_FAILED,
            STATUS_CANCEL_REQUESTED,
            STATUS_CANCELED,
        ],
        "edit_task": edit_task,
        "payload_example": html.escape(
            json.dumps(
                {
                    "input_path": "D:/inspection/input",
                    "output_path": "D:/inspection/output",
                },
                ensure_ascii=False,
                indent=2,
            )
        ),
    }


async def load_jobs(settings: Settings, include_hidden: bool = False) -> list[dict[str, Any]]:
    async with InspectionNats(settings) as store:
        return await load_jobs_from_store(settings, store, include_hidden=include_hidden)


async def load_jobs_from_store(
    settings: Settings, store: InspectionNats, include_hidden: bool = False
) -> list[dict[str, Any]]:
    rows = await store.list_json(settings.job_state_bucket)
    if not include_hidden:
        rows = [row for row in rows if row.get("visible", True)]
    rows.sort(key=lambda row: row.get("created_at", ""), reverse=True)
    return rows


async def load_tasks_from_store(settings: Settings, store: InspectionNats) -> list[dict[str, Any]]:
    rows = await store.list_json(settings.schedule_bucket)
    rows.sort(key=lambda row: row.get("name", ""))
    return rows


async def load_heartbeats_from_store(settings: Settings, store: InspectionNats) -> list[dict[str, Any]]:
    rows = await store.list_json(settings.heartbeat_bucket)
    now = datetime.now(ZoneInfo(settings.scheduler_timezone))
    for row in rows:
        row["status"] = heartbeat_display_status(
            row.get("timestamp", now_iso(settings.scheduler_timezone)),
            now,
            stale_after_sec=max(30, settings.heartbeat_interval_sec * 3),
        )
    rows.sort(key=lambda row: row.get("server_id", ""))
    return rows


def request_values() -> dict[str, Any]:
    if request.is_json:
        return request.get_json(silent=True) or {}
    return request.form.to_dict()


def parse_payload_value(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    return parse_payload_json(str(value))


def build_task_from_form(
    settings: Settings, existing: ScheduleTask | None = None
) -> ScheduleTask:
    data = request_values()
    schedule_type = str(data.get("schedule_type", SCHEDULE_DAILY_TIME))
    interval_seconds = None
    if schedule_type == SCHEDULE_INTERVAL:
        interval_seconds = int(data.get("interval_seconds") or "0")

    if existing is None:
        return ScheduleTask.create(
            name=str(data["name"]).strip(),
            enabled=parse_enabled(data.get("enabled")),
            schedule_type=schedule_type,
            time=str(data.get("time") or "") or None,
            interval_seconds=interval_seconds,
            runner_command=str(data.get("runner_command", "")).strip()
            or settings.default_runner_cmd,
            process=str(data.get("process", "")).strip() or "maintenance",
            lot=str(data.get("lot", "")).strip() or "scheduled",
            recipe=str(data.get("recipe", "")).strip() or "default",
            payload=parse_payload_value(data.get("payload")),
            tz_name=settings.scheduler_timezone,
        )

    return replace(
        existing,
        name=str(data["name"]).strip(),
        enabled=parse_enabled(data.get("enabled")),
        schedule_type=schedule_type,
        time=str(data.get("time") or "") or None,
        interval_seconds=interval_seconds,
        runner_command=str(data.get("runner_command", "")).strip()
        or settings.default_runner_cmd,
        process=str(data.get("process", "")).strip() or "maintenance",
        lot=str(data.get("lot", "")).strip() or "scheduled",
        recipe=str(data.get("recipe", "")).strip() or "default",
        payload=parse_payload_value(data.get("payload")),
        updated_at=now_iso(settings.scheduler_timezone),
    )


def parse_enabled(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on", "enabled"}


def error_response(exc: Exception):
    return (
        render_template_string(
            ERROR_TEMPLATE,
            message=html.escape(str(exc)),
        ),
        400,
    )


def main() -> None:
    settings = Settings.from_env()
    app = create_app(settings)
    app.run(host=settings.ui_host, port=settings.ui_port)


ERROR_TEMPLATE = """
<!doctype html>
<meta charset="utf-8">
<title>Inspection MVP Error</title>
<style>
body { font-family: system-ui, sans-serif; margin: 32px; color: #1f2937; }
.box { border: 1px solid #ef4444; padding: 16px; border-radius: 8px; max-width: 720px; }
a { color: #0f766e; }
</style>
<div class="box">
  <h1>入力エラー</h1>
  <p>{{ message }}</p>
  <a href="/">戻る</a>
</div>
"""


INDEX_TEMPLATE = """
<!doctype html>
<html lang="ja">
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>AI検査ジョブ</title>
<style>
:root {
  --bg: #f6f8fa;
  --panel: #ffffff;
  --ink: #172033;
  --muted: #667085;
  --line: #d9e0e8;
  --accent: #0f766e;
  --accent-strong: #0b5f59;
  --danger: #b42318;
  --soft: #eef6f5;
}
* { box-sizing: border-box; }
body {
  margin: 0;
  background: var(--bg);
  color: var(--ink);
  font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  line-height: 1.5;
}
header {
  background: #ffffff;
  border-bottom: 1px solid var(--line);
  padding: 18px 24px 12px;
  position: sticky;
  top: 0;
  z-index: 2;
}
h1 { font-size: 22px; margin: 0 0 12px; letter-spacing: 0; }
nav { display: flex; gap: 8px; flex-wrap: wrap; }
nav a {
  color: var(--ink);
  text-decoration: none;
  border: 1px solid var(--line);
  background: var(--soft);
  padding: 7px 12px;
  border-radius: 6px;
  font-size: 14px;
}
main { max-width: 1260px; margin: 0 auto; padding: 24px; }
section { margin-bottom: 32px; }
h2 { font-size: 18px; margin: 0 0 12px; letter-spacing: 0; }
form.grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(160px, 1fr));
  gap: 12px;
  align-items: end;
}
label { display: grid; gap: 5px; font-size: 13px; color: var(--muted); }
input, select, textarea {
  width: 100%;
  border: 1px solid var(--line);
  border-radius: 6px;
  padding: 8px 9px;
  background: #fff;
  color: var(--ink);
  font: inherit;
}
textarea { min-height: 92px; resize: vertical; grid-column: span 2; font-family: ui-monospace, SFMono-Regular, Consolas, monospace; font-size: 13px; }
button {
  border: 0;
  background: var(--accent);
  color: white;
  border-radius: 6px;
  padding: 9px 12px;
  font: inherit;
  cursor: pointer;
  min-height: 38px;
}
button:hover { background: var(--accent-strong); }
button.danger { background: var(--danger); }
button.muted { background: #475467; }
.table-wrap { overflow-x: auto; border: 1px solid var(--line); background: var(--panel); border-radius: 8px; }
table { width: 100%; border-collapse: collapse; min-width: 900px; }
th, td { border-bottom: 1px solid var(--line); padding: 9px 10px; text-align: left; vertical-align: top; font-size: 13px; }
th { background: #f0f4f8; color: #344054; font-weight: 650; }
tr:last-child td { border-bottom: 0; }
.status { font-weight: 650; color: var(--accent-strong); }
.status.failed, .status.cancel_requested { color: var(--danger); }
.inline { display: inline; }
.empty { color: var(--muted); padding: 14px 0; }
.wide { grid-column: span 2; }
@media (max-width: 920px) {
  main { padding: 16px; }
  form.grid { grid-template-columns: 1fr; }
  textarea, .wide { grid-column: span 1; }
}
</style>
<body>
  <header>
    <h1>AI検査ジョブ</h1>
    <nav>
      <a href="#submit">ジョブ投入</a>
      <a href="#jobs">ジョブ一覧</a>
      <a href="#schedule">定期実行タスク</a>
      <a href="#servers">サーバー状態</a>
    </nav>
  </header>
  <main>
    <section id="submit">
      <h2>ジョブ投入</h2>
      <form class="grid" method="post" action="/api/jobs">
        <label>工程<input name="process" required value="inspection-process-a"></label>
        <label>ロット<input name="lot" required value="LOT-001"></label>
        <label>レシピ<input name="recipe" required value="model-v1"></label>
        <label>runner command<input name="runner_command" required value="{{ settings.default_runner_cmd }}"></label>
        <label class="wide">payload JSON<textarea name="payload">{{ payload_example | safe }}</textarea></label>
        <button type="submit">登録</button>
      </form>
    </section>

    <section id="jobs">
      <h2>ジョブ一覧</h2>
      <form method="get" action="/" style="display:flex;gap:8px;align-items:end;margin-bottom:12px;flex-wrap:wrap;">
        <label style="min-width:180px;">状態フィルタ
          <select name="status">
            <option value="" {% if not status_filter %}selected{% endif %}>すべて</option>
            {% for status in status_options %}
              <option value="{{ status }}" {% if status_filter == status %}selected{% endif %}>{{ status }}</option>
            {% endfor %}
          </select>
        </label>
        <button type="submit">更新</button>
      </form>
      {% if jobs %}
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>ジョブID</th><th>工程</th><th>ロット</th><th>レシピ</th><th>状態</th><th>実行サーバー</th><th>登録時刻</th><th>開始時刻</th><th>終了時刻</th><th>終了コード</th><th>メッセージ</th><th>操作</th>
            </tr>
          </thead>
          <tbody>
          {% for job in jobs %}
            <tr>
              <td>{{ job.job_id }}</td>
              <td>{{ job.process or "" }}</td>
              <td>{{ job.lot or "" }}</td>
              <td>{{ job.recipe or "" }}</td>
              <td class="status {{ job.status }}">{{ job.status }}</td>
              <td>{{ job.server_id or "" }}</td>
              <td>{{ job.created_at or "" }}</td>
              <td>{{ job.started_at or "" }}</td>
              <td>{{ job.finished_at or "" }}</td>
              <td>{{ job.exit_code if job.exit_code is not none else "" }}</td>
              <td>{{ job.message or "" }}</td>
              <td>
                {% if job.status in ["queued", "running"] %}
                <form class="inline" method="post" action="/api/jobs/{{ job.job_id }}/cancel">
                  <button class="danger" type="submit">キャンセル</button>
                </form>
                {% endif %}
              </td>
            </tr>
          {% endfor %}
          </tbody>
        </table>
      </div>
      {% else %}
      <div class="empty">表示対象のジョブはありません。</div>
      {% endif %}
    </section>

    <section id="schedule">
      <h2>定期実行タスク</h2>
      <form class="grid" method="post" action="{% if edit_task %}/api/schedule-tasks/{{ edit_task.task_id }}/edit{% else %}/api/schedule-tasks{% endif %}">
        <label>タスク名<input name="name" required value="{{ edit_task.name if edit_task else 'daily-camera-check' }}"></label>
        <label>有効/無効
          <select name="enabled">
            <option value="1" {% if not edit_task or edit_task.enabled %}selected{% endif %}>有効</option>
            <option value="0" {% if edit_task and not edit_task.enabled %}selected{% endif %}>無効</option>
          </select>
        </label>
        <label>方式
          <select name="schedule_type">
            <option value="daily_time" {% if not edit_task or edit_task.schedule_type == 'daily_time' %}selected{% endif %}>毎日</option>
            <option value="interval" {% if edit_task and edit_task.schedule_type == 'interval' %}selected{% endif %}>間隔</option>
          </select>
        </label>
        <label>実行時刻<input name="time" value="{{ edit_task.time if edit_task and edit_task.time else '08:30' }}" pattern="[0-2][0-9]:[0-5][0-9]"></label>
        <label>実行間隔 秒<input name="interval_seconds" type="number" min="1" value="{{ edit_task.interval_seconds if edit_task and edit_task.interval_seconds else 3600 }}"></label>
        <label>runner command<input name="runner_command" required value="{{ edit_task.runner_command if edit_task else settings.default_runner_cmd }}"></label>
        <label>工程<input name="process" value="{{ edit_task.process if edit_task else 'maintenance' }}"></label>
        <label>ロット<input name="lot" value="{{ edit_task.lot if edit_task else 'daily' }}"></label>
        <label>レシピ<input name="recipe" value="{{ edit_task.recipe if edit_task else 'camera-check' }}"></label>
        <label class="wide">payload JSON<textarea name="payload">{{ edit_task.payload | tojson(indent=2) if edit_task else '{}' }}</textarea></label>
        <button type="submit">{{ "更新" if edit_task else "登録" }}</button>
        {% if edit_task %}<a href="/#schedule" style="align-self:center;color:var(--accent);">編集解除</a>{% endif %}
      </form>

      {% if tasks %}
      <div class="table-wrap" style="margin-top: 16px;">
        <table>
          <thead><tr><th>ID</th><th>名前</th><th>有効</th><th>方式</th><th>次回</th><th>前回投入</th><th>操作</th></tr></thead>
          <tbody>
          {% for task in tasks %}
            <tr>
              <td>{{ task.task_id }}</td>
              <td>{{ task.name }}</td>
              <td>{{ "有効" if task.enabled else "無効" }}</td>
              <td>{{ task.schedule_type }}</td>
              <td>{{ task.next_run_at or "" }}</td>
              <td>{{ task.last_enqueued_at or "" }}</td>
              <td>
                <a href="/?edit_task={{ task.task_id }}#schedule" style="display:inline-block;margin-right:6px;color:var(--accent);">編集</a>
                <form class="inline" method="post" action="/api/schedule-tasks/{{ task.task_id }}/toggle"><button class="muted" type="submit">切替</button></form>
                <form class="inline" method="post" action="/api/schedule-tasks/{{ task.task_id }}/run-now"><button type="submit">今すぐ実行</button></form>
                <form class="inline" method="post" action="/api/schedule-tasks/{{ task.task_id }}/delete"><button class="danger" type="submit">削除</button></form>
              </td>
            </tr>
          {% endfor %}
          </tbody>
        </table>
      </div>
      {% endif %}
    </section>

    <section id="servers">
      <h2>サーバー状態</h2>
      {% if heartbeats %}
      <div class="table-wrap">
        <table>
          <thead><tr><th>server_id</th><th>role</th><th>hostname</th><th>status</th><th>running_job_id</th><th>current_jobs</th><th>max_jobs</th><th>last heartbeat</th></tr></thead>
          <tbody>
          {% for hb in heartbeats %}
            <tr>
              <td>{{ hb.server_id }}</td><td>{{ hb.role }}</td><td>{{ hb.hostname }}</td><td class="status {{ hb.status }}">{{ hb.status }}</td>
              <td>{{ hb.running_job_id or "" }}</td><td>{{ hb.current_jobs }}</td><td>{{ hb.max_jobs }}</td><td>{{ hb.timestamp }}</td>
            </tr>
          {% endfor %}
          </tbody>
        </table>
      </div>
      {% else %}
      <div class="empty">Heartbeatはまだありません。</div>
      {% endif %}
    </section>
  </main>
</body>
</html>
"""


if __name__ == "__main__":
    main()
