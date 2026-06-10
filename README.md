# AI inspection job MVP

Flask UI/API, NATS JetStream work queue, NATS KV state, worker, scheduler, and a sample runner for the distributed AI inspection job MVP.

## Install

```powershell
python -m pip install -e .
```

If the virtual environment does not include `pip`, use `uv`:

```powershell
uv pip install -e .
```

Start or join a NATS cluster separately, for example with `kitfactory/nats-bootstrap`.

```powershell
nats-bootstrap up --cluster inspection-demo --datafolder C:\nats\data
```

## Run

UI node:

```powershell
$env:UI_ENABLED="1"
$env:SCHEDULER_ENABLED="1"
$env:INSPECTION_ROLE="ui"
inspection-ui
```

Worker:

```powershell
$env:INSPECTION_ROLE="worker"
inspection-worker
```

Scheduler:

```powershell
$env:SCHEDULER_ENABLED="1"
inspection-scheduler
```

Heartbeat API for worker-only nodes:

```powershell
inspection-heartbeat-api
```

Open the UI at `http://127.0.0.1:5000`.

The UI/API, worker, and scheduler expect `NATS_URL` to be reachable.

## Environment

| Name | Default | Purpose |
|---|---|---|
| `SERVER_ID` | hostname | Server identity stored in heartbeat and job state. |
| `INSPECTION_ROLE` | `worker` | `ui` or `worker`. |
| `NATS_URL` | `nats://127.0.0.1:4222` | NATS connection URL. |
| `WORKER_MAX_JOBS` | `1` | MVP assumes one active job per worker. |
| `INSPECTION_RUNNER_CMD` | `python -m inspection_lite.runner` | Default command used by jobs and schedule tasks. |
| `SCHEDULER_TIMEZONE` | `Asia/Tokyo` | Timezone used for job IDs and schedules. |
| `HEARTBEAT_INTERVAL_SEC` | `10` | Heartbeat freshness baseline. |
| `UI_HOST` / `UI_PORT` | `0.0.0.0` / `5000` | Flask UI bind address. |

## Notes

- The worker pulls at most one JetStream message only when it is idle.
- The worker checks KV state before execution. Hidden or canceled jobs are acknowledged and skipped.
- Runner processes receive the job JSON path as the last argument.
- Runner processes receive `INSPECTION_CANCEL_FILE`; cooperative cancellation should exit with code `130`.
- Logs are written to stdout/stderr.
- MVP recovery for stale `running` jobs is manual, matching the requirement.
