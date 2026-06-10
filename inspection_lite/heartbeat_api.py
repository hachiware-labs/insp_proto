from __future__ import annotations

import asyncio
import socket

from flask import Flask, jsonify

from .models import Heartbeat, now_iso
from .nats_store import InspectionNats
from .settings import Settings


def create_app(settings: Settings | None = None) -> Flask:
    settings = settings or Settings.from_env()
    app = Flask(__name__)

    @app.get("/healthz")
    def healthz():
        return jsonify({"status": "ok", "server_id": settings.server_id})

    @app.post("/heartbeat")
    def heartbeat():
        heartbeat = Heartbeat(
            server_id=settings.server_id,
            role=settings.role,
            hostname=socket.gethostname(),
            timestamp=now_iso(settings.scheduler_timezone),
            max_jobs=settings.worker_max_jobs,
            current_jobs=0,
            running_job_id=None,
        )

        async def _write():
            async with InspectionNats(settings) as store:
                await store.put_json(
                    settings.heartbeat_bucket,
                    settings.server_id,
                    heartbeat.to_dict(),
                )

        asyncio.run(_write())
        return jsonify(heartbeat.to_dict())

    return app


def main() -> None:
    settings = Settings.from_env()
    app = create_app(settings)
    app.run(host=settings.heartbeat_api_host, port=settings.heartbeat_api_port)


if __name__ == "__main__":
    main()
