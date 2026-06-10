from __future__ import annotations

import json
from typing import Any

import nats
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.api import AckPolicy, ConsumerConfig, RetentionPolicy, StorageType, StreamConfig
from nats.js.errors import BadRequestError, NotFoundError

from .models import Job, JobState
from .settings import Settings


class InspectionNats:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.nc = None
        self.js = None
        self._kv = {}

    async def connect(self) -> None:
        self.nc = await nats.connect(self.settings.nats_url)
        self.js = self.nc.jetstream()

    async def close(self) -> None:
        if self.nc:
            await self.nc.drain()

    async def __aenter__(self) -> "InspectionNats":
        await self.connect()
        await self.ensure()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def ensure(self) -> None:
        await self._ensure_stream()
        for bucket in [
            self.settings.job_state_bucket,
            self.settings.cancel_bucket,
            self.settings.heartbeat_bucket,
            self.settings.schedule_bucket,
        ]:
            await self._ensure_bucket(bucket)
        await self._ensure_consumer()

    async def _ensure_stream(self) -> None:
        try:
            await self.js.stream_info(self.settings.stream_name)
            return
        except NotFoundError:
            pass
        config = StreamConfig(
            name=self.settings.stream_name,
            subjects=[self.settings.job_subject],
            retention=RetentionPolicy.WORK_QUEUE,
            storage=StorageType.FILE,
        )
        await self.js.add_stream(config=config)

    async def _ensure_consumer(self) -> None:
        try:
            await self.js.consumer_info(
                self.settings.stream_name, self.settings.consumer_name
            )
            return
        except NotFoundError:
            pass
        config = ConsumerConfig(
            durable_name=self.settings.consumer_name,
            ack_policy=AckPolicy.EXPLICIT,
        )
        try:
            await self.js.add_consumer(self.settings.stream_name, config=config)
        except BadRequestError:
            pass

    async def _ensure_bucket(self, bucket: str):
        try:
            kv = await self.js.key_value(bucket)
        except NotFoundError:
            kv = await self.js.create_key_value(bucket=bucket)
        self._kv[bucket] = kv
        return kv

    async def kv(self, bucket: str):
        if bucket not in self._kv:
            return await self._ensure_bucket(bucket)
        return self._kv[bucket]

    async def put_json(self, bucket: str, key: str, value: dict[str, Any]) -> None:
        kv = await self.kv(bucket)
        payload = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()
        await kv.put(key, payload)

    async def get_json(self, bucket: str, key: str) -> dict[str, Any] | None:
        kv = await self.kv(bucket)
        try:
            entry = await kv.get(key)
        except Exception as exc:
            if exc.__class__.__name__ in {"KeyNotFoundError", "NotFoundError"}:
                return None
            raise
        return json.loads(entry.value.decode())

    async def list_json(self, bucket: str) -> list[dict[str, Any]]:
        kv = await self.kv(bucket)
        try:
            keys = await kv.keys()
        except Exception as exc:
            if exc.__class__.__name__ in {"NoKeysError", "NotFoundError"}:
                return []
            raise
        rows = []
        for key in keys:
            item = await self.get_json(bucket, key)
            if item is not None:
                rows.append(item)
        return rows

    async def delete_key(self, bucket: str, key: str) -> None:
        kv = await self.kv(bucket)
        try:
            await kv.delete(key)
        except Exception as exc:
            if exc.__class__.__name__ in {"KeyNotFoundError", "NotFoundError"}:
                return
            raise

    async def create_job(self, job: Job) -> None:
        state = JobState.queued(job)
        await self.put_json(self.settings.job_state_bucket, job.job_id, state.to_dict())
        await self.publish_job(job)

    async def publish_job(self, job: Job) -> None:
        payload = json.dumps(job.to_dict(), ensure_ascii=False).encode()
        await self.js.publish(self.settings.job_subject, payload)

    async def fetch_one_job(self, timeout: float = 1.0):
        sub = await self.js.pull_subscribe(
            self.settings.job_subject,
            durable=self.settings.consumer_name,
            stream=self.settings.stream_name,
        )
        try:
            messages = await sub.fetch(1, timeout=timeout)
        except NatsTimeoutError:
            return None
        if not messages:
            return None
        message = messages[0]
        return message, Job.from_dict(json.loads(message.data.decode()))
