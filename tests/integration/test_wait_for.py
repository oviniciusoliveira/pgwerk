"""Integration tests for wait_for functionality."""

from __future__ import annotations

import asyncio

import pytest

from pgwerk.commons import JobStatus

from .tasks import noop
from .tasks import fail_always
from .tasks import clear_callback_log
from .conftest import make_worker


@pytest.fixture(autouse=True)
def _clear_cbs():
    clear_callback_log()
    yield
    clear_callback_log()


class TestWaitFor:
    async def test_wait_for_returns_when_complete(self, app):
        job = await app.enqueue(noop)
        worker = make_worker(app, burst=True, poll_interval=0.05)
        worker_task = asyncio.create_task(worker.run())
        completed = await app.wait_for(job.id, poll_interval=0.1, timeout=3.0)
        await worker_task
        assert completed.status == JobStatus.Complete

    async def test_wait_for_already_complete(self, app):
        job = await app.enqueue(noop)
        await make_worker(app).run()
        result = await app.wait_for(job.id)
        assert result.status == JobStatus.Complete

    async def test_wait_for_times_out(self, app):
        # Don't run a worker — job stays queued
        job = await app.enqueue(noop)
        with pytest.raises(asyncio.TimeoutError):
            await app.wait_for(job.id, timeout=0.1, poll_interval=0.05)

    async def test_wait_for_failed_job_returns_failed(self, app):
        job = await app.enqueue(fail_always)
        worker = make_worker(app, burst=True, poll_interval=0.05)
        worker_task = asyncio.create_task(worker.run())
        result = await app.wait_for(job.id, poll_interval=0.1, timeout=3.0)
        await worker_task
        assert result.status == JobStatus.Failed

    async def test_wait_for_without_listen_uses_polling(self, app, monkeypatch):
        app.config.listen = False
        job = await app.enqueue(noop)

        listen_statements: list[str] = []
        original_connect = app._connect

        async def _spy_connect():
            conn = await original_connect()
            original_execute = conn.execute

            async def _execute(query, *args, **kwargs):
                text = query.as_string(conn) if hasattr(query, "as_string") else str(query)
                if text.lstrip().upper().startswith("LISTEN"):
                    listen_statements.append(text)
                return await original_execute(query, *args, **kwargs)

            conn.execute = _execute
            return conn

        monkeypatch.setattr(app, "_connect", _spy_connect)
        worker = make_worker(app, burst=True, poll_interval=0.05, listen=False)
        worker_task = asyncio.create_task(worker.run())
        result = await app.wait_for(job.id, poll_interval=0.05, timeout=3.0)
        await worker_task
        assert result.status == JobStatus.Complete
        assert listen_statements == []
