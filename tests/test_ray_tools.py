import asyncio

import pytest

from fastapi_client.app import ray_tools


class FakeAwaitableRef:
    def __init__(self, value):
        self.value = value

    def __await__(self):
        async def resolve():
            return self.value

        return resolve().__await__()


class FakeRemoteFunction:
    def __init__(self, handler):
        self.handler = handler

    def remote(self, *args, **kwargs):
        return FakeAwaitableRef(self.handler(*args, **kwargs))


class DelayedAwaitableRef:
    def __init__(self, value, delay):
        self.value = value
        self.delay = delay

    def __await__(self):
        async def resolve():
            await asyncio.sleep(self.delay)
            return self.value

        return resolve().__await__()


def test_normalize_ray_address_variants():
    assert ray_tools.normalize_ray_address(None) == "ray://ray-head:10001"
    assert ray_tools.normalize_ray_address("ray://custom:10001") == "ray://custom:10001"
    assert ray_tools.normalize_ray_address("ray-head") == "ray://ray-head:10001"
    assert ray_tools.normalize_ray_address("ray-head:10001") == "ray://ray-head:10001"


def test_current_ray_address_reads_env(monkeypatch):
    monkeypatch.setenv("RAY_ADDRESS", "ray-head")
    assert ray_tools.current_ray_address() == "ray://ray-head:10001"


def test_connect_to_ray_retries_until_success(monkeypatch):
    calls = {"init": 0, "shutdown": 0}

    async def fake_sleep(_):
        return None

    monkeypatch.setattr(ray_tools.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(ray_tools.ray, "is_initialized", lambda: True)
    monkeypatch.setattr(ray_tools.ray, "shutdown", lambda: calls.__setitem__("shutdown", calls["shutdown"] + 1))

    def fake_init(**kwargs):
        calls["init"] += 1
        if calls["init"] == 1:
            raise RuntimeError("temporary")

    monkeypatch.setattr(ray_tools.ray, "init", fake_init)

    asyncio.run(ray_tools.connect_to_ray("ray://cluster", max_attempts=2, delay_seconds=0))

    assert calls == {"init": 2, "shutdown": 2}


def test_connect_to_ray_raises_after_retries(monkeypatch):
    async def fake_sleep(_):
        return None

    monkeypatch.setattr(ray_tools.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(ray_tools.ray, "is_initialized", lambda: False)
    monkeypatch.setattr(ray_tools.ray, "init", lambda **kwargs: (_ for _ in ()).throw(RuntimeError("boom")))

    with pytest.raises(RuntimeError, match="Не удалось подключиться к Ray"):
        asyncio.run(ray_tools.connect_to_ray("ray://cluster", max_attempts=2, delay_seconds=0))


def test_fan_out_tracks_completion_and_submission_order():
    refs = [
        DelayedAwaitableRef({"value": 0}, 0.03),
        DelayedAwaitableRef({"value": 1}, 0.02),
        DelayedAwaitableRef({"value": 2}, 0.01),
    ]

    fanout_result = asyncio.run(ray_tools.fan_out(refs, timeout=1))

    assert isinstance(fanout_result, ray_tools.FanoutResult)
    assert fanout_result.completed_order == [2, 1, 0]
    assert fanout_result.completed_results == [{"value": 2}, {"value": 1}, {"value": 0}]
    assert fanout_result.ordered_results == [{"value": 0}, {"value": 1}, {"value": 2}]


def test_fan_out_raises_timeout_when_no_task_completed():
    refs = [DelayedAwaitableRef(1, 0.5)]

    with pytest.raises(TimeoutError, match="Истекло время ожидания при распределении задач"):
        asyncio.run(ray_tools.fan_out(refs, timeout=0.01))


def test_run_chain_passes_object_refs_between_steps():
    class RecordingRemoteFunction:
        def __init__(self, handler):
            self.handler = handler
            self.calls = []

        def remote(self, value):
            self.calls.append(value)
            return FakeAwaitableRef(self.handler(value))

    first_step = RecordingRemoteFunction(lambda value: value + 2)
    second_step = RecordingRemoteFunction(lambda ref: ref.value * 10)

    result = asyncio.run(ray_tools.run_chain(1, [first_step, second_step]))

    assert isinstance(second_step.calls[0], FakeAwaitableRef)
    assert result == 30
