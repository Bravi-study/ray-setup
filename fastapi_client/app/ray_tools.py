"""Переиспользуемые утилиты и паттерны интеграции приложений с Ray."""

import asyncio
import os
from dataclasses import dataclass
from typing import Any

import ray

DEFAULT_RAY_ADDRESS = "ray://ray-head:10001"
RAY_WAIT_TIMEOUT_SECONDS = 10


@dataclass(slots=True)
class FanoutResult:
    """Хранит результат fan-out/fan-in с порядком завершения и порядком отправки."""

    submitted_refs: list[Any]
    completed_order: list[int]
    completed_results: list[Any]
    ordered_results: list[Any]


def normalize_ray_address(raw_address: str | None) -> str:
    """Нормализует адрес в формат ray://host:port."""
    if not raw_address:
        return DEFAULT_RAY_ADDRESS
    if raw_address.startswith("ray://"):
        return raw_address
    if "://" in raw_address:
        return raw_address
    if ":" in raw_address:
        return f"ray://{raw_address}"

    return f"ray://{raw_address}:10001"


def current_ray_address() -> str:
    """Возвращает адрес Ray из окружения в нормализованном виде."""
    return normalize_ray_address(os.getenv("RAY_ADDRESS"))


async def connect_to_ray(address: str, max_attempts: int = 20, delay_seconds: float = 2) -> None:
    """Подключается к кластеру Ray с повторными попытками без блокировки event loop."""
    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            if ray.is_initialized():
                ray.shutdown()
            ray.init(address=address, ignore_reinit_error=True, logging_level="ERROR")

            return
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt == max_attempts:
                break

            await asyncio.sleep(delay_seconds)

    raise RuntimeError(f"Не удалось подключиться к Ray по адресу {address}: {last_error}")


async def fan_out(submitted_refs: list[Any], timeout: float = RAY_WAIT_TIMEOUT_SECONDS) -> FanoutResult:
    """Собирает завершившиеся Ray задачи по мере готовности через чистый asyncio."""
    pending_tasks = {asyncio.ensure_future(ref): (index, ref) for index, ref in enumerate(submitted_refs)}

    completed_order: list[int] = []
    completed_pairs: list[tuple[int, Any]] = []

    while pending_tasks:
        done_tasks, _ = await asyncio.wait(
            pending_tasks.keys(),
            timeout=timeout,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if not done_tasks:
            for task, (ready_index, original_ref) in pending_tasks.items():
                # Отменяем ожидание на стороне FastAPI
                task.cancel()

                # Убиваем задачу на воркере Ray физически (force=True убивает даже зависший процесс)
                try:
                    ray.cancel(original_ref, force=True)
                except Exception as e:
                    print(f"Warning: could not cancel task {ready_index}: {e}")

            await asyncio.gather(*pending_tasks, return_exceptions=True)
            raise TimeoutError("Истекло время ожидания при распределении задач.")

        for task in done_tasks:
            ready_index, _ = pending_tasks.pop(task)
            ready_result = await task
            completed_order.append(ready_index)
            completed_pairs.append((ready_index, ready_result))

    ordered_results = [result for _, result in sorted(completed_pairs, key=lambda item: item[0])]
    completed_results = [result for _, result in completed_pairs]

    return FanoutResult(
        submitted_refs=submitted_refs,
        completed_order=completed_order,
        completed_results=completed_results,
        ordered_results=ordered_results,
    )


async def run_chain(initial_value: Any, steps: list[Any]) -> Any:
    """Запускает цепь Ray задач, передавая ObjectRef между шагами напрямую."""
    current_value = initial_value
    for step in steps:
        # Ray разрешит ObjectRef на стороне следующего воркера без возврата данных в FastAPI.
        current_value = step.remote(current_value)

    return await current_value
