import asyncio
import time
from contextlib import asynccontextmanager
from typing import Any

import pandas as pd
import ray
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from fastapi_client.app import ray_tools

type Matrix = list[list[float]]


class PredictRequest(BaseModel):
    # Ожидаем простую числовую матрицу, чтобы удобно агрегировать данные через pandas.
    rows: Matrix = Field(min_length=1, description="Матрица признаков")


class PredictResponse(BaseModel):
    rows_processed: int
    columns_processed: int
    total_sum: float
    column_means: list[float]
    row_sums: list[float]
    max_value: float
    min_value: float


class FanoutRowResult(BaseModel):
    row_index: int
    values_count: int
    row_sum: float
    row_mean: float
    row_max: float


class PredictFanoutResponse(BaseModel):
    tasks_submitted: int
    completed_order: list[int]
    row_results: list[FanoutRowResult]
    total_sum: float


class PredictGroupResponse(BaseModel):
    tasks_submitted: int
    row_results: list[FanoutRowResult]
    total_sum: float


class PredictChainResponse(BaseModel):
    rows_processed: int
    total_sum: float
    max_value: float
    risk_score: float
    route: str
    pipeline_steps: list[str]


class PredictChordResponse(BaseModel):
    tasks_submitted: int
    rows_processed: int
    total_sum: float
    max_row_sum: float
    mean_row_sum: float


def validate_matrix_rows(rows: Matrix) -> int:
    """Проверяет, что на вход пришла непустая прямоугольная матрица."""
    expected_row_length = len(rows[0])
    if expected_row_length == 0:
        raise HTTPException(status_code=400, detail="Матрица должна содержать хотя бы один столбец")
    if any(len(row) != expected_row_length for row in rows):
        raise HTTPException(status_code=400, detail="Все строки должны иметь одинаковую длину")

    return expected_row_length


@ray.remote
def heavy_aggregation(rows: Matrix) -> dict[str, Any]:
    """Запускает тяжелую pandas-агрегацию на worker-узле Ray."""
    # Remote-функция сериализуется и исполняется на worker-процессе, а не в FastAPI.
    time.sleep(2)
    dataframe = pd.DataFrame(rows)

    return {
        "rows_processed": int(dataframe.shape[0]),
        "columns_processed": int(dataframe.shape[1]),
        "total_sum": float(dataframe.to_numpy().sum()),
        "column_means": [float(value) for value in dataframe.mean(axis=0).tolist()],
        "row_sums": [float(value) for value in dataframe.sum(axis=1).tolist()],
        "max_value": float(dataframe.max().max()),
        "min_value": float(dataframe.min().min()),
    }


@ray.remote
def aggregate_row(row: list[float], row_index: int) -> dict[str, Any]:
    """Агрегирует одну строку матрицы для fan-out сценариев."""
    time.sleep(1)

    return {
        "row_index": row_index,
        "values_count": len(row),
        "row_sum": float(sum(row)),
        "row_mean": float(sum(row) / len(row)),
        "row_max": float(max(row)),
    }


@ray.remote
def score_batch(summary: dict[str, Any]) -> dict[str, Any]:
    """Добавляет batch сводке производный risk score для последующей маршрутизации."""
    enriched_summary = dict(summary)
    enriched_summary["risk_score"] = float(
        enriched_summary["max_value"] * enriched_summary["columns_processed"]
        + enriched_summary["total_sum"] / max(enriched_summary["rows_processed"], 1)
    )
    enriched_summary["pipeline_steps"] = ["aggregate", "score"]

    return enriched_summary


@ray.remote
def route_batch(summary: dict[str, Any]) -> dict[str, Any]:
    """Маршрутизирует batch в стандартную или приоритетную обработку."""
    routed_summary = dict(summary)
    routed_summary["route"] = "priority" if routed_summary["risk_score"] >= 25 else "standard"
    routed_summary["pipeline_steps"] = [*routed_summary["pipeline_steps"], "route"]

    return routed_summary


@ray.remote
def summarize_group(row_results: list[Any]) -> dict[str, Any]:
    """Сводит независимые row-level результаты в итоговую batch-агрегацию."""
    total_sum = float(sum(row_result["row_sum"] for row_result in row_results))
    max_row_sum = float(max((row_result["row_sum"] for row_result in row_results), default=0.0))
    mean_row_sum = float(total_sum / len(row_results)) if row_results else 0.0

    return {
        "rows_processed": len(row_results),
        "total_sum": total_sum,
        "max_row_sum": max_row_sum,
        "mean_row_sum": mean_row_sum,
    }


@asynccontextmanager
async def lifespan(_: FastAPI):
    """Подключает приложение к Ray при старте и освобождает клиент при выходе."""
    await ray_tools.connect_to_ray(ray_tools.current_ray_address())
    try:
        yield
    finally:
        if ray.is_initialized():
            ray.shutdown()


app = FastAPI(
    title="FastAPI Ray Client",
    description="Микросервис делегирует тяжелые вычисления в локально эмулируемый Ray-кластер.",
    lifespan=lifespan,
)


@app.get("/health")
async def healthcheck() -> dict[str, str]:
    """Возвращает статус приложения и адрес текущего Ray-кластера."""
    return {
        "status": "ok",
        "ray_address": ray_tools.current_ray_address(),
    }


@app.post("/predict", response_model=PredictResponse)
@app.post("/predict_async", response_model=PredictResponse)
async def predict_async(payload: PredictRequest) -> PredictResponse:
    """Ждет результат Ray напрямую через awaitable ClientObjectRef."""
    validate_matrix_rows(payload.rows)

    # Современный Ray Client позволяет await-ить ObjectRef прямо в async-ручке.
    result = await heavy_aggregation.remote(payload.rows)

    return PredictResponse(**result)


@app.post("/predict_sync", response_model=PredictResponse)
def predict_sync(payload: PredictRequest) -> PredictResponse:
    """Синхронный эндпоинт, блокирующий на ray.get() для демонстрации sync-паттерна."""
    validate_matrix_rows(payload.rows)

    # FastAPI исполняет sync-обработчики в threadpool, поэтому ray.get() не блокирует event loop.
    object_ref = heavy_aggregation.remote(payload.rows)
    result = ray.get(object_ref)

    return PredictResponse(**result)


@app.post("/predict_group", response_model=PredictGroupResponse)
async def predict_group(payload: PredictRequest) -> PredictGroupResponse:
    """Показывает независимый group-паттерн для параллельной обработки строк."""
    validate_matrix_rows(payload.rows)

    row_refs = [aggregate_row.remote(row, row_index) for row_index, row in enumerate(payload.rows)]
    row_results = await asyncio.gather(*row_refs)
    ordered_row_results = [FanoutRowResult(**row_result) for row_result in row_results]

    return PredictGroupResponse(
        tasks_submitted=len(row_refs),
        row_results=ordered_row_results,
        total_sum=float(sum(row_result.row_sum for row_result in ordered_row_results)),
    )


@app.post("/predict_fanout", response_model=PredictFanoutResponse)
async def predict_fanout(payload: PredictRequest) -> PredictFanoutResponse:
    """Отправляет строки в Ray по отдельности и показывает fan-out/fan-in паттерн."""
    validate_matrix_rows(payload.rows)

    row_refs: list[ray.ObjectRef] = [aggregate_row.remote(row, row_index) for row_index, row in enumerate(payload.rows)]

    # FanoutResult одновременно показывает порядок завершения задач и детерминированный итоговый порядок.
    try:
        fanout_result = await ray_tools.fan_out(row_refs)
    except TimeoutError as exc:
        raise HTTPException(status_code=504, detail=str(exc)) from exc

    ordered_row_results = [FanoutRowResult(**row_result) for row_result in fanout_result.ordered_results]

    return PredictFanoutResponse(
        tasks_submitted=len(fanout_result.submitted_refs),
        completed_order=fanout_result.completed_order,
        row_results=ordered_row_results,
        total_sum=float(sum(row_result.row_sum for row_result in ordered_row_results)),
    )


@app.post("/predict_chain", response_model=PredictChainResponse)
async def predict_chain(payload: PredictRequest) -> PredictChainResponse:
    """Показывает цепочку зависимых стадий обработки одного батча."""
    validate_matrix_rows(payload.rows)

    chain_result = await ray_tools.run_chain(
        payload.rows,
        [
            heavy_aggregation,
            score_batch,
            route_batch,
        ],
    )

    return PredictChainResponse(
        rows_processed=chain_result["rows_processed"],
        total_sum=chain_result["total_sum"],
        max_value=chain_result["max_value"],
        risk_score=chain_result["risk_score"],
        route=chain_result["route"],
        pipeline_steps=chain_result["pipeline_steps"],
    )


@app.post("/predict_chord", response_model=PredictChordResponse)
async def predict_chord(payload: PredictRequest) -> PredictChordResponse:
    """Показывает chord: сначала независимые задачи, затем общий reducer callback."""
    validate_matrix_rows(payload.rows)

    row_refs = [aggregate_row.remote(row, row_index) for row_index, row in enumerate(payload.rows)]

    # Zero-copy для data plane: callback получает ObjectRef и Ray сам разруливает зависимости на worker-е.
    chord_result = await summarize_group.remote(row_refs)

    return PredictChordResponse(
        tasks_submitted=len(row_refs),
        rows_processed=chord_result["rows_processed"],
        total_sum=chord_result["total_sum"],
        max_row_sum=chord_result["max_row_sum"],
        mean_row_sum=chord_result["mean_row_sum"],
    )
