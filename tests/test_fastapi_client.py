from fastapi.testclient import TestClient

from fastapi_client.app import main as app_module


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


def aggregation_result(rows):
    column_means = [float(sum(column) / len(column)) for column in zip(*rows)]
    return {
        "rows_processed": len(rows),
        "columns_processed": len(rows[0]),
        "total_sum": float(sum(sum(row) for row in rows)),
        "column_means": column_means,
        "row_sums": [float(sum(row)) for row in rows],
        "max_value": float(max(max(row) for row in rows)),
        "min_value": float(min(min(row) for row in rows)),
    }


def build_client(monkeypatch) -> TestClient:
    async def fake_connect_to_ray(address):
        return None

    monkeypatch.setattr(app_module.ray_tools, "connect_to_ray", fake_connect_to_ray)
    monkeypatch.setattr(app_module.ray, "shutdown", lambda: None)
    return TestClient(app_module.app)


def test_predict_async_returns_aggregation(monkeypatch):
    monkeypatch.setattr(
        app_module,
        "heavy_aggregation",
        FakeRemoteFunction(lambda rows: aggregation_result(rows)),
    )

    with build_client(monkeypatch) as client:
        response = client.post("/predict_async", json={"rows": [[1, 2], [3, 4]]})

    assert response.status_code == 200
    assert response.json() == {
        "rows_processed": 2,
        "columns_processed": 2,
        "total_sum": 10.0,
        "column_means": [2.0, 3.0],
        "row_sums": [3.0, 7.0],
        "max_value": 4.0,
        "min_value": 1.0,
    }


def test_predict_sync_uses_direct_ray_get(monkeypatch):
    called = {"threaded": 0}

    monkeypatch.setattr(
        app_module,
        "heavy_aggregation",
        FakeRemoteFunction(lambda rows: aggregation_result(rows)),
    )

    def fake_ray_get(ref):
        called["threaded"] += 1
        return ref.value

    monkeypatch.setattr(app_module.ray, "get", fake_ray_get)

    with build_client(monkeypatch) as client:
        response = client.post("/predict_sync", json={"rows": [[1, 2, 3], [4, 5, 6]]})

    assert response.status_code == 200
    assert response.json()["total_sum"] == 21.0
    assert called["threaded"] == 1


def test_predict_group_returns_parallel_row_results(monkeypatch):
    monkeypatch.setattr(
        app_module,
        "aggregate_row",
        FakeRemoteFunction(
            lambda row, row_index: {
                "row_index": row_index,
                "values_count": len(row),
                "row_sum": float(sum(row)),
                "row_mean": float(sum(row) / len(row)),
                "row_max": float(max(row)),
            }
        ),
    )

    with build_client(monkeypatch) as client:
        response = client.post("/predict_group", json={"rows": [[1, 2], [3, 4], [5, 6]]})

    assert response.status_code == 200
    assert response.json() == {
        "tasks_submitted": 3,
        "row_results": [
            {"row_index": 0, "values_count": 2, "row_sum": 3.0, "row_mean": 1.5, "row_max": 2.0},
            {"row_index": 1, "values_count": 2, "row_sum": 7.0, "row_mean": 3.5, "row_max": 4.0},
            {"row_index": 2, "values_count": 2, "row_sum": 11.0, "row_mean": 5.5, "row_max": 6.0},
        ],
        "total_sum": 21.0,
    }


def test_predict_fanout_tracks_completion_order(monkeypatch):
    monkeypatch.setattr(
        app_module,
        "aggregate_row",
        FakeRemoteFunction(lambda row, row_index: {"row_index": row_index}),
    )
    monkeypatch.setattr(
        app_module.ray_tools,
        "fan_out",
        lambda refs, timeout=app_module.ray_tools.RAY_WAIT_TIMEOUT_SECONDS: FakeAwaitableRef(
            app_module.ray_tools.FanoutResult(
                submitted_refs=[object(), object(), object()],
                completed_order=[2, 1, 0],
                completed_results=[
                    {"row_index": 2, "values_count": 2, "row_sum": 11.0, "row_mean": 5.5, "row_max": 6.0},
                    {"row_index": 1, "values_count": 2, "row_sum": 7.0, "row_mean": 3.5, "row_max": 4.0},
                    {"row_index": 0, "values_count": 2, "row_sum": 3.0, "row_mean": 1.5, "row_max": 2.0},
                ],
                ordered_results=[
                    {"row_index": 0, "values_count": 2, "row_sum": 3.0, "row_mean": 1.5, "row_max": 2.0},
                    {"row_index": 1, "values_count": 2, "row_sum": 7.0, "row_mean": 3.5, "row_max": 4.0},
                    {"row_index": 2, "values_count": 2, "row_sum": 11.0, "row_mean": 5.5, "row_max": 6.0},
                ],
            )
        ),
    )

    with build_client(monkeypatch) as client:
        response = client.post("/predict_fanout", json={"rows": [[1, 2], [3, 4], [5, 6]]})

    assert response.status_code == 200
    assert response.json() == {
        "tasks_submitted": 3,
        "completed_order": [2, 1, 0],
        "row_results": [
            {"row_index": 0, "values_count": 2, "row_sum": 3.0, "row_mean": 1.5, "row_max": 2.0},
            {"row_index": 1, "values_count": 2, "row_sum": 7.0, "row_mean": 3.5, "row_max": 4.0},
            {"row_index": 2, "values_count": 2, "row_sum": 11.0, "row_mean": 5.5, "row_max": 6.0},
        ],
        "total_sum": 21.0,
    }


def test_predict_chain_returns_pipeline_summary(monkeypatch):
    async def fake_run_chain(initial_value, steps):
        assert initial_value == [[1, 2], [3, 4]]
        assert len(steps) == 3
        assert steps == [app_module.heavy_aggregation, app_module.score_batch, app_module.route_batch]
        return {
            "rows_processed": 2,
            "total_sum": 10.0,
            "max_value": 4.0,
            "risk_score": 13.0,
            "route": "standard",
            "pipeline_steps": ["aggregate", "score", "route"],
        }

    monkeypatch.setattr(app_module.ray_tools, "run_chain", fake_run_chain)

    with build_client(monkeypatch) as client:
        response = client.post("/predict_chain", json={"rows": [[1, 2], [3, 4]]})

    assert response.status_code == 200
    assert response.json() == {
        "rows_processed": 2,
        "total_sum": 10.0,
        "max_value": 4.0,
        "risk_score": 13.0,
        "route": "standard",
        "pipeline_steps": ["aggregate", "score", "route"],
    }


def test_predict_chord_returns_reduced_batch_summary(monkeypatch):
    monkeypatch.setattr(
        app_module,
        "aggregate_row",
        FakeRemoteFunction(lambda row, row_index: {"row_index": row_index}),
    )
    monkeypatch.setattr(
        app_module,
        "summarize_group",
        FakeRemoteFunction(
            lambda refs: {
                "rows_processed": len(refs),
                "total_sum": 21.0,
                "max_row_sum": 11.0,
                "mean_row_sum": 7.0,
            }
        ),
    )

    with build_client(monkeypatch) as client:
        response = client.post("/predict_chord", json={"rows": [[1, 2], [3, 4], [5, 6]]})

    assert response.status_code == 200
    assert response.json() == {
        "tasks_submitted": 3,
        "rows_processed": 3,
        "total_sum": 21.0,
        "max_row_sum": 11.0,
        "mean_row_sum": 7.0,
    }


def test_predict_async_rejects_ragged_matrix(monkeypatch):
    monkeypatch.setattr(app_module, "heavy_aggregation", FakeRemoteFunction(lambda rows: {}))

    with build_client(monkeypatch) as client:
        response = client.post("/predict_async", json={"rows": [[1, 2], [3]]})

    assert response.status_code == 400
    assert response.json() == {"detail": "Все строки должны иметь одинаковую длину"}


def test_predict_async_rejects_empty_columns(monkeypatch):
    monkeypatch.setattr(app_module, "heavy_aggregation", FakeRemoteFunction(lambda rows: {}))

    with build_client(monkeypatch) as client:
        response = client.post("/predict_async", json={"rows": [[]]})

    assert response.status_code == 400
    assert response.json() == {"detail": "Матрица должна содержать хотя бы один столбец"}
