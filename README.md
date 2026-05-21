# ray-setup

Локальная sandbox-среда, которая эмулирует продакшн-развертывание Ray-кластера и микросервиса на FastAPI без KubeRay.

## Структура

```text
.
├── docker-compose.yml
├── fastapi_client/
│   ├── Dockerfile
│   ├── app/
│   │   ├── main.py
│   │   └── ray_tools.py
│   ├── pyproject.toml
│   └── uv.lock
├── ray_cluster/
│   ├── Dockerfile
│   ├── pyproject.toml
│   └── uv.lock
├── main.py
├── pyproject.toml
└── uv.lock
```

## Что здесь поднимается

- `ray-head` — head-узел Ray с Dashboard и Ray Client API.
- `ray-worker` — worker-узлы Ray на том же базовом образе, что и head.
- `fastapi-app` — API-сервис, который отправляет тяжелые задачи в Ray и ждет результат.

Все Docker-образы собираются только через `uv`, а зависимости фиксируются в отдельных `uv.lock` для каждого сервиса.

## Локальный запуск

1. Убедитесь, что установлен Docker с поддержкой `docker compose`.
2. При необходимости пересоберите lock-файлы командой `uv lock` внутри каталогов `ray_cluster` и `fastapi_client`.
3. Поднимите окружение одной командой:

```bash
docker compose up --build --scale ray-worker=2
```

`deploy.replicas: 2` оставлен в `docker-compose.yml` как аналог production-спеки, но для локального `docker compose` надежнее явно задавать `--scale ray-worker=2`.

## Полезные адреса

- Ray Dashboard: `http://localhost:8265`
- FastAPI docs: `http://localhost:8000/docs`
- FastAPI healthcheck: `http://localhost:8000/health`

## Демонстрационные ручки

- `POST /predict` и `POST /predict_async` — прямой `await` на `Ray ObjectRef` в async-ручке FastAPI.
- `POST /predict_sync` — настоящая sync-ручка FastAPI с прямым `ray.get(...)` для демонстрации синхронного паттерна.
- `POST /predict_group` — независимые row-level задачи, результаты которых собираются через обычный `asyncio.gather(*refs)`.
- `POST /predict_fanout` — fan-out по строкам матрицы через `asyncio.wait(..., return_when=FIRST_COMPLETED)`, чтобы показать порядок завершения задач.
- `POST /predict_chain` — последовательный pipeline из зависимых стадий: batch-агрегация, scoring и routing.
- `POST /predict_chord` — scatter/gather паттерн: callback получает список `ObjectRef` напрямую, а Ray сам резолвит зависимости на worker-е.

Переиспользуемые Ray-хелперы вынесены в `fastapi_client/app/ray_tools.py`, чтобы пример можно было проще переносить в другие сервисы.
Там оставлены только Ray-native паттерны: нормализация адреса, асинхронное подключение, `fan_out` и `run_chain`. Zero-copy chord показан прямо на месте вызова через `await callback.remote(refs)`.

В API-ответах больше не дублируется `ray_address`: технический адрес кластера остается только в `GET /health`, а бизнес-ответы содержат только результат вычислений.

Такой набор ручек оставлен намеренно: проект служит эталонным примером разных паттернов интеграции FastAPI и Ray.

## Когда это реально применять

- `group` — когда нужно распараллелить независимые куски одной партии данных: shard-by-shard feature engineering, расчеты по клиентам, батчевые enrichment-job'ы.
- `fan_out` — когда важен не только итог, но и порядок фактического завершения задач: progressive delivery результатов, early partial responses, наблюдаемость медленных shard'ов.
- `chain` — когда каждый следующий шаг зависит от результата предыдущего: scoring после feature aggregation, нормализация после инференса, routing после оценки batch.
- `chord` — когда сначала нужны независимые параллельные вычисления, а потом единый reducer: map/reduce агрегации, ансамбли моделей, объединение shard-level метрик. В Ray reducer лучше кормить списком `ObjectRef`, а не скачанными на клиент данными.

Thread-bridge через `await asyncio.to_thread(ray.get, ...)` из demo-ручек убран: при подключении через Ray Client `ObjectRef` можно `await`-ить напрямую, а лишнее размножение потоков под высокой нагрузкой здесь не нужно.

## Пример запроса

```bash
curl --request POST \
	--url http://localhost:8000/predict \
	--header 'Content-Type: application/json' \
	--data '{
		"rows": [
			[1, 2, 3],
			[4, 5, 6],
			[7, 8, 9]
		]
	}'
```

Сервис подключается к Ray при старте. Если в окружении передан `RAY_ADDRESS=ray-head`, приложение автоматически преобразует его в адрес Ray Client `ray://ray-head:10001`.

## Тесты

Тесты запускаются стандартной командой из корня репозитория:

```bash
uv run pytest
```

В тестах Ray замокан, поэтому для них не требуется живой кластер или поднятый `docker compose`.
