FROM python:3.12-slim AS uv-installer

# В отдельной стадии ставим uv официальным install-скриптом и копируем только бинарники.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && curl -LsSf https://astral.sh/uv/install.sh -o /tmp/install-uv.sh \
    && env UV_INSTALL_DIR=/uv-bin sh /tmp/install-uv.sh \
    && rm /tmp/install-uv.sh

FROM python:3.12-slim

COPY --from=uv-installer /uv-bin/uv /uv-bin/uvx /bin/

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    PATH="/app/.venv/bin:${PATH}"

WORKDIR /app

# Сначала копируем только файлы зависимостей, чтобы максимально использовать Docker cache.
COPY pyproject.toml uv.lock ./

RUN uv sync --frozen --no-install-project \
    && useradd --create-home --shell /bin/bash rayuser \
    && chown -R rayuser:rayuser /app

USER rayuser

# Порты Ray GCS, Dashboard и Ray Client.
EXPOSE 6379 8265 10001
