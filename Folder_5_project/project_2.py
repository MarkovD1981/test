Проект 2: API-коннектор для мониторинга цен конкурентов (строительные материалы)
Бизнес-проблема: Компания продаёт цемент, кирпич, гипсокартон и хочет ежедневно отслеживать цены конкурентов в открытых источниках (например, через парсинг сайтов или открытые API маркетплейсов). Ручной мониторинг невозможен.

Архитектура и стек:

Источник: Внешний API (имитация или реальный открытый API) + Python-скрипт парсинга.

ETL: Python (requests, async) → загрузка в PostgreSQL (сырой слой).

Трансформация: dbt (PostgreSQL) для построения модели mart_price_comparison (сравнение цен по SKU и конкурентам).   напиши код всех файлов этого проекта

Визуализация: Apache Superset (дашборд с графиками динамики цен, фильтрами по категориям).

Что демонстрирует: Работа с внешними API (rate limits, retry), использование dbt для трансформаций, построение витрины для конкурентного анализа, опыт с Superset (SQL-heavy аналитика) .

Структура репозитория:

text
/api-price-monitor-building-materials
├── scripts/fetch_prices.py
├── dbt_project/models/mart_price_comparison.sql
├── docker-compose.yml (Postgres + Superset + dbt)
└── README.md (пример API-запроса, схема витрины)
API-коннектор для мониторинга цен конкурентов (строительные материалы)
Ниже — полный набор файлов проекта. Структура:

text
api-price-monitor-building-materials/
├── scripts/
│   ├── fetch_prices.py
│   ├── mock_api.py
│   └── requirements.txt
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── sources.yml
│       ├── schema.yml
│       ├── staging/
│       │   └── stg_prices.sql
│       ├── intermediate/
│       │   └── int_prices_normalized.sql
│       └── mart_price_comparison.sql
├── db/
│   └── init.sql
├── superset/
│   └── superset_config.py
├── docker-compose.yml
├── .env.example
└── README.md
1. scripts/requirements.txt
txt
requests==2.32.3
aiohttp==3.9.5
asyncpg==0.29.0
psycopg2-binary==2.9.9
python-dotenv==1.0.1
tenacity==8.3.0
2. scripts/mock_api.py — имитация внешнего API (для локального запуска)
python
"""
Имитация внешнего API маркетплейса стройматериалов.
Запуск: uvicorn mock_api:app --host 0.0.0.0 --port 8080
(для standalone-теста можно python mock_api.py — на встроенном HTTP-сервере)
"""
import random
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
import json

COMPETITORS = ["stroy-baza", "cement-pro", "kirpich-market"]
CATEGORIES = {
    "cement": [("CEM-500-50", "Цемент М500 50кг"), ("CEM-400-50", "Цемент М400 50кг")],
    "brick": [("BRK-RED-250", "Кирпич красный 250мм"), ("BRK-WHITE-250", "Кирпич белый 250мм")],
    "drywall": [("GKL-2500-1200", "Гипсокартон 2500x1200"), ("GKL-VL-2500-1200", "Гипсокартон влагостойкий")],
}


def gen_prices(competitor: str, category: str | None = None):
    cats = [category] if category else list(CATEGORIES.keys())
    items = []
    ts = datetime.utcnow().isoformat() + "Z"
    for cat in cats:
        for sku, name in CATEGORIES[cat]:
            base = {"cement": 400, "brick": 25, "drywall": 650}[cat]
            price = round(base * random.uniform(0.9, 1.15), 2)
            items.append({
                "competitor": competitor,
                "sku": sku,
                "product_name": name,
                "category": cat,
                "price_rub": price,
                "currency": "RUB",
                "in_stock": random.choice([True, True, True, False]),
                "fetched_at": ts,
            })
    return items


class Handler(BaseHTTPRequestHandler):
    def _send(self, code: int, payload: dict):
        body = json.dumps(payload, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query)

        if parsed.path == "/api/v1/prices":
            competitor = qs.get("competitor", [None])[0]
            category = qs.get("category", [None])[0]
            if competitor and competitor not in COMPETITORS:
                self._send(404, {"error": "competitor not found"})
                return
            competitors = [competitor] if competitor else COMPETITORS
            data = []
            for c in competitors:
                data.extend(gen_prices(c, category))
            self._send(200, {"items": data, "next_cursor": None})
            return

        if parsed.path == "/api/v1/competitors":
            self._send(200, {"competitors": COMPETITORS, "categories": list(CATEGORIES)})
            return

        self._send(404, {"error": "not found"})

    def log_message(self, fmt, *args):
        pass


if __name__ == "__main__":
    print("Mock API on http://0.0.0.0:8080")
    HTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
3. scripts/fetch_prices.py — ETL-коннектор (async + retry + rate limit)
python
"""
ETL-загрузчик цен конкурентов в сырой слой PostgreSQL.

Особенности:
 - async (aiohttp) + семафор для ограничения конкурентности
 - retry с экспоненциальным бэкоффом (tenacity)
 - уважение rate limit через простой token bucket (asyncio)
 - идемпотентность: уникальный индекс в raw.prices (competitor, sku, fetched_at)
 - сохранение сырого JSON в raw.prices_payload для аудита

Запуск:
    python fetch_prices.py
    API_URL=http://mock_api:8080 python fetch_prices.py --once
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import aiohttp
import asyncpg
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
)
log = logging.getLogger("fetch_prices")

API_URL = os.getenv("API_URL", "http://localhost:8080")
PG_DSN = os.getenv(
    "PG_DSN",
    "postgresql://analytics:analytics@localhost:5432/analytics",
)
RATE_LIMIT_RPS = float(os.getenv("RATE_LIMIT_RPS", "5"))
MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", "10"))
TIMEOUT_S = float(os.getenv("TIMEOUT_S", "15"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))


class RateLimiter:
    """Простейший token bucket. Гарантирует не более rps запросов в секунду."""

    def __init__(self, rps: float):
        self.rps = rps
        self._interval = 1.0 / rps
        self._lock = asyncio.Lock()
        self._last = 0.0

    async def acquire(self) -> None:
        async with self._lock:
            now = asyncio.get_event_loop().time()
            wait = self._last + self._interval - now
            if wait > 0:
                await asyncio.sleep(wait)
                now = asyncio.get_event_loop().time()
            self._last = now


limiter = RateLimiter(RATE_LIMIT_RPS)
sem = asyncio.Semaphore(MAX_CONCURRENCY)


@retry(
    retry=retry_if_exception_type(
        (aiohttp.ClientError, asyncio.TimeoutError, aiohttp.ClientResponseError)
    ),
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=0.5, min=0.5, max=10),
    reraise=True,
)
async def fetch_json(session: aiohttp.ClientSession, url: str, params: dict | None = None) -> dict[str, Any]:
    async with sem:
        await limiter.acquire()
        async with session.get(url, params=params, timeout=TIMEOUT_S) as resp:
            if resp.status == 429:
                retry_after = float(resp.headers.get("Retry-After", "1"))
                log.warning("429 rate limited, sleeping %.1fs", retry_after)
                await asyncio.sleep(retry_after)
                raise aiohttp.ClientResponseError(
                    resp.request_info, resp.history, status=429
                )
            resp.raise_for_status()
            return await resp.json()


async def list_competitors(session: aiohttp.ClientSession) -> list[str]:
    data = await fetch_json(session, f"{API_URL}/api/v1/competitors")
    return data.get("competitors", [])


async def fetch_competitor_prices(
    session: aiohttp.ClientSession, competitor: str
) -> list[dict[str, Any]]:
    url = f"{API_URL}/api/v1/prices"
    items: list[dict[str, Any]] = []
    cursor: str | None = None
    while True:
        params = {"competitor": competitor}
        if cursor:
            params["cursor"] = cursor
        data = await fetch_json(session, url, params=params)
        items.extend(data.get("items", []))
        cursor = data.get("next_cursor")
        if not cursor:
            break
    log.info("competitor=%s fetched %d rows", competitor, len(items))
    return items


async def ensure_schema(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            CREATE SCHEMA IF NOT EXISTS raw;

            CREATE TABLE IF NOT EXISTS raw.prices (
                id            BIGSERIAL PRIMARY KEY,
                competitor    TEXT NOT NULL,
                sku           TEXT NOT NULL,
                product_name  TEXT NOT NULL,
                category      TEXT NOT NULL,
                price_rub     NUMERIC(12,2) NOT NULL,
                currency      TEXT NOT NULL,
                in_stock      BOOLEAN NOT NULL,
                fetched_at    TIMESTAMPTZ NOT NULL,
                ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
                UNIQUE (competitor, sku, fetched_at)
            );

            CREATE TABLE IF NOT EXISTS raw.prices_payload (
                id            BIGSERIAL PRIMARY KEY,
                competitor    TEXT NOT NULL,
                payload       JSONB NOT NULL,
                ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now()
            );

            CREATE INDEX IF NOT EXISTS idx_raw_prices_cat_ts
                ON raw.prices (category, fetched_at DESC);
            """
        )


async def load_rows(pool: asyncpg.Pool, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    records = [
        (
            r["competitor"],
            r["sku"],
            r["product_name"],
            r["category"],
            r["price_rub"],
            r["currency"],
            r["in_stock"],
            datetime.fromisoformat(r["fetched_at"].replace("Z", "+00:00"))
            if isinstance(r["fetched_at"], str)
            else r["fetched_at"],
        )
        for r in rows
    ]
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.executemany(
                """
                INSERT INTO raw.prices
                    (competitor, sku, product_name, category, price_rub, currency, in_stock, fetched_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                ON CONFLICT (competitor, sku, fetched_at) DO NOTHING
                """,
                records,
            )
            await conn.execute(
                "INSERT INTO raw.prices_payload (competitor, payload) VALUES ($1, $2::jsonb)",
                ",".join(sorted({r["competitor"] for r in rows})),
                json.dumps(rows, ensure_ascii=False, default=str),
            )
    return len(records)


async def run_once() -> None:
    started = datetime.now(timezone.utc)
    pool = await asyncpg.create_pool(PG_DSN, min_size=1, max_size=5)
    try:
        await ensure_schema(pool)
        async with aiohttp.ClientSession() as session:
            competitors = await list_competitors(session)
            log.info("competitors: %s", competitors)
            results = await asyncio.gather(
                *[fetch_competitor_prices(session, c) for c in competitors]
            )
        all_rows = [row for chunk in results for row in chunk]
        inserted = await load_rows(pool, all_rows)
        log.info(
            "done: fetched=%d inserted=%d elapsed=%.1fs",
            len(all_rows),
            inserted,
            (datetime.now(timezone.utc) - started).total_seconds(),
        )
    finally:
        await pool.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="run one iteration and exit")
    args = parser.parse_args()
    asyncio.run(run_once())


if __name__ == "__main__":
    main()
4. db/init.sql — инициализация БД
sql
-- Схемы
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS dbt;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Пользователи
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dbt') THEN
        CREATE ROLE dbt LOGIN PASSWORD 'dbt';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'superset') THEN
        CREATE ROLE superset LOGIN PASSWORD 'superset';
    END IF;
END $$;

GRANT USAGE, CREATE ON SCHEMA raw, dbt, analytics TO dbt;
GRANT USAGE ON SCHEMA analytics TO superset;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO superset;
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT SELECT ON TABLES TO superset;
5. dbt_project/dbt_project.yml
yaml
name: "price_monitor"
version: "1.0.0"
config-version: 2

profile: "price_monitor"

model-paths: ["models"]
macro-paths: ["macros"]

target-path: "target"
clean-targets: ["target", "dbt_packages"]

models:
  price_monitor:
    staging:
      +schema: staging
      +materialized: view
    intermediate:
      +schema: intermediate
      +materialized: view
    +materialized: table
6. dbt_project/profiles.yml
yaml
price_monitor:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "{{ env_var('DBT_HOST', 'postgres') }}"
      port: 5432
      user: "{{ env_var('DBT_USER', 'dbt') }}"
      password: "{{ env_var('DBT_PASSWORD', 'dbt') }}"
      dbname: "{{ env_var('DBT_DB', 'analytics') }}"
      schema: analytics
      threads: 4
      keepalives_idle: 0
7. dbt_project/models/sources.yml
yaml
version: 2

sources:
  - name: raw
    schema: raw
    tables:
      - name: prices
        description: "Сырой слой цен конкурентов, загружается scripts/fetch_prices.py"
        columns:
          - name: id
            tests: [unique, not_null]
          - name: competitor
            tests: [not_null]
          - name: sku
            tests: [not_null]
          - name: price_rub
            tests: [not_null]
          - name: fetched_at
            tests: [not_null]
8. dbt_project/models/staging/stg_prices.sql
sql
{{ config(materialized='view') }}

with src as (
    select * from {{ source('raw', 'prices') }}
),

dedup as (
    -- на случай дублей из-за ретраев: берём последнюю запись по (competitor, sku, fetched_at)
    select
        competitor,
        sku,
        product_name,
        category,
        price_rub::numeric(12,2)      as price_rub,
        currency,
        in_stock,
        fetched_at,
        ingested_at,
        row_number() over (
            partition by competitor, sku, fetched_at
            order by ingested_at desc
        ) as rn
    from src
)

select
    competitor,
    sku,
    product_name,
    category,
    price_rub,
    currency,
    in_stock,
    fetched_at,
    ingested_at
from dedup
where rn = 1
9. dbt_project/models/intermediate/int_prices_normalized.sql
sql
{{ config(materialized='view') }}

-- Нормализация: приведение к единой единице измерения (кг / шт),
-- флаг аномалий (слишком сильное отклонение от предыдущей цены).
with base as (
    select * from {{ ref('stg_prices') }}
),

with_prev as (
    select
        *,
        lag(price_rub) over (
            partition by competitor, sku
            order by fetched_at
        ) as prev_price_rub,
        lag(fetched_at) over (
            partition by competitor, sku
            order by fetched_at
        ) as prev_fetched_at
    from base
),

final as (
    select
        competitor,
        sku,
        product_name,
        category,
        price_rub,
        currency,
        in_stock,
        fetched_at,
        prev_price_rub,
        prev_fetched_at,
        case
            when prev_price_rub is null or prev_price_rub = 0 then null
            else round(((price_rub - prev_price_rub) / prev_price_rub * 100)::numeric, 2)
        end as price_change_pct,
        case
            when prev_price_rub is null or prev_price_rub = 0 then false
            when abs((price_rub - prev_price_rub) / prev_price_rub) > 0.30 then true
            else false
        end as is_price_anomaly
    from with_prev
)

select * from final
10. dbt_project/models/mart_price_comparison.sql
sql
{{ config(
    materialized='table',
    indexes=[
        {'columns': ['category', 'fetched_at']},
        {'columns': ['sku', 'fetched_at']},
        {'columns': ['competitor', 'fetched_at']},
    ]
) }}

/*
Витрина для Superset:
 - по каждому (sku, competitor, дата) — последняя цена дня
 - сравнение с медианой рынка по SKU и с минимальной ценой
 - позиция конкурента на рынке
 - дельта к вчерашней цене
*/
with daily as (
    select
        competitor,
        sku,
        max(product_name)                as product_name,
        max(category)                    as category,
        currency,
        date_trunc('day', fetched_at)::date as price_date,
        (array_agg(price_rub order by fetched_at desc))[1] as price_rub,
        (array_agg(in_stock  order by fetched_at desc))[1] as in_stock,
        (array_agg(price_change_pct order by fetched_at desc))[1] as price_change_pct,
        bool_or(is_price_anomaly)        as is_price_anomaly,
        max(fetched_at)                  as fetched_at
    from {{ ref('int_prices_normalized') }}
    group by competitor, sku, currency, date_trunc('day', fetched_at)::date
),

market as (
    select
        sku,
        price_date,
        percentile_cont(0.5) within group (order by price_rub) as median_price_rub,
        min(price_rub)                                        as min_price_rub,
        max(price_rub)                                        as max_price_rub,
        avg(price_rub)                                        as avg_price_rub,
        count(distinct competitor)                            as competitors_count
    from daily
    group by sku, price_date
),

with_yesterday as (
    select
        d.*,
        lag(d.price_rub) over (
            partition by d.competitor, d.sku
            order by d.price_date
        ) as price_yesterday_rub
    from daily d
)

select
    w.competitor,
    w.sku,
    w.product_name,
    w.category,
    w.currency,
    w.price_date,
    w.price_rub,
    w.price_yesterday_rub,
    case
        when w.price_yesterday_rub is null or w.price_yesterday_rub = 0 then null
        else round(((w.price_rub - w.price_yesterday_rub) / w.price_yesterday_rub * 100)::numeric, 2)
    end                                   as daily_change_pct,
    m.median_price_rub,
    m.min_price_rub,
    m.max_price_rub,
    m.avg_price_rub,
    m.competitors_count,
    round(
        ((w.price_rub - m.median_price_rub) / nullif(m.median_price_rub, 0) * 100)::numeric,
        2
    )                                     as vs_median_pct,
    case
        when w.price_rub = m.min_price_rub then 'lowest'
        when w.price_rub = m.max_price_rub then 'highest'
        else 'middle'
    end                                   as price_position,
    w.in_stock,
    w.is_price_anomaly,
    w.price_change_pct                    as intraday_change_pct,
    w.fetched_at
from with_yesterday w
join market m
  on m.sku = w.sku
 and m.price_date = w.price_date
11. dbt_project/models/schema.yml (тесты качества)
yaml
version: 2

models:
  - name: stg_prices
    description: "Очищенный staging цен конкурентов"
    columns:
      - name: sku
        tests: [not_null]
      - name: competitor
        tests: [not_null]
      - name: price_rub
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: "> 0"
      - name: category
        tests:
          - accepted_values:
              values: ['cement', 'brick', 'drywall']

  - name: mart_price_comparison
    description: "Витрина сравнения цен по SKU и конкурентам (для Superset)"
    columns:
      - name: sku
        tests: [not_null]
      - name: competitor
        tests: [not_null]
      - name: price_date
        tests: [not_null]
      - name: price_position
        tests:
          - accepted_values:
              values: ['lowest', 'middle', 'highest']
Тест dbt_utils.expression_is_true требует пакета dbt-labs/dbt_utils. Если не хочется ставить пакеты — замени на not_null + отдельный singular-тест dbt_project/tests/assert_price_positive.sql.

12. dbt_project/tests/assert_price_positive.sql
sql
-- singular-тест: вернёт строки, если есть невалидные цены
select *
from {{ ref('stg_prices') }}
where price_rub <= 0
13. docker-compose.yml
yaml
version: "3.9"

x-postgres-env: &pg-env
  POSTGRES_USER: analytics
  POSTGRES_PASSWORD: analytics
  POSTGRES_DB: analytics

services:
  postgres:
    image: postgres:15
    container_name: price_postgres
    environment: *pg-env
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
      - ./db/init.sql:/docker-entrypoint-initdb.d/init.sql:ro
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U analytics -d analytics"]
      interval: 5s
      timeout: 3s
      retries: 20

  mock_api:
    build:
      context: ./scripts
      dockerfile: Dockerfile.mock_api
    container_name: price_mock_api
    ports:
      - "8080:8080"
    depends_on:
      postgres:
        condition: service_healthy

  fetcher:
    build:
      context: ./scripts
      dockerfile: Dockerfile.fetcher
    container_name: price_fetcher
    environment:
      API_URL: http://mock_api:8080
      PG_DSN: postgresql://analytics:analytics@postgres:5432/analytics
      RATE_LIMIT_RPS: "10"
      MAX_CONCURRENCY: "10"
    depends_on:
      postgres:
        condition: service_healthy
      mock_api:
        condition: service_started
    restart: "no"

  dbt:
    image: ghcr.io/dbt-labs/dbt-postgres:1.7.8
    container_name: price_dbt
    working_dir: /usr/app/dbt_project
    volumes:
      - ./dbt_project:/usr/app/dbt_project
    environment:
      DBT_HOST: postgres
      DBT_USER: dbt
      DBT_PASSWORD: dbt
      DBT_DB: analytics
    command: >
      bash -c "dbt deps || true &&
               dbt run &&
               dbt test"
    depends_on:
      postgres:
        condition: service_healthy
      fetcher:
        condition: service_completed_successfully
    restart: "no"

  superset:
    image: apache/superset:3.1.2
    container_name: price_superset
    environment:
      SUPERSET_SECRET_KEY: "please-change-me-to-a-long-random-string"
      SUPERSET_CONFIG_PATH: /app/pythonpath/superset_config.py
    ports:
      - "8088:8088"
    volumes:
      - ./superset/superset_config.py:/app/pythonpath/superset_config.py:ro
    depends_on:
      postgres:
        condition: service_healthy
      dbt:
        condition: service_completed_successfully
    command: >
      bash -c "superset db upgrade &&
               superset fab create-admin
                 --username admin --firstname Admin --lastname User
                 --email admin@example.com --password admin || true &&
               superset init &&
               /usr/bin/run-server.sh"

volumes:
  pgdata:
14. scripts/Dockerfile.mock_api
dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY mock_api.py .
EXPOSE 8080
CMD ["python", "mock_api.py"]
15. scripts/Dockerfile.fetcher
dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY fetch_prices.py .
CMD ["python", "fetch_prices.py", "--once"]
16. superset/superset_config.py
python
import os

SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "dev-secret")
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"
WTF_CSRF_ENABLED = True
ENABLE_PROXY_FIX = True

FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_NATIVE_FILTERS": True,
}

# Подключение к Postgres для Superset meta (для prod заменить на отдельный PG)
PREVENT_UNSAFE_DB_CONNECTIONS = False
17. .env.example
dotenv
# API
API_URL=http://localhost:8080
RATE_LIMIT_RPS=10
MAX_CONCURRENCY=10

# Postgres
PG_DSN=postgresql://analytics:analytics@localhost:5432/analytics

# dbt
DBT_HOST=localhost
DBT_USER=dbt
DBT_PASSWORD=dbt
DBT_DB=analytics

# Superset
SUPERSET_SECRET_KEY=replace-me-with-64-char-random
18. README.md
markdown
# API Price Monitor — строительные материалы

Ежедневный мониторинг цен конкурентов (цемент, кирпич, гипсокартон) из открытого API
с загрузкой в PostgreSQL, трансформацией через dbt и визуализацией в Apache Superset.

## Архитектура

```
External API ──► fetch_prices.py (async, retry, rate-limit) ──► raw.prices (Postgres)
                                                                    │
                                                                    ▼
                                                     dbt: stg_prices ─► int_prices_normalized
                                                                          └► mart_price_comparison
                                                                                    │
                                                                                    ▼
                                                                              Superset dashboard
```

## Быстрый старт

```bash
cp .env.example .env
docker compose up -d postgres mock_api
docker compose run --rm fetcher
docker compose run --rm dbt
docker compose up -d superset
```

- Superset: http://localhost:8088 (admin / admin)
- Mock API: http://localhost:8080/api/v1/competitors
- Postgres: `localhost:5432`, БД `analytics`, user `analytics`

## Пример запроса к API

```bash
curl 'http://localhost:8080/api/v1/prices?competitor=cement-pro&category=cement'
```

Ответ:

```json
{
  "items": [
    {
      "competitor": "cement-pro",
      "sku": "CEM-500-50",
      "product_name": "Цемент М500 50кг",
      "category": "cement",
      "price_rub": 412.3,
      "currency": "RUB",
      "in_stock": true,
      "fetched_at": "2024-06-01T08:00:00Z"
    }
  ],
  "next_cursor": null
}
```

## Ручной запуск ETL (без Docker)

```bash
cd scripts
pip install -r requirements.txt
API_URL=http://localhost:8080 \
PG_DSN=postgresql://analytics:analytics@localhost:5432/analytics \
python fetch_prices.py --once
```

## Модели dbt

| Модель                  | Слой         | Описание                                                     |
|-------------------------|--------------|--------------------------------------------------------------|
| `stg_prices`            | staging      | дедупликация и типизация сырых данных                         |
| `int_prices_normalized` | intermediate | расчёт `price_change_pct`, флаг аномалий (>30% изменения)     |
| `mart_price_comparison` | mart         | последняя цена дня + `vs_median_pct`, `price_position`         |

### Схема витрины `analytics.mart_price_comparison`

| Колонка              | Тип            | Описание                                         |
|----------------------|----------------|--------------------------------------------------|
| competitor           | text           | идентификатор конкурента                         |
| sku                  | text           | артикул                                          |
| product_name         | text           | название                                         |
| category             | text           | `cement` / `brick` / `drywall`                   |
| currency             | text           | RUB                                              |
| price_date           | date           | дата снимка                                      |
| price_rub            | numeric(12,2)  | последняя цена дня                               |
| price_yesterday_rub  | numeric(12,2)  | цена предыдущего дня                             |
| daily_change_pct     | numeric        | % изменения день ко дню                          |
| median_price_rub     | numeric        | медиана по SKU среди конкурентов в этот день     |
| min_price_rub        | numeric        | минимум по рынку                                 |
| max_price_rub        | numeric        | максимум по рынку                                |
| avg_price_rub        | numeric        | среднее                                          |
| competitors_count    | int            | число конкурентов с этим SKU                     |
| vs_median_pct        | numeric        | отклонение от медианы рынка, %                   |
| price_position       | text           | `lowest` / `middle` / `highest`                  |
| in_stock             | boolean        | наличие                                          |
| is_price_anomaly     | boolean        | резкое изменение (>30%)                          |
| intraday_change_pct  | numeric        | изменение внутри дня                             |
| fetched_at           | timestamptz    | время последнего снимка                          |

## Дашборд Superset


1. **Connect Database** → Postgres: `postgresql://superset:superset@postgres:5432/analytics`.
2. Добавить датасет `analytics.mart_price_comparison`.
3. Чарты:
   - **Динамика цен**: line chart, X=`price_date`, Y=`avg(price_rub)`, series=`competitor`, фильтр `category`.
   - **Сравнение с медианой**: bar chart, X=`competitor`, Y=`avg(vs_median_pct)`, фильтр `sku`.
   - **Позиция на рынке**: pie chart по `price_position`.
   - **Аномалии**: таблица с фильтром `is_price_anomaly = true`.
4. Фильтры дашборда: `category`, `sku`, `competitor`, диапазон `price_date`.

## Надёжность

- **Retry** — `tenacity`, экспоненциальный бэкофф (0.5–10 с), до 5 попыток.
- **Rate limit** — клиентский token bucket (`RATE_LIMIT_RPS`) + обработка `429` c `Retry-After`.
- **Конкурентность** — `asyncio.Semaphore(MAX_CONCURRENCY)`.
- **Идемпотентность** — `UNIQUE (competitor, sku, fetched_at)` в `raw.prices`.
- **Аудит** — сырой payload пишется в `raw.prices_payload` (JSONB).
Как это запустить целиком
bash
docker compose up -d postgres mock_api
docker compose run --rm fetcher      # ETL
docker compose run --rm dbt          # dbt run + dbt test
docker compose up -d superset        # UI на :8088
Что демонстрирует проект
Работа с внешним API: async I/O, retry/backoff, обработка 429, rate limiting.

ETL: идемпотентная загрузка в raw-слой + JSONB-аудит, схема создаётся самим скриптом.

dbt: трёхслойная модель (staging → intermediate → mart), тесты, индексы на витрине.

Аналитика: витрина mart_price_comparison с медианой рынка, позицией конкурента, аномалиями и дневной дельтой — готова к подключению в Superset без доп. SQL.

Инфраструктура: `docker