5555555555555555555555555
4444444444444444444444
Проект 5: Оркестрированный пайплайн «Каталог товаров + Продажи» с двумя витринами
Бизнес-проблема: Компания хочет объединить каталог товаров (SKU, категории, единицы измерения — из JSON/API) и исторические продажи (из PostgreSQL) в ClickHouse, чтобы аналитики могли строить отчёты по оборачиваемости и ABC-анализу без нагрузки на операционную БД.


Архитектура и стек:

Источники: JSON-файл (каталог) + PostgreSQL (продажи).

Оркестрация: Apache Airflow (DAG с двумя ветками: load_catalog, load_sales).

Хранилище: ClickHouse (две сырые таблицы + две витрины: mart_product_turnover, mart_abc_analysis).

Визуализация: Metabase (два дашборда: «Оборачиваемость» и «ABC-анализ»).

Что демонстрирует: Комплексный ETL с несколькими источниками, оркестрация зависимых задач в Airflow, построение нескольких витрин в ClickHouse, навык подготовки данных для конкретных бизнес-методик (ABC-анализ), что особенно ценно для ритейла/дистрибуции стройматериалов .

Структура репозитория:



text
/product-sales-marts-airflow
├── dags/catalog_sales_pipeline.py
├── sql/clickhouse_marts.sql
├── data/catalog_sample.json
├── docker-compose.yml (Airflow + Postgres + ClickHouse + Metabase)
└── README.md (DAG-граф, описание витрин, пример ABC-отчёта)
Общие рекомендации для GitHub:



Добавьте в каждый репозиторий docker-compose.yml для локального запуска (Airflow, Postgres, ClickHouse, BI) — это сильно повышает ценность портфолио .

В README включите схему архитектуры (можно сгенерировать в draw.io или Mermaid), скриншоты дашбордов и примеры SQL-запросов к витринам.

Для визуализации используйте open-source решения: Metabase (простой старт), Superset (SQL-heavy), Grafana (real-time), Lightdash (dbt-native) — выбор инструмента под задачу покажет зрелость мышления .

     напиши код всех файлов этого проекта
Проект: product-sales-marts-airflow
Ниже — полный набор файлов для запуска проекта. Стек: Airflow 2.9 + PostgreSQL (как источник продаж + метаданные Airflow) + ClickHouse + Metabase.

📁 docker-compose.yml
yaml
version: "3.8"

x-airflow-common: &airflow-common
  image: apache/airflow:2.9.2-python3.11
  environment: &airflow-common-env
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@airflow-postgres:5432/airflow
    AIRFLOW__CORE__FERNET_KEY: ""
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: "true"
    AIRFLOW__CORE__LOAD_EXAMPLES: "false"
    AIRFLOW__API__AUTH_BACKENDS: "airflow.api.auth.backend.basic_auth"
    AIRFLOW__WEBSERVER__EXPOSE_CONFIG: "true"
    _PIP_ADDITIONAL_REQUIREMENTS: >-
      apache-airflow-providers-postgres==5.11.1
      apache-airflow-providers-clickhouse==0.0.1
      clickhouse-driver==0.2.7
      clickhouse-connect==0.7.16
      psycopg2-binary==2.9.9
  volumes:
    - ./dags:/opt/airflow/dags
    - ./sql:/opt/airflow/sql
    - ./data:/opt/airflow/data
    - ./logs:/opt/airflow/logs
    - airflow-plugins:/opt/airflow/plugins
  user: "${AIRFLOW_UID:-50000}:0"
  depends_on:
    airflow-postgres:
      condition: service_healthy

services:
  # ---------- Airflow metadata DB ----------



  airflow-postgres:
    image: postgres:15
    container_name: airflow-postgres
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 5s
      retries: 5
    volumes:
      - airflow-db-data:/var/lib/postgresql/data

  airflow-init:
    <<: *airflow-common
    container_name: airflow-init
    entrypoint: /bin/bash
    command:
      - -c
      - |
        airflow db migrate
        airflow users create \
          --username admin --firstname Admin --lastname User \
          --role Admin --email admin@example.com --password admin
        echo "Airflow init done"

  airflow-webserver:
    <<: *airflow-common
    container_name: airflow-webserver
    command: webserver
    ports:
      - "8080:8080"
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
      interval: 30s
      retries: 5
    depends_on:
      airflow-init:
        condition: service_completed_successfully

  airflow-scheduler:
    <<: *airflow-common
    container_name: airflow-scheduler
    command: scheduler
    depends_on:
      airflow-init:
        condition: service_completed_successfully

  # ---------- Source: PostgreSQL (продажи) ----------
  sales-postgres:
    image: postgres:15
    container_name: sales-postgres
    environment:
      POSTGRES_USER: sales
      POSTGRES_PASSWORD: sales
      POSTGRES_DB: sales_db
    ports:
      - "5433:5432"
    volumes:
      - sales-db-data:/var/lib/postgresql/data
      - ./data/init_sales.sql:/docker-entrypoint-initdb.d/init_sales.sql:ro

  # ---------- ClickHouse ----------
  clickhouse:
    image: clickhouse/clickhouse-server:24.3
    container_name: clickhouse
    ulimits:
      nofile:
        soft: 262144
        hard: 262144
    ports:
      - "8123:8123"   # HTTP
      - "9000:9000"   # Native
    environment:
      CLICKHOUSE_DB: analytics
      CLICKHOUSE_USER: default
      CLICKHOUSE_PASSWORD: ""
      CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT: "1"
    volumes:
      - clickhouse-data:/var/lib/clickhouse
      - ./sql/clickhouse_init.sql:/docker-entrypoint-initdb.d/init.sql:ro

  # ---------- Metabase ----------
  metabase:
    image: metabase/metabase:v0.50.21
    container_name: metabase
    ports:
      - "3000:3000"
    environment:
      MB_DB_TYPE: h2
    volumes:
      - metabase-data:/metabase-data
    depends_on:
      - clickhouse

volumes:
  airflow-db-data:
  airflow-plugins:
  sales-db-data:
  clickhouse-data:
  metabase-data:
⚠️ Провайдер apache-airflow-providers-clickhouse может отсутствовать в PyPI — если возникнут проблемы, уберите его из _PIP_ADDITIONAL_REQUIREMENTS. DAG использует нативный clickhouse-driver, так что провайдер не обязателен.

📁 data/catalog_sample.json
json
[
  {"sku": "CEM-M500-50", "name": "Цемент М500, 50 кг", "category": "Вяжущие", "uom": "мешок", "price": 420.00},
  {"sku": "CEM-M400-50", "name": "Цемент М400, 50 кг", "category": "Вяжущие", "uom": "мешок", "price": 350.00},
  {"sku": "SAND-25", "name": "Песок строительный, 25 кг", "category": "Сыпучие", "uom": "мешок", "price": 95.00},
  {"sku": "GRAVEL-25", "name": "Щебень фр. 5-20, 25 кг", "category": "Сыпучие", "uom": "мешок", "price": 120.00},
  {"sku": "BRICK-RED", "name": "Кирпич керамический полнотелый", "category": "Кладочные", "uom": "шт", "price": 28.50},
  {"sku": "BRICK-GAS", "name": "Блок газобетонный D500", "category": "Кладочные", "uom": "шт", "price": 210.00},
  {"sku": "PLASTER-GYPS", "name": "Штукатурка гипсовая, 30 кг", "category": "Отделочные", "uom": "мешок", "price": 380.00},
  {"sku": "PUTTY-FIN", "name": "Шпатлёвка финишная, 25 кг", "category": "Отделочные", "uom": "мешок", "price": 640.00},
  {"sku": "PRIMER-DEEP", "name": "Грунтовка глубокого проникновения, 10 л", "category": "Отделочные", "uom": "канистра", "price": 520.00},
  {"sku": "TILE-ADH", "name": "Клей для плитки, 25 кг", "category": "Отделочные", "uom": "мешок", "price": 310.00},
  {"sku": "INSUL-EPPS-50", "name": "Пеноплэкс 50 мм", "category": "Теплоизоляция", "uom": "лист", "price": 480.00},
  {"sku": "INSUL-MIN-50", "name": "Минвата 50 мм", "category": "Теплоизоляция", "uom": "упаковка", "price": 720.00},
  {"sku": "ROOF-METAL", "name": "Металлочерепица, м²", "category": "Кровля", "uom": "м2", "price": 560.00},
  {"sku": "ROOF-BITUM", "name": "Битумная черепица, м²", "category": "Кровля", "uom": "м2", "price": 890.00},
  {"sku": "PIPE-PPR-20", "name": "Труба PPR 20 мм", "category": "Сантехника", "uom": "м", "price": 85.00},
  {"sku": "PIPE-PPR-25", "name": "Труба PPR 25 мм", "category": "Сантехника", "uom": "м", "price": 130.00},
  {"sku": "CABLE-VVG-3x2", "name": "Кабель ВВГ 3x2.5", "category": "Электрика", "uom": "м", "price": 95.00},
  {"sku": "SOCKET-220", "name": "Розетка 220В с заземлением", "category": "Электрика", "uom": "шт", "price": 180.00},
  {"sku": "TOOL-HAMMER", "name": "Молоток 500 г", "category": "Инструмент", "uom": "шт", "price": 350.00},
  {"sku": "TOOL-LEVEL", "name": "Уровень строительный 60 см", "category": "Инструмент", "uom": "шт", "price": 890.00}
]
📁 data/init_sales.sql
Скрипт инициализации источника продаж в PostgreSQL (генерирует тестовые данные за 90 дней).

sql
CREATE TABLE IF NOT EXISTS sales (
    sale_id     BIGSERIAL PRIMARY KEY,
    sku         VARCHAR(50)  NOT NULL,
    sale_date   DATE         NOT NULL,
    quantity    NUMERIC(14,2) NOT NULL,
    unit_price  NUMERIC(14,2) NOT NULL,
    store_id    INT          NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sales_date ON sales(sale_date);
CREATE INDEX IF NOT EXISTS idx_sales_sku  ON sales(sku);

-- Наполнение: ~20 SKU × 90 дней × 2 магазина, случайные объёмы.
INSERT INTO sales (sku, sale_date, quantity, unit_price, store_id)
SELECT
    sku,
    CURRENT_DATE - (d || ' days')::interval AS sale_date,
    ROUND((RANDOM() * 20 + 1)::numeric, 2)   AS quantity,
    price * (1 + (RANDOM() - 0.5) * 0.1)     AS unit_price,
    1 + (RANDOM() < 0.5)::int                AS store_id
FROM (
    SELECT unnest(ARRAY[
      'CEM-M500-50','CEM-M400-50','SAND-25','GRAVEL-25','BRICK-RED',
      'BRICK-GAS','PLASTER-GYPS','PUTTY-FIN','PRIMER-DEEP','TILE-ADH',
      'INSUL-EPPS-50','INSUL-MIN-50','ROOF-METAL','ROOF-BITUM',
      'PIPE-PPR-20','PIPE-PPR-25','CABLE-VVG-3x2','SOCKET-220',
      'TOOL-HAMMER','TOOL-LEVEL'
    ]) AS sku,
    unnest(ARRAY[
      420,350,95,120,28.5,210,380,640,520,310,
      480,720,560,890,85,130,95,180,350,890
    ]) AS price
) skus
CROSS JOIN generate_series(0, 89) AS d;
📁 sql/clickhouse_init.sql
Создаёт БД analytics и сырые таблицы при первом старте контейнера.

sql
CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.raw_catalog
(
    sku         String,
    name        String,
    category    String,
    uom         String,
    price       Decimal(14, 2),
    loaded_at   DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY sku;

CREATE TABLE IF NOT EXISTS analytics.raw_sales
(
    sale_id     Int64,
    sku         String,
    sale_date   Date,
    quantity    Decimal(14, 2),
    unit_price  Decimal(14, 2),
    store_id    Int32,
    loaded_at   DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (sale_date, sku);
📁 sql/clickhouse_marts.sql
Витрины: оборачиваемость и ABC-анализ.

sql
-- =============================================================
-- Витрина 1: mart_product_turnover
-- Оборачиваемость по SKU: продано, выручка, средняя цена,
-- число активных дней, среднесуточные продажи.
-- =============================================================
DROP TABLE IF EXISTS analytics.mart_product_turnover;

CREATE TABLE analytics.mart_product_turnover
ENGINE = MergeTree
ORDER BY sku
AS
WITH
    catalog AS (
        SELECT sku, name, category, uom, price AS catalog_price
        FROM analytics.raw_catalog FINAL
    ),
    sales AS (
        SELECT
            sku,
            sum(quantity)                       AS qty_sold,
            sum(quantity * unit_price)          AS revenue,
            avg(unit_price)                     AS avg_price,
            uniqExact(sale_date)                AS active_days,
            min(sale_date)                      AS first_sale,
            max(sale_date)                      AS last_sale
        FROM analytics.raw_sales
        GROUP BY sku
    )
SELECT
    c.sku,
    c.name,
    c.category,
    c.uom,
    c.catalog_price,
    coalesce(s.qty_sold, 0)                                 AS qty_sold,
    coalesce(s.revenue, 0)                                  AS revenue,
    coalesce(round(s.avg_price, 2), 0)                      AS avg_sale_price,
    coalesce(s.active_days, 0)                              AS active_days,
    coalesce(s.first_sale, toDate('1970-01-01'))            AS first_sale,
    coalesce(s.last_sale,  toDate('1970-01-01'))            AS last_sale,
    if(s.active_days > 0,
       round(s.qty_sold / s.active_days, 4),
       0)                                                    AS avg_daily_qty,
    -- "Оборачиваемость" как средний период, за который продаётся
    -- объём, примерно равный месячной продаже (30 * avg_daily_qty).
    if(s.qty_sold > 0,
       round(30.0 * s.active_days / s.qty_sold, 2),
       999)                                                  AS days_of_supply_30d,
    now()                                                    AS refreshed_at
FROM catalog c
LEFT JOIN sales s ON c.sku = s.sku;


-- =============================================================
-- Витрина 2: mart_abc_analysis
-- Классическая ABC по вкладу в выручку (A ≤ 80%, B ≤ 95%, C — остальное)
-- + XYZ не делаем: для этого нужны регулярные ряды; см. README.
-- =============================================================
DROP TABLE IF EXISTS analytics.mart_abc_analysis;

CREATE TABLE analytics.mart_abc_analysis
ENGINE = MergeTree
ORDER BY (abc_class, rank_revenue)
AS
WITH base AS (
    SELECT
        sku,
        name,
        category,
        revenue,
        qty_sold,
        avg_sale_price,
        active_days,
        row_number() OVER (ORDER BY revenue DESC)                            AS rank_revenue,
        revenue / sum(revenue) OVER ()                                       AS share_revenue,
        sum(revenue) OVER (ORDER BY revenue DESC
                           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_revenue
    FROM analytics.mart_product_turnover
    WHERE revenue > 0
),
pct AS (
    SELECT
        *,
        cum_revenue / sum(revenue) OVER () AS cum_share
    FROM base
)
SELECT
    sku,
    name,
    category,
    revenue,
    qty_sold,
    avg_sale_price,
    active_days,
    rank_revenue,
    round(share_revenue * 100, 2) AS share_revenue_pct,
    round(cum_share * 100, 2)     AS cum_share_pct,
    multiIf(
        cum_share <= 0.80, 'A',
        cum_share <= 0.95, 'B',
        'C'
    ) AS abc_class,
    now() AS refreshed_at
FROM pct;
📁 dags/catalog_sales_pipeline.py
DAG с двумя ветками (load_catalog из JSON, load_sales из Postgres), сведением в ClickHouse и пересборкой витрин.

python
"""
DAG: catalog_sales_pipeline
---------------------------
Две независимые ветки загрузки:
  • load_catalog  : JSON  -> ClickHouse.raw_catalog
  • load_sales    : Postgres -> ClickHouse.raw_sales

После завершения обеих веток:
  • build_mart_turnover  — пересобирает mart_product_turnover
  • build_mart_abc       — пересобирает mart_abc_analysis (зависит от turnover)
"""

from __future__ import annotations

import json
import os
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from clickhouse_driver import Client


log = logging.getLogger(__name__)

# ---------- Конфигурация через Airflow Variables (с дефолтами) ----------
CH_HOST     = Variable.get("ch_host",     default_var="clickhouse")
CH_PORT     = int(Variable.get("ch_port", default_var="9000"))
CH_DB       = Variable.get("ch_db",       default_var="analytics")
CH_USER     = Variable.get("ch_user",     default_var="default")
CH_PASSWORD = Variable.get("ch_password", default_var="")

PG_CONN_ID       = "sales_postgres"
CATALOG_JSON     = "/opt/airflow/data/catalog_sample.json"
MARTS_SQL_PATH   = "/opt/airflow/sql/clickhouse_marts.sql"

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "depends_on_past": False,
}


def get_clickhouse_client() -> Client:
    return Client(
        host=CH_HOST,
        port=CH_PORT,
        database=CH_DB,
        user=CH_USER,
        password=CH_PASSWORD,
    )


def split_statements(sql_text: str) -> list[str]:
    """Разбивает SQL на отдельные стейтменты (учитывает комментарии и ; внутри строк)."""
    statements, buf, i, n = [], [], 0, len(sql_text)
    in_str = False
    quote = ""
    while i < n:
        ch = sql_text[i]
        if not in_str and sql_text.startswith("--", i):
            # пропустить до конца строки
            nl = sql_text.find("\n", i)
            i = n if nl == -1 else nl + 1
            continue
        if ch in ("'", '"'):
            if not in_str:
                in_str, quote = True, ch
            elif ch == quote:
                # проверка на экранирование ''
                if i + 1 < n and sql_text[i + 1] == quote:
                    buf.append(ch); i += 1
                else:
                    in_str = False
        buf.append(ch)
        if ch == ";" and not in_str:
            s = "".join(buf).strip().rstrip(";").strip()
            if s:
                statements.append(s)
            buf = []
        i += 1
    tail = "".join(buf).strip()
    if tail:
        statements.append(tail)
    return statements


with DAG(
    dag_id="catalog_sales_pipeline",
    description="Catalog (JSON) + Sales (Postgres) -> ClickHouse -> Marts",
    default_args=default_args,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule="0 2 * * *",     # ежедневно в 02:00 UTC
    catchup=False,
    max_active_runs=1,
    tags=["etl", "clickhouse", "retail"],
) as dag:

    # -------------------------------------------------------------
    # Ветка 1: каталог
    # -------------------------------------------------------------
    @task(task_id="load_catalog")
    def load_catalog() -> int:
        path = Path(CATALOG_JSON)
        if not path.exists():
            raise FileNotFoundError(f"Catalog file not found: {CATALOG_JSON}")

        with path.open(encoding="utf-8") as f:
            items = json.load(f)

        rows = [
            (
                it["sku"],
                it["name"],
                it["category"],
                it["uom"],
                float(it["price"]),
            )
            for it in items
        ]

        client = get_clickhouse_client()
        client.execute(
            "TRUNCATE TABLE IF EXISTS analytics.raw_catalog"
        )
        client.execute(
            "INSERT INTO analytics.raw_catalog (sku, name, category, uom, price) VALUES",
            rows,
        )
        log.info("Loaded %d catalog rows", len(rows))
        return len(rows)

    # -------------------------------------------------------------
    # Ветка 2: продажи
    # -------------------------------------------------------------
    @task(task_id="load_sales")
    def load_sales() -> int:
        pg = PostgresHook(postgres_conn_id=PG_CONN_ID)
        with pg.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT sale_id, sku, sale_date, quantity, unit_price, store_id
                    FROM sales
                    WHERE sale_date >= CURRENT_DATE - INTERVAL '180 days'
                    """
                )
                rows = cur.fetchall()

        if not rows:
            log.warning("No sales rows fetched")
            return 0

        client = get_clickhouse_client()
        client.execute("TRUNCATE TABLE IF EXISTS analytics.raw_sales")
        client.execute(
            """
            INSERT INTO analytics.raw_sales
                (sale_id, sku, sale_date, quantity, unit_price, store_id)
            VALUES
            """,
            [
                (int(r[0]), r[1], r[2], float(r[3]), float(r[4]), int(r[5]))
                for r in rows
            ],
        )
        log.info("Loaded %d sales rows", len(rows))
        return len(rows)

    # -------------------------------------------------------------
    # Витрины
    # -------------------------------------------------------------
    @task(task_id="build_mart_turnover")
    def build_mart_turnover() -> None:
        sql_text = Path(MARTS_SQL_PATH).read_text(encoding="utf-8")
        statements = split_statements(sql_text)
        client = get_clickhouse_client()

        # Пересобираем только turnover (первый блок до комментария ABC).
        # Простое решение — выполнить все CREATE/DROP из файла, но
        # для чистоты: ищем блок с "mart_product_turnover".
        turnover_stmts = [
            s for s in statements
            if "mart_product_turnover" in s
        ]
        if not turnover_stmts:
            raise RuntimeError("No turnover statements found in SQL file")
        for s in turnover_stmts:
            client.execute(s)
        log.info("Rebuilt mart_product_turnover")

    @task(task_id="build_mart_abc")
    def build_mart_abc() -> None:
        sql_text = Path(MARTS_SQL_PATH).read_text(encoding="utf-8")
        statements = split_statements(sql_text)
        client = get_clickhouse_client()

        abc_stmts = [s for s in statements if "mart_abc_analysis" in s]
        if not abc_stmts:
            raise RuntimeError("No ABC statements found in SQL file")
        for s in abc_stmts:
            client.execute(s)
        log.info("Rebuilt mart_abc_analysis")

    # -------------------------------------------------------------
    # Граф
    # -------------------------------------------------------------
    cat = load_catalog()
    sal = load_sales()

    turnover = build_mart_turnover()
    abc      = build_mart_abc()

    # обе ветки должны завершиться перед витринами
    [cat, sal] >> turnover
    turnover >> abc
📁 README.md
markdown
# product-sales-marts-airflow

ETL-пайплайн «Каталог товаров + Продажи → ClickHouse → 2 витрины → Metabase».

## Архитектура

```mermaid
flowchart LR
    subgraph Sources
        JSON[/catalog_sample.json/]
        PG[(PostgreSQL<br/>sales)]
    end

    subgraph Airflow[DAG: catalog_sales_pipeline]
        LC[load_catalog]
        LS[load_sales]
        MT[build_mart_turnover]
        MA[build_mart_abc]
    end

    subgraph ClickHouse
        RC[(raw_catalog)]
        RS[(raw_sales)]
        V1[(mart_product_turnover)]
        V2[(mart_abc_analysis)]
    end

    MB[Metabase<br/>2 дашборда]

    JSON --> LC --> RC
    PG   --> LS --> RS
    RC --> MT
    RS --> MT
    MT --> V1
    MT --> MA --> V2
    V1 --> MB
    V2 --> MB
```

## Сервисы (docker-compose)

| Сервис            | URL                     | Логин / пароль             |
|-------------------|-------------------------|----------------------------|
| Airflow Webserver | http://localhost:8080   | `admin` / `admin`          |
| Metabase          | http://localhost:3000   | первичная настройка        |
| ClickHouse HTTP   | http://localhost:8123   | `default` / (пусто)        |
| Postgres (sales)  | localhost:5433          | `sales` / `sales` / `sales_db` |

## Запуск

```bash
mkdir -p logs
export AIRFLOW_UID=$(id -u)
docker compose up -d --build
```

Через 1–2 минуты Airflow будет доступен. DAG `catalog_sales_pipeline` стартует по расписанию `0 2 * * *`, но можно запустить вручную (кнопка **Trigger DAG**).

## Витрины

### `mart_product_turnover`
- `sku`, `name`, `category`, `uom`, `catalog_price`
- `qty_sold`, `revenue`, `avg_sale_price`
- `active_days` — число различных дней с продажами
- `avg_daily_qty` — среднесуточный объём продаж
- `days_of_supply_30d` — грубая оценка «на сколько дней хватит объёма, примерно равного 30-дневному»

### `mart_abc_analysis`
- `abc_class` (A ≤ 80% накопленной выручки, B ≤ 95%, C — остальное)
- `rank_revenue`, `share_revenue_pct`, `cum_share_pct`

## Пример SQL для Metabase

**Топ-10 SKU по выручке:**
```sql
SELECT sku, name, category, revenue, qty_sold
FROM analytics.mart_product_turnover
ORDER BY revenue DESC
LIMIT 10;
```

**Распределение по классам ABC:**
```sql
SELECT abc_class,
       count()                AS sku_count,
       sum(revenue)           AS revenue,
       round(avg(cum_share_pct), 2) AS avg_cum_share
FROM analytics.mart_abc_analysis
GROUP BY abc_class
ORDER BY abc_class;
```

**Проблемные SKU (нет продаж 30+ дней, но есть в каталоге):**
```sql
SELECT sku, name, category, last_sale, revenue
FROM analytics.mart_product_turnover
WHERE active_days = 0
   OR last_sale < today() - 30
ORDER BY revenue DESC;
```

## Дашборды Metabase

1. **«Оборачиваемость»**:
   - таблица `mart_product_turnover` с фильтрами по категории;
   - bar-чарт «Топ-15 по выручке»;
   - scatter `avg_daily_qty` × `days_of_supply_30d`.

2. **«ABC-анализ»**:
   - pie-чарт доли выручки по классам;
   - таблица с накопленной долей;
   - фильтр по `abc_class`.

### Подключение Metabase к ClickHouse
1. **Admin → Databases → Add database**.
2. Тип — **ClickHouse** (плагин ставится из «Add a database» → "ClickHouse").
3. Host `clickhouse`, port `8123`, DB `analytics`, user `default`, без пароля.
4. Включите опцию **"Include schemas in queries"**, чтобы Metabase показывал таблицы.

## Структура репозитория

```
.
├── dags/catalog_sales_pipeline.py
├── sql/
│   ├── clickhouse_init.sql
│   └── clickhouse_marts.sql
├── data/
│   ├── catalog_sample.json
│   └── init_sales.sql
├── docker-compose.yml
└── README.md
```

## Что демонстрирует проект

- Мультиисточниковый ETL (JSON + Postgres) с оркестрацией в Airflow.
- Разделение DAG на независимые ветки с последующим слиянием.
- Две аналитические витрины в ClickHouse с реальной бизнес-логикой (оборачиваемость, ABC).
- Готовность к BI: Metabase как self-service.
- Идемпотентность: `TRUNCATE + INSERT` для сырья, полная пересборка витрин.
Примечания по запуску
Перед docker compose up создайте папку logs/ — она монтируется в контейнеры Airflow.

AIRFLOW_UID должен совпадать с вашим UID (на Linux/macOS): export AIRFLOW_UID=$(id -u).

Metabase в первый раз поднимется 1–2 минуты и попросит создать администратора — после этого подключите ClickHouse через Admin → Databases (см. README).

Плагин ClickHouse для Metabase ставится автоматически из «Add a database» (образ Metabase v0.50+ содержит его).

Если _PIP_ADDITIONAL_REQUIREMENTS падает из-за отсутствующего пакета apache-airflow-providers-clickhouse, уберите его — DAG использует нативный clickhouse-driver напрямую.

