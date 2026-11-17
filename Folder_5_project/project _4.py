Проект 4: Витрина поставщиков и логистики (PostgreSQL → ClickHouse через dbt)
Бизнес-проблема: Закупщикам строительных материалов нужна единая витрина, которая объединяет данные о поставщиках (из PostgreSQL — операционная БД) и данные о логистике (из CSV-файлов или API транспортной компании). Сейчас данные разрознены, отчёты строятся вручную.

Архитектура и стек:

Источники: PostgreSQL (таблица suppliers), CSV (логистика).

ETL: Python-скрипт (или Airflow DAG) для извлечения из PostgreSQL и CSV → загрузка в ClickHouse (сырой слой).

Трансформация: dbt (ClickHouse adapter) для построения витрины mart_supplier_performance (сроки поставки, доля брака, стоимость логистики на единицу).

Визуализация: Lightdash (open-source BI, нативно работает с dbt-моделями ClickHouse).

Что демонстрирует: Классический паттерн «операционная БД → аналитическое хранилище» (PostgreSQL → ClickHouse), использование dbt для документирования и тестирования витрин, работа с современным dbt-ориентированным BI (Lightdash) .

Структура репозитория:


text
/supplier-logistics-mart
├── extract/load_to_clickhouse.py
├── dbt_project/models/mart_supplier_performance.sql
├── docker-compose.yml (Postgres + ClickHouse + Lightdash)
└── README.md (lineage-граф dbt, скриншот дашборда)                 напиши код всех файлов этого проекта
Проект: Витрина поставщиков и логистики (PostgreSQL → ClickHouse через dbt)
Ниже — полный код всех файлов проекта. Структура:


text
/supplier-logistics-mart
├── docker-compose.yml
├── .env
├── extract/
│   ├── load_to_clickhouse.py
│   ├── requirements.txt
│   ├── init_postgres.sql
│   └── data/logistics.csv
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml
│   │   │   ├── stg_suppliers.sql
│   │   │   └── stg_logistics.sql
│   │   ├── intermediate/
│   │   │   └── int_supplier_logistics.sql
│   │   └── mart/
│   │       ├── mart_supplier_performance.sql
│   │       └── schema.yml
│   └── tests/
│       └── assert_no_negative_cost.sql
└── README.md
1. docker-compose.yml
yaml
version: "3.9"

services:
  postgres:
    image: postgres:15
    container_name: sl_postgres
    environment:
      POSTGRES_USER: ${PG_USER}
      POSTGRES_PASSWORD: ${PG_PASSWORD}
      POSTGRES_DB: ${PG_DB}
    ports:
      - "5432:5432"
    volumes:
      - pg_data:/var/lib/postgresql/data
      - ./extract/init_postgres.sql:/docker-entrypoint-initdb.d/init.sql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${PG_USER} -d ${PG_DB}"]
      interval: 5s
      retries: 10

  clickhouse:
    image: clickhouse/clickhouse-server:23.8
    container_name: sl_clickhouse
    environment:
      CLICKHOUSE_USER: ${CH_USER}
      CLICKHOUSE_PASSWORD: ${CH_PASSWORD}
      CLICKHOUSE_DB: ${CH_DB}
      CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT: 1
    ports:
      - "8123:8123"
      - "9000:9000"
    volumes:
      - ch_data:/var/lib/clickhouse
    ulimits:
      nofile:
        soft: 262144
        hard: 262144

  loader:
    build:
      context: ./extract
    container_name: sl_loader
    depends_on:
      postgres:
        condition: service_healthy
      clickhouse:
        condition: service_started
    env_file: .env
    volumes:
      - ./extract/data:/app/data

  dbt:
    image: ghcr.io/dbt-labs/dbt-clickhouse:1.6.4
    container_name: sl_dbt
    depends_on:
      - clickhouse
    env_file: .env
    volumes:
      - ./dbt_project:/usr/app/dbt
    working_dir: /usr/app/dbt
    entrypoint: ["/bin/bash", "-c", "sleep 15 && dbt deps && dbt run && dbt test"]
    # После первого запуска можно перезапускать вручную: docker compose run --rm dbt

  lightdash:
    image: lightdash/lightdash:latest
    container_name: sl_lightdash
    depends_on:
      - clickhouse
    environment:
      LIGHTDASH_SECRET: ${LIGHTDASH_SECRET}
      PGHOST: postgres
      PGPORT: 5432
      PGUSER: ${PG_USER}
      PGPASSWORD: ${PG_PASSWORD}
      PGDATABASE: ${LIGHTDASH_DB}
      LIGHTDASH_MODE: production
    ports:
      - "8080:8080"
    volumes:
      - lightdash_data:/usr/app/dbt

volumes:
  pg_data:
  ch_data:
  lightdash_data:
Для Lightdash нужна отдельная БД-метастор. В init_postgres.sql создадим LIGHTDASH_DB.

2. .env
dotenv
PG_USER=supplier_user
PG_PASSWORD=supplier_pass
PG_DB=supplier_db
PG_HOST=postgres
PG_PORT=5432

CH_USER=default
CH_PASSWORD=clickhouse_pass
CH_DB=analytics
CH_HOST=clickhouse
CH_PORT=8123

LIGHTDASH_SECRET=super_secret_change_me
LIGHTDASH_DB=lightdash_meta
3. extract/init_postgres.sql
sql
-- Операционная БД: поставщики
CREATE TABLE IF NOT EXISTS suppliers (
    supplier_id     SERIAL PRIMARY KEY,
    supplier_name   VARCHAR(255) NOT NULL,
    country         VARCHAR(100),
    category        VARCHAR(100),        -- например: 'cement', 'bricks', 'steel'
    contract_date   DATE,
    is_active       BOOLEAN DEFAULT TRUE
);

INSERT INTO suppliers (supplier_name, country, category, contract_date, is_active) VALUES
('ООО Цемент-Строй',    'Россия',   'cement', '2023-01-15', TRUE),
('ООО КирпичГрад',      'Россия',   'bricks', '2023-03-10', TRUE),
('SteelWorks GmbH',     'Германия', 'steel',  '2022-11-05', TRUE),
('Beijing Building Co','Китай',    'steel',  '2023-06-20', TRUE),
('ООО ПесокПлюс',       'Россия',   'sand',   '2023-09-01', FALSE);

-- Метастор для Lightdash (создаём отдельную БД)
CREATE DATABASE lightdash_meta;
4. extract/requirements.txt
text
psycopg2-binary==2.9.9
clickhouse-connect==0.7.4
pandas==2.2.0
python-dotenv==1.0.0
5. extract/load_to_clickhouse.py
python
"""
ETL: PostgreSQL (suppliers) + CSV (logistics) -> ClickHouse (raw layer).
Идемпотентно: перед загрузкой очищает raw-таблицы (TRUNCATE).
"""
import os
import logging
from datetime import datetime, date
from pathlib import Path

import pandas as pd
import psycopg2
from clickhouse_connect import get_client
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

load_dotenv()

# ---------- CONFIG ----------
PG_CONF = dict(
    host=os.getenv("PG_HOST", "postgres"),
    port=int(os.getenv("PG_PORT", 5432)),
    user=os.getenv("PG_USER"),
    password=os.getenv("PG_PASSWORD"),
    dbname=os.getenv("PG_DB"),
)
CH_CONF = dict(
    host=os.getenv("CH_HOST", "clickhouse"),
    port=int(os.getenv("CH_PORT", 8123)),
    username=os.getenv("CH_USER", "default"),
    password=os.getenv("CH_PASSWORD", ""),
)
CH_DB = os.getenv("CH_DB", "analytics")
CSV_PATH = Path(os.getenv("CSV_PATH", "/app/data/logistics.csv"))


# ---------- EXTRACT ----------
def extract_suppliers() -> pd.DataFrame:
    log.info("Extracting suppliers from PostgreSQL...")
    with psycopg2.connect(**PG_CONF) as conn:
        df = pd.read_sql("SELECT * FROM suppliers;", conn)
    log.info("Extracted %d suppliers", len(df))
    return df


def extract_logistics() -> pd.DataFrame:
    log.info("Extracting logistics from CSV: %s", CSV_PATH)
    df = pd.read_csv(CSV_PATH, parse_dates=["shipment_date", "delivery_date"])
    log.info("Extracted %d logistics rows", len(df))
    return df


# ---------- LOAD ----------
def _ensure_database(client):
    client.command(f"CREATE DATABASE IF NOT EXISTS {CH_DB}")


def _create_tables(client):
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {CH_DB}.suppliers_raw (
            supplier_id     UInt32,
            supplier_name   String,
            country         LowCardinality(String),
            category        LowCardinality(String),
            contract_date   Nullable(Date),
            is_active       UInt8,
            _loaded_at      DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree(_loaded_at)
        ORDER BY supplier_id
    """)

    client.command(f"""
        CREATE TABLE IF NOT EXISTS {CH_DB}.logistics_raw (
            shipment_id     String,
            supplier_id     UInt32,
            shipment_date   Nullable(Date),
            delivery_date   Nullable(Date),
            planned_lead_time_days  UInt16,
            actual_lead_time_days   UInt16,
            quantity        Float64,
            unit            LowCardinality(String),
            logistics_cost  Float64,
            defect_quantity Float64,
            _loaded_at      DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree(_loaded_at)
        ORDER BY (supplier_id, shipment_id)
    """)


def load_to_clickhouse(suppliers: pd.DataFrame, logistics: pd.DataFrame):
    log.info("Connecting to ClickHouse %s:%s...", CH_CONF["host"], CH_CONF["port"])
    client = get_client(**CH_CONF)
    _ensure_database(client)
    _create_tables(client)

    # --- suppliers ---
    suppliers = suppliers.copy()
    suppliers["contract_date"] = pd.to_datetime(suppliers["contract_date"]).dt.date
    suppliers["is_active"] = suppliers["is_active"].astype(int)

    client.command(f"TRUNCATE TABLE {CH_DB}.suppliers_raw")
    client.insert(
        f"{CH_DB}.suppliers_raw",
        suppliers[
            ["supplier_id", "supplier_name", "country", "category", "contract_date", "is_active"]
        ].values.tolist(),
        column_names=[
            "supplier_id", "supplier_name", "country", "category", "contract_date", "is_active"
        ],
    )
    log.info("Loaded %d suppliers into ClickHouse", len(suppliers))

    # --- logistics ---
    logistics = logistics.copy()
    logistics["shipment_date"] = pd.to_datetime(logistics["shipment_date"]).dt.date
    logistics["delivery_date"] = pd.to_datetime(logistics["delivery_date"]).dt.date

    client.command(f"TRUNCATE TABLE {CH_DB}.logistics_raw")
    client.insert(
        f"{CH_DB}.logistics_raw",
        logistics[
            [
                "shipment_id", "supplier_id", "shipment_date", "delivery_date",
                "planned_lead_time_days", "actual_lead_time_days",
                "quantity", "unit", "logistics_cost", "defect_quantity",
            ]
        ].values.tolist(),
        column_names=[
            "shipment_id", "supplier_id", "shipment_date", "delivery_date",
            "planned_lead_time_days", "actual_lead_time_days",
            "quantity", "unit", "logistics_cost", "defect_quantity",
        ],
    )
    log.info("Loaded %d logistics rows into ClickHouse", len(logistics))
    client.close()


if __name__ == "__main__":
    log.info("=== ETL started at %s ===", datetime.now().isoformat())
    sup = extract_suppliers()
    lg = extract_logistics()
    load_to_clickhouse(sup, lg)
    log.info("=== ETL finished ===")
6. extract/Dockerfile
dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY load_to_clickhouse.py .
COPY data ./data

CMD ["python", "load_to_clickhouse.py"]
7. extract/data/logistics.csv
csv
shipment_id,supplier_id,shipment_date,delivery_date,planned_lead_time_days,actual_lead_time_days,quantity,unit,logistics_cost,defect_quantity
SH-0001,1,2024-01-05,2024-01-09,4,4,120,tons,45000.00,0
SH-0002,1,2024-01-15,2024-01-21,4,6,80,tons,32000.00,2
SH-0003,2,2024-02-01,2024-02-04,3,3,50000,pieces,18000.00,150
SH-0004,2,2024-02-10,2024-02-13,3,3,45000,pieces,16500.00,0
SH-0005,3,2024-02-20,2024-03-01,7,10,60,tons,95000.00,1
SH-0006,3,2024-03-05,2024-03-11,7,6,70,tons,105000.00,0
SH-0007,4,2024-03-12,2024-03-25,10,13,200,tons,220000.00,5
SH-0008,1,2024-04-01,2024-04-05,4,4,150,tons,55000.00,1
SH-0009,2,2024-04-10,2024-04-14,3,4,60000,pieces,21000.00,300
SH-0010,3,2024-05-02,2024-05-08,7,6,90,tons,130000.00,0
8. dbt_project/dbt_project.yml
yaml
name: 'supplier_logistics'
version: '1.0.0'
config-version: 2

profile: 'supplier_logistics'

model-paths: ["models"]
test-paths: ["tests"]
macro-paths: ["macros"]
seed-paths: ["seeds"]

target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"

models:
  supplier_logistics:
    staging:
      +materialized: view
      +schema: staging
    intermediate:
      +materialized: view
      +schema: intermediate
    mart:
      +materialized: table
      +schema: mart
9. dbt_project/profiles.yml
yaml
supplier_logistics:
  target: dev
  outputs:
    dev:
      type: clickhouse
      schema: "{{ env_var('CH_DB', 'analytics') }}"
      host: "{{ env_var('CH_HOST', 'clickhouse') }}"
      port: "{{ env_var('CH_PORT', 8123) | int }}"
      user: "{{ env_var('CH_USER', 'default') }}"
      password: "{{ env_var('CH_PASSWORD', '') }}"
      secure: false
      verify: false
      threads: 4
10. dbt_project/packages.yml
yaml
packages:
  - package: dbt-labs/dbt_utils
    version: ">=1.1.0"
11. dbt_project/models/staging/sources.yml
yaml
version: 2

sources:
  - name: raw
    database: "{{ env_var('CH_DB', 'analytics') }}"
    schema: "{{ env_var('CH_DB', 'analytics') }}"
    description: "Сырой слой, загруженный ETL-скриптом из PostgreSQL и CSV"
    tables:
      - name: suppliers_raw
        description: "Поставщики из операционной БД PostgreSQL"
        columns:
          - name: supplier_id
            tests: [not_null, unique]
          - name: supplier_name
            tests: [not_null]
      - name: logistics_raw
        description: "Логистика поставок из CSV / API транспортной компании"
        columns:
          - name: shipment_id
            tests: [not_null, unique]
          - name: supplier_id
            tests:
              - not_null
              - relationships:
                  to: source('raw', 'suppliers_raw')
                  field: supplier_id
12. dbt_project/models/staging/stg_suppliers.sql
sql
{{ config(materialized='view') }}

WITH src AS (
    SELECT *
    FROM {{ source('raw', 'suppliers_raw') }}
    -- ReplacingMergeTree: берём последнюю версию записи
    FINAL
)

SELECT
    supplier_id,
    trim(supplier_name)          AS supplier_name,
    upper(trim(country))         AS country,
    lower(trim(category))        AS category,
    contract_date,
    toUInt8(is_active)           AS is_active
FROM src
WHERE supplier_id IS NOT NULL
13. dbt_project/models/staging/stg_logistics.sql
sql
{{ config(materialized='view') }}

WITH src AS (
    SELECT *
    FROM {{ source('raw', 'logistics_raw') }}
    FINAL
)

SELECT
    shipment_id,
    supplier_id,
    shipment_date,
    delivery_date,
    planned_lead_time_days,
    actual_lead_time_days,
    quantity,
    unit,
    logistics_cost,
    defect_quantity,
    -- вычисляем задержку поставки (в днях)
    dateDiff('day', shipment_date, delivery_date)     AS delivery_duration_days,
    actual_lead_time_days - planned_lead_time_days    AS lead_time_delta_days
FROM src
WHERE shipment_id IS NOT NULL
  AND supplier_id IS NOT NULL
14. dbt_project/models/intermediate/int_supplier_logistics.sql
sql
{{ config(materialized='view') }}

WITH sup AS (
    SELECT * FROM {{ ref('stg_suppliers') }}
),
log AS (
    SELECT * FROM {{ ref('stg_logistics') }}
)

SELECT
    l.shipment_id,
    l.supplier_id,
    s.supplier_name,
    s.country,
    s.category,
    s.is_active,
    l.shipment_date,
    l.delivery_date,
    l.planned_lead_time_days,
    l.actual_lead_time_days,
    l.lead_time_delta_days,
    l.quantity,
    l.unit,
    l.logistics_cost,
    l.defect_quantity
FROM log AS l
LEFT JOIN sup AS s USING (supplier_id)
15. dbt_project/models/mart/mart_supplier_performance.sql
sql
{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(supplier_id)'
) }}

WITH base AS (
    SELECT * FROM {{ ref('int_supplier_logistics') }}
),

aggregated AS (
    SELECT
        supplier_id,
        any(supplier_name) AS supplier_name,
        any(country)       AS country,
        any(category)      AS category,
        max(is_active)     AS is_active,

        -- объёмы
        count()                              AS shipments_count,
        sum(quantity)                        AS total_quantity,
        any(unit)                            AS unit,

        -- сроки поставки
        avg(actual_lead_time_days)           AS avg_lead_time_days,
        avg(planned_lead_time_days)          AS avg_planned_lead_time_days,
        avg(lead_time_delta_days)            AS avg_delay_days,
        countIf(lead_time_delta_days > 0)    AS delayed_shipments_count,
        countIf(lead_time_delta_days <= 0)   AS on_time_shipments_count,

        -- качество
        sum(defect_quantity)                 AS total_defects,
        sum(quantity)                        AS total_quantity_for_defects,

        -- стоимость логистики
        sum(logistics_cost)                  AS total_logistics_cost,
        sum(quantity)                        AS total_quantity_for_cost,

        min(shipment_date)                   AS first_shipment_date,
        max(shipment_date)                   AS last_shipment_date

    FROM base
    GROUP BY supplier_id
),

final AS (
    SELECT
        supplier_id,
        supplier_name,
        country,
        category,
        is_active,
        shipments_count,
        total_quantity,
        unit,

        round(avg_lead_time_days, 2)        AS avg_lead_time_days,
        round(avg_planned_lead_time_days, 2) AS avg_planned_lead_time_days,
        round(avg_delay_days, 2)            AS avg_delay_days,

        -- доля поставок с задержкой
        round(delayed_shipments_count / shipments_count * 100, 2)  AS delayed_share_pct,
        round(on_time_shipments_count / shipments_count * 100, 2)  AS on_time_share_pct,

        -- доля брака
        round(
            total_defects / nullIf(total_quantity_for_defects, 0) * 100,
            4
        )                                   AS defect_rate_pct,

        -- стоимость логистики на единицу
        round(
            total_logistics_cost / nullIf(total_quantity_for_cost, 0),
            2
        )                                   AS logistics_cost_per_unit,

        total_logistics_cost,
        total_defects,
        first_shipment_date,
        last_shipment_date,

        -- интегральный скоринг (для сортировки закупщиком):
        -- меньше задержек, меньше брака, ниже стоимость = лучше
        round(
            100
            - coalesce(delayed_share_pct, 0) * 0.4
            - coalesce(defect_rate_pct, 0) * 10 * 0.4
            - least(coalesce(logistics_cost_per_unit, 0), 1000) / 1000 * 100 * 0.2,
            2
        )                                   AS supplier_score
    FROM aggregated
)

SELECT * FROM final
16. dbt_project/models/mart/schema.yml
yaml
version: 2

models:
  - name: mart_supplier_performance
    description: >
      Витрина эффективности поставщиков и логистики.
      Одна строка на поставщика. Используется закупщиками для сравнения
      поставщиков по срокам поставки, доле брака и стоимости логистики.
    columns:
      - name: supplier_id
        description: "Уникальный идентификатор поставщика"
        tests:
          - not_null
          - unique

      - name: supplier_name
        description: "Название поставщика"
        tests:
          - not_null

      - name: country
        description: "Страна поставщика"

      - name: category
        description: "Категория материалов"

      - name: is_active
        description: "Признак активности контракта"

      - name: shipments_count
        description: "Количество поставок"
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= 0"

      - name: avg_lead_time_days
        description: "Средний фактический срок поставки, дни"

      - name: avg_delay_days
        description: "Средняя задержка относительно плана, дни (может быть отрицательной)"

      - name: delayed_share_pct
        description: "Доля поставок с задержкой, %"
        tests:
          - dbt_utils.expression_is_true:
              expression: "between 0 and 100"

      - name: defect_rate_pct
        description: "Доля брака от объёма, %"
        tests:
          - dbt_utils.expression_is_true:
              expression: ">= 0 and <= 100"

      - name: logistics_cost_per_unit
        description: "Стоимость логистики на единицу объёма"
        tests:
          - dbt_utils.expression_is_true:
              expression: ">= 0"

      - name: supplier_score
        description: "Интегральный скоринг поставщика (0–100, выше — лучше)"
17. dbt_project/tests/assert_no_negative_cost.sql
sql
-- Кастомный generic-тест: стоимость логистики не может быть отрицательной
SELECT *
FROM {{ ref('mart_supplier_performance') }}
WHERE logistics_cost_per_unit < 0
   OR total_logistics_cost < 0
18. README.md
markdown
# 🏗️ Supplier & Logistics Mart (PostgreSQL → ClickHouse → dbt → Lightdash)

Единая витрина эффективности поставщиков и логистики для закупщиков строительных материалов.

## 📌 Бизнес-задача
Данные о поставщиках хранятся в операционной **PostgreSQL**, данные о логистике приходят в **CSV** (или API транспортной компании). Отчёты раньше собирались вручную. Теперь — **одна витрина `mart_supplier_performance`**, обновляемая автоматически.

## 🏛 Архитектура

```
PostgreSQL ──┐
             ├──► ETL (Python) ──► ClickHouse (raw) ──► dbt ──► mart_supplier_performance ──► Lightdash
CSV ─────────┘
```

### Lineage-граф (dbt)
```
source('raw','suppliers_raw') ─┐
                                ├──► stg_suppliers ─┐
source('raw','logistics_raw') ─┘                   ├──► int_supplier_logistics ──► mart_supplier_performance
                                └──► stg_logistics ┘
```

Построить реальный граф:
```bash
docker compose run --rm dbt dbt docs generate
# docs/  ->  target/index.html
```

## 🚀 Запуск

```bash
cp .env.example .env          # отредактируйте секреты
docker compose up -d postgres clickhouse
docker compose up loader       # ETL: PG + CSV -> ClickHouse raw
docker compose up dbt          # dbt deps + run + test
docker compose up -d lightdash # http://localhost:8080
```

### Первый вход в Lightdash
1. Откройте http://localhost:8080
2. Зарегистрируйте admin-пользователя.
3. Project → **dbt project** → укажите путь `/usr/app/dbt` (mounted).
4. Warehouse → **ClickHouse**:
   - Host: `clickhouse`, Port: `8123`
   - User: `default`, Password: `clickhouse_pass`, Database: `analytics`
5. Lightdash автоматически подхватит `mart_supplier_performance` и метрики из `schema.yml`.

## 📊 Метрики витрины

| Поле | Описание |
|------|----------|
| `avg_lead_time_days` | Средний фактический срок поставки |
| `avg_delay_days` | Средняя задержка относительно плана |
| `delayed_share_pct` | Доля поставок с задержкой, % |
| `defect_rate_pct` | Доля брака от объёма, % |
| `logistics_cost_per_unit` | Стоимость логистики на единицу |
| `supplier_score` | Интегральный скоринг (0–100) |

## 🧪 Тесты dbt

- `unique` / `not_null` на `supplier_id`
- `relationships` — все `supplier_id` из логистики есть в справочнике поставщиков
- `expression_is_true` — диапазоны процентов (0–100), неотрицательная стоимость
- Кастомный тест `assert_no_negative_cost` — защита от отрицательной стоимости логистики

Запуск:
```bash
docker compose run --rm dbt dbt test
```

## 📸 Скриншот дашборда

> *Замените этот блок на актуальный скриншот из Lightdash:*

![Lightdash dashboard](./docs/dashboard.png)

Дашборд содержит:
- **KPI-карточки**: топ-5 поставщиков по `supplier_score`, средняя задержка, средний брак.
- **Таблица-рейтинг** поставщиков с сортировкой по категории/стране.
- **Bar chart** «Стоимость логистики на единицу» по категориям.
- **Trend chart** по месяцам: объём поставок и доля задержек.

## 📂 Структура

```
├── docker-compose.yml
├── extract/               # ETL: PG + CSV -> ClickHouse
│   ├── load_to_clickhouse.py
│   ├── init_postgres.sql
│   └── data/logistics.csv
├── dbt_project/
│   ├── models/
│   │   ├── staging/       # stg_suppliers, stg_logistics + sources.yml
│   │   ├── intermediate/  # int_supplier_logistics
│   │   └── mart/          # mart_supplier_performance + schema.yml
│   └── tests/
└── README.md
```

## 🔄 Идемпотентность
- ETL делает `TRUNCATE` перед загрузкой — повторный запуск безопасен.
- `ReplacingMergeTree` в ClickHouse + `FINAL` в staging — корректная работа при повторных вставках.
- dbt-модели `view`/`table` пересоздаются при каждом `dbt run`.
🔑 Как запустить end-to-end
bash
cd supplier-logistics-mart
cp .env .env.example      # зафиксируйте шаблон
docker compose up -d postgres clickhouse
docker compose up loader
docker compose up dbt
docker compose up -d lightdash
Что демонстрирует проект (для портфолио):

Паттерн OLTP → OLAP (PostgreSQL → ClickHouse) через Python-ETL.

ReplacingMergeTree и FINAL для дедупликации.

Слои staging → intermediate → mart в dbt с sources.yml, schema.yml, тестами и документацией.

Lightdash — BI, работающий напрямую с dbt-моделями ClickHouse (без дублирования определений метрик).

Кастомные тесты (assert_no_negative_cost) и generic-тесты (dbt_utils.expression_is_true).

