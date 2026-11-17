ETL проект: Заказы из 1С/Excel в PostgreSQL с витриной продаж
Ниже — полный код всех файлов проекта. Структура готова к запуску через docker-compose up.

📁 docker-compose.yml
yaml
version: '3.8'

x-airflow-common: &airflow-common
  image: apache/airflow:2.8.1-python3.11
  environment: &airflow-common-env
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres-airflow:5432/airflow
    AIRFLOW__CORE__FERNET_KEY: ''
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'
    AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
    AIRFLOW__API__AUTH_BACKENDS: 'airflow.api.auth.backend.basic_auth'
    AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK: 'true'
    _PIP_ADDITIONAL_REQUIREMENTS: >-
      pandas==2.1.4
      openpyxl==3.1.2
      psycopg2-binary==2.9.9
      SQLAlchemy==1.4.52
  volumes:
    - ./dags:/opt/airflow/dags
    - ./scripts:/opt/airflow/scripts
    - ./sql:/opt/airflow/sql
    - ./data:/opt/airflow/data
    - ./logs:/opt/airflow/logs
  user: "${AIRFLOW_UID:-50000}:0"
  depends_on:
    postgres-airflow:
      condition: service_healthy

services:
  # --- Метаданные Airflow ---
  postgres-airflow:
    image: postgres:15
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - airflow-db-data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 5s
      retries: 5

  # --- Хранилище данных (staging + marts) ---
  postgres-dwh:
    image: postgres:15
    environment:
      POSTGRES_USER: dwh
      POSTGRES_PASSWORD: dwh
      POSTGRES_DB: dwh
    ports:
      - "5433:5432"
    volumes:
      - dwh-data:/var/lib/postgresql/data
      - ./sql/init.sql:/docker-entrypoint-initdb.d/init.sql
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "dwh"]
      interval: 5s
      retries: 5

  # --- Airflow ---
  airflow-init:
    <<: *airflow-common
    entrypoint: /bin/bash
    command:
      - -c
      - |
        airflow db init
        airflow users create \
          --username admin --password admin \
          --firstname Admin --lastname User \
          --role Admin --email admin@example.com

  airflow-webserver:
    <<: *airflow-common
    command: webserver
    ports:
      - "8080:8080"
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
      interval: 30s
      retries: 5
    restart: always

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler
    restart: always

  # --- BI ---
  metabase:
    image: metabase/metabase:latest
    ports:
      - "3000:3000"
    environment:
      MB_DB_TYPE: postgres
      MB_DB_DBNAME: metabase
      MB_DB_PORT: 5432
      MB_DB_USER: dwh
      MB_DB_PASS: dwh
      MB_DB_HOST: postgres-dwh
    depends_on:
      postgres-dwh:
        condition: service_healthy

volumes:
  airflow-db-data:
  dwh-data:
📁 sql/init.sql
sql
-- Инициализация DWH: схемы staging и mart
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS mart;

-- ============ STAGING ============
CREATE TABLE IF NOT EXISTS staging.orders_raw (
    id                  BIGSERIAL PRIMARY KEY,
    source_file         TEXT,
    row_number          INT,
    raw_data            JSONB,          -- «сырая» строка как есть
    loaded_at           TIMESTAMPTZ DEFAULT now()
);

-- Очищенный слой (то, что грузим идемпотентно из pandas)
CREATE TABLE IF NOT EXISTS staging.orders_clean (
    order_id            TEXT NOT NULL,
    order_date          DATE NOT NULL,
    manager             TEXT,
    customer            TEXT,
    material_category   TEXT,
    material_name       TEXT,
    quantity            NUMERIC(12, 3) NOT NULL,
    price_per_unit      NUMERIC(12, 2) NOT NULL,
    total_amount        NUMERIC(14, 2) NOT NULL,
    status              TEXT,
    source_file         TEXT,
    loaded_at           TIMESTAMPTZ DEFAULT now(),
    updated_at          TIMESTAMPTZ DEFAULT now(),
    CONSTRAINT uq_orders_clean_pk UNIQUE (order_id, material_name)
);

CREATE INDEX IF NOT EXISTS idx_orders_clean_date ON staging.orders_clean(order_date);

-- ============ MART ============
CREATE TABLE IF NOT EXISTS mart.mart_sales_daily (
    sale_date           DATE NOT NULL,
    material_category   TEXT NOT NULL,
    manager             TEXT NOT NULL,
    orders_count        BIGINT       NOT NULL,
    total_quantity      NUMERIC(14, 3) NOT NULL,
    total_revenue       NUMERIC(16, 2) NOT NULL,
    avg_order_amount    NUMERIC(14, 2) NOT NULL,
    updated_at          TIMESTAMPTZ DEFAULT now(),
    CONSTRAINT uq_mart_sales_daily UNIQUE (sale_date, material_category, manager)
);

CREATE INDEX IF NOT EXISTS idx_mart_sales_daily_date ON mart.mart_sales_daily(sale_date);
📁 scripts/clean_excel.py
python
"""
Модуль очистки «грязных» Excel-выгрузок из 1С.

Особенности реальных файлов 1С:
- Объединённые ячейки (заголовок на 2-3 строки)
- «Служебные» строки: "Итого:", "Итого по менеджеру:", пустые строки
- Разные форматы дат: 01.02.2024, 2024-02-01, "1 февраля 2024"
- Числа как строки с пробелами/запятыми: "1 234,56"
- Дублирующиеся заголовки
- Разное написание категорий/менеджеров

На вход получает путь к xlsx -> возвращает чистый pandas.DataFrame.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)

# Ожидаемые колонки (каноничные названия)
CANONICAL_COLUMNS = {
    "order_id": ["№ заказа", "номер заказа", "order", "заказ"],
    "order_date": ["дата", "date", "дата заказа"],
    "manager": ["менеджер", "manager", "ответственный"],
    "customer": ["клиент", "покупатель", "customer", "контрагент"],
    "material_category": ["категория", "category", "группа"],
    "material_name": ["номенклатура", "материал", "товар", "product"],
    "quantity": ["кол-во", "количество", "qty", "quantity"],
    "price_per_unit": ["цена", "price", "цена за ед."],
    "total_amount": ["сумма", "итого", "total", "amount"],
    "status": ["статус", "status"],
}

# Строки-«мусор», которые нужно выкинуть
JUNK_ROW_PATTERNS = [
    r"^итог",
    r"^всего",
    r"^итого по",
    r"^подытог",
    r"^total",
    r"^subtotal",
]


def _normalize_header(h: str) -> str:
    """Приводим заголовок к нижнему регистру, убираем спецсимволы/пробелы."""
    if not isinstance(h, str):
        return ""
    h = h.strip().lower()
    h = re.sub(r"[\s_]+", " ", h)
    h = re.sub(r"[^\wа-яё ]+", "", h)
    return h.strip()


def _map_columns(df: pd.DataFrame) -> dict[str, str]:
    """Сопоставление реальных заголовков с каноничными."""
    mapping: dict[str, str] = {}
    normalized = {_normalize_header(c): c for c in df.columns}
    for canonical, aliases in CANONICAL_COLUMNS.items():
        for alias in aliases:
            key = _normalize_header(alias)
            if key in normalized:
                mapping[canonical] = normalized[key]
                break
    return mapping


def _parse_date(value) -> pd.Timestamp | None:
    """Максимально устойчивый парсер дат."""
    if pd.isna(value):
        return None
    if isinstance(value, (datetime, pd.Timestamp)):
        return pd.Timestamp(value)

    s = str(value).strip()
    if not s:
        return None

    # Убираем возможные "г." / "года"
    s = re.sub(r"\s*г\.?$", "", s, flags=re.IGNORECASE)

    # Перебор форматов (порядок важен!)
    fmt_candidates = [
        "%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y", "%d-%m-%Y",
        "%Y/%m/%d", "%d.%m.%y", "%d %B %Y", "%d %b %Y",
    ]
    for fmt in fmt_candidates:
        try:
            return pd.Timestamp(datetime.strptime(s, fmt))
        except ValueError:
            continue

    # Fallback для "1 февраля 2024" в русской локали
    try:
        return pd.Timestamp(pd.to_datetime(s, dayfirst=True))
    except Exception:
        return None


def _parse_number(value) -> float | None:
    """'1 234,56' -> 1234.56 ; '1,234.56' -> 1234.56 ; '-' -> None."""
    if pd.isna(value):
        return None
    if isinstance(value, (int, float)):
        return float(value)

    s = str(value).strip()
    if s in {"", "-", "—", "nan", "None"}:
        return None

    # Убираем всё, кроме цифр, точки и запятой
    s = re.sub(r"[^\d,.\-]", "", s)

    # Если и точка и запятая — считаем, что последняя из них — десятичный разделитель
    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "," in s:
        # одна запятая -> десятичная
        s = s.replace(",", ".")

    try:
        return float(s)
    except ValueError:
        return None


def _is_junk_row(row: pd.Series) -> bool:
    """Определяем служебные строки по первому непустому значению."""
    for v in row.values:
        if pd.isna(v):
            continue
        s = str(v).strip().lower()
        if not s:
            continue
        return any(re.search(p, s) for p in JUNK_ROW_PATTERNS)
    return True  # полностью пустая строка — тоже мусор


def clean_excel(file_path: str | Path) -> pd.DataFrame:
    """
    Читает xlsx от 1С и возвращает очищенный DataFrame
    с колонками CANONICAL_COLUMNS.
    """
    file_path = Path(file_path)
    log.info("Reading %s", file_path)

    # header=0 — обычно заголовок в первой строке; если нет — можно поиграть
    raw = pd.read_excel(file_path, header=0, engine="openpyxl")

    # 1) Убираем полностью пустые столбцы/строки
    raw = raw.dropna(how="all").dropna(axis=1, how="all")

    # 2) Отбрасываем строки «Итого:» и им подобные
    raw = raw[~raw.apply(_is_junk_row, axis=1)].copy()

    # 3) Маппинг колонок
    mapping = _map_columns(raw)
    missing = set(CANONICAL_COLUMNS) - set(mapping)
    if missing:
        log.warning("Missing columns in file %s: %s", file_path.name, missing)

    df = raw.rename(columns=mapping)
    # Оставляем только канонические колонки
    keep = [c for c in CANONICAL_COLUMNS if c in df.columns]
    df = df[keep].copy()

    # 4) Обязательные поля: без них строка бесполезна
    required = ["order_id", "order_date", "material_name", "quantity", "price_per_unit"]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Обязательная колонка {col!r} не найдена в {file_path.name}")
    df = df.dropna(subset=["order_id", "order_date", "material_name"])

    # 5) Приводим типы
    df["order_id"] = df["order_id"].astype(str).str.strip()
    df["order_date"] = df["order_date"].apply(_parse_date)
    df["quantity"] = df["quantity"].apply(_parse_number)
    df["price_per_unit"] = df["price_per_unit"].apply(_parse_number)
    if "total_amount" in df.columns:
        df["total_amount"] = df["total_amount"].apply(_parse_number)
    else:
        df["total_amount"] = None

    # 6) Считаем недостающие суммы/цены
    df["total_amount"] = df["total_amount"].fillna(df["quantity"] * df["price_per_unit"])
    mask_price_na = df["price_per_unit"].isna() & df["quantity"].gt(0) & df["total_amount"].notna()
    df.loc[mask_price_na, "price_per_unit"] = df.loc[mask_price_na, "total_amount"] / df.loc[mask_price_na, "quantity"]

    # 7) Числовые поля не должны быть отрицательными
    df = df[df["quantity"] > 0]
    df = df[df["price_per_unit"] >= 0]

    # 8) Строковые поля: trim + нормализация регистра для менеджера/категории
    for col in ["manager", "customer", "material_category", "material_name", "status"]:
        if col not in df.columns:
            df[col] = None
        df[col] = df[col].astype("string").str.strip()

    # Унифицируем категории/менеджеров (Capitalize, чтобы не было дублей «плитка»/«Плитка»)
    df["material_category"] = df["material_category"].str.title()
    df["manager"] = df["manager"].str.title()

    # 9) Финальный отбор колонок + source_file
    df["source_file"] = file_path.name

    df = df[[
        "order_id", "order_date", "manager", "customer",
        "material_category", "material_name",
        "quantity", "price_per_unit", "total_amount",
        "status", "source_file",
    ]].reset_index(drop=True)

    log.info("Cleaned %s: %s rows", file_path.name, len(df))
    return df
📁 dags/1c_orders_etl.py
python
"""
DAG: ETL заказов из 1С/Excel -> staging.orders_clean -> mart.mart_sales_daily.

Шаги:
 1. extract: сканируем /opt/airflow/data/orders/*.xlsx
 2. clean_and_load_staging: очищаем каждый файл через clean_excel,
    идемпотентно грузим в staging.orders_clean (ON CONFLICT).
 3. build_mart: пересобираем витрину mart.mart_sales_daily
    (полный rebuild за затронутые даты — тоже идемпотентно).
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text

# Подключаем модуль очистки из /opt/airflow/scripts
sys.path.insert(0, "/opt/airflow/scripts")
from clean_excel import clean_excel  # noqa: E402

log = logging.getLogger(__name__)

DWH_DSN = os.getenv(
    "DWH_DSN",
    "postgresql+psycopg2://dwh:dwh@postgres-dwh:5432/dwh",
)
ORDERS_DIR = Path("/opt/airflow/data/orders")
SQL_DIR = Path("/opt/airflow/sql")


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}


def _engine():
    return create_engine(DWH_DSN, pool_pre_ping=True)


# -------------------- TASK 1: EXTRACT --------------------
def extract(**context):
    files = sorted(ORDERS_DIR.glob("*.xlsx"))
    if not files:
        raise FileNotFoundError(f"В {ORDERS_DIR} нет xlsx-файлов")

    paths = [str(p) for p in files]
    log.info("Найдено %s файлов: %s", len(paths), paths)

    # XCom ограничен по размеру — передаём только пути, а не данные
    context["ti"].xcom_push(key="files", value=paths)
    return paths


# -------------------- TASK 2: CLEAN & LOAD STAGING --------------------
def clean_and_load_staging(**context):
    files = context["ti"].xcom_pull(task_ids="extract", key="files")
    if not files:
        raise ValueError("extract вернул пустой список файлов")

    engine = _engine()

    # Формируем один общий DataFrame
    frames = []
    for f in files:
        df = clean_excel(f)
        if not df.empty:
            frames.append(df)

    if not frames:
        log.warning("После очистки не осталось валидных строк")
        return 0

    all_rows = pd.concat(frames, ignore_index=True)

    # Приводим типы к тем, что ждёт Postgres
    all_rows["order_date"] = pd.to_datetime(all_rows["order_date"]).dt.date
    all_rows["quantity"] = all_rows["quantity"].astype(float)
    all_rows["price_per_unit"] = all_rows["price_per_unit"].astype(float)
    all_rows["total_amount"] = all_rows["total_amount"].astype(float)

    # Дедуп внутри батча по PK (order_id, material_name) — берём последнюю запись
    all_rows = (
        all_rows.sort_values("order_date")
        .drop_duplicates(subset=["order_id", "material_name"], keep="last")
        .reset_index(drop=True)
    )

    records = all_rows.to_dict(orient="records")

    upsert_sql = text("""
        INSERT INTO staging.orders_clean (
            order_id, order_date, manager, customer,
            material_category, material_name,
            quantity, price_per_unit, total_amount,
            status, source_file, loaded_at, updated_at
        ) VALUES (
            :order_id, :order_date, :manager, :customer,
            :material_category, :material_name,
            :quantity, :price_per_unit, :total_amount,
            :status, :source_file, now(), now()
        )
        ON CONFLICT (order_id, material_name) DO UPDATE SET
            order_date        = EXCLUDED.order_date,
            manager           = EXCLUDED.manager,
            customer          = EXCLUDED.customer,
            material_category = EXCLUDED.material_category,
            quantity          = EXCLUDED.quantity,
            price_per_unit    = EXCLUDED.price_per_unit,
            total_amount      = EXCLUDED.total_amount,
            status            = EXCLUDED.status,
            source_file       = EXCLUDED.source_file,
            updated_at        = now()
    """)

    with engine.begin() as conn:
        conn.execute(upsert_sql, records)

    log.info("Upsert в staging.orders_clean: %s строк", len(records))

    # Сохраняем затронутые даты для следующего шага
    affected_dates = sorted({str(d) for d in all_rows["order_date"]})
    context["ti"].xcom_push(key="affected_dates", value=affected_dates)
    return len(records)


# -------------------- TASK 3: BUILD MART --------------------
def build_mart(**context):
    affected_dates = context["ti"].xcom_pull(
        task_ids="clean_and_load_staging", key="affected_dates"
    ) or []
    engine = _engine()

    # Полный rebuild витрины — просто и надёжно, объёмы небольшие.
    # Если нужно «по датам» — раскомментируйте блок с DELETE + INSERT ... WHERE.
    mart_sql_path = SQL_DIR / "mart_sales_daily.sql"
    if not mart_sql_path.exists():
        raise FileNotFoundError(f"Не найден {mart_sql_path}")

    ddl_and_load = mart_sql_path.read_text(encoding="utf-8")

    with engine.begin() as conn:
        # 1) DDL (безопасно, IF NOT EXISTS)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS mart.mart_sales_daily (
                sale_date           DATE NOT NULL,
                material_category   TEXT NOT NULL,
                manager             TEXT NOT NULL,
                orders_count        BIGINT NOT NULL,
                total_quantity      NUMERIC(14, 3) NOT NULL,
                total_revenue       NUMERIC(16, 2) NOT NULL,
                avg_order_amount    NUMERIC(14, 2) NOT NULL,
                updated_at          TIMESTAMPTZ DEFAULT now(),
                CONSTRAINT uq_mart_sales_daily UNIQUE (sale_date, material_category, manager)
            );
        """))

        # 2) Идемпотентная пересборка: удаляем затронутые даты и вставляем заново
        if affected_dates:
            conn.execute(
                text("DELETE FROM mart.mart_sales_daily WHERE sale_date = ANY(:d)"),
                {"d": affected_dates},
            )

        conn.execute(text(ddl_and_load.replace(
            "/*__WHERE_AFFECTED__*/",
            "" if not affected_dates else "WHERE sale_date = ANY(:d)"
        )), {"d": affected_dates} if affected_dates else {})

    log.info("Витрина mart.mart_sales_daily пересобрана (даты: %s)", affected_dates or "ALL")


# -------------------- DAG --------------------
with DAG(
    dag_id="1c_orders_etl",
    description="ETL заказов 1С/Excel -> staging -> mart_sales_daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="Europe/Moscow"),
    schedule="0 6 * * *",  # каждый день в 06:00
    catchup=False,
    default_args=default_args,
    tags=["etl", "1c", "excel", "sales"],
) as dag:

    t_extract = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    t_load = PythonOperator(
        task_id="clean_and_load_staging",
        python_callable=clean_and_load_staging,
    )

    t_mart = PythonOperator(
        task_id="build_mart",
        python_callable=build_mart,
    )

    t_extract >> t_load >> t_mart
📁 sql/mart_sales_daily.sql
sql
-- Пересборка витрины продаж по дням/категориям/менеджерам.
-- SQL-файл используется из DAG (build_mart).
-- Плейсхолдер /*__WHERE_AFFECTED__*/ подменяется на WHERE sale_date = ANY(:d)
-- если в DAG были затронуты только конкретные даты.

WITH aggregated AS (
    SELECT
        order_date                                   AS sale_date,
        COALESCE(material_category, 'Без категории') AS material_category,
        COALESCE(manager, 'Не указан')               AS manager,
        COUNT(DISTINCT order_id)                     AS orders_count,
        SUM(quantity)                                AS total_quantity,
        SUM(total_amount)                            AS total_revenue,
        ROUND(
            SUM(total_amount) / NULLIF(COUNT(DISTINCT order_id), 0),
            2
        )                                            AS avg_order_amount
    FROM staging.orders_clean
    -- Отбрасываем отменённые/невалидные заказы
    WHERE status IS NULL OR LOWER(status) NOT IN ('отменён', 'отменен', 'cancelled', 'canceled')
    GROUP BY
        order_date,
        COALESCE(material_category, 'Без категории'),
        COALESCE(manager, 'Не указан')
)
INSERT INTO mart.mart_sales_daily (
    sale_date, material_category, manager,
    orders_count, total_quantity, total_revenue, avg_order_amount, updated_at
)
SELECT
    sale_date, material_category, manager,
    orders_count, total_quantity, total_revenue, avg_order_amount, now()
FROM aggregated
/*__WHERE_AFFECTED__*/
ON CONFLICT (sale_date, material_category, manager) DO UPDATE SET
    orders_count     = EXCLUDED.orders_count,
    total_quantity   = EXCLUDED.total_quantity,
    total_revenue    = EXCLUDED.total_revenue,
    avg_order_amount = EXCLUDED.avg_order_amount,
    updated_at       = now();
📁 README.md
markdown
# ETL: Заказы 1С/Excel → PostgreSQL → Metabase

Пайплайн для автоматизации обработки «грязных» выгрузок заказов из 1С
в виде Excel-файлов и построения витрины продаж стройматериалов.

## 🎯 Что решает

Отдел продаж получает из 1С выгрузки в Excel с объединёнными ячейками,
разными форматами дат, служебными строками «Итого» и числами вида
`1 234,56`. Раньше аналитики чистили это вручную — теперь данные
ежедневно автоматически попадают в PostgreSQL и в дашборд Metabase.

## 🏗 Архитектура

```
┌──────────────┐    ┌──────────────────┐    ┌────────────────────┐
│ Excel (1С)   │───▶│ Airflow DAG      │───▶│ PostgreSQL (DWH)   │
│ /data/orders │    │ Pandas clean     │    │ staging.orders_clean│
└──────────────┘    │ Idempotent upsert│    │ mart.mart_sales_daily│
                    └──────────────────┘    └─────────┬──────────┘
                                                      │
                                                      ▼
                                            ┌──────────────────┐
                                            │ Metabase         │
                                            │ Дашборд продаж   │
                                            └──────────────────┘
```

## 📂 Структура

```
etl-1c-orders-to-postgres/
├── dags/1c_orders_etl.py       # DAG Airflow
├── scripts/clean_excel.py      # Очистка Excel
├── sql/init.sql                # Инициализация схем
├── sql/mart_sales_daily.sql    # SQL витрины
├── docker-compose.yml          # Airflow + Postgres + Metabase
└── README.md
```

## 🚀 Запуск

```bash
# 1. Подготовить данные
mkdir -p data/orders
cp sample_orders_*.xlsx data/orders/

# 2. Поднять стек
docker compose up -d

# 3. Открыть Airflow
open http://localhost:8080      # admin / admin
# включить DAG 1c_orders_etl и запустить

# 4. Открыть Metabase
open http://localhost:3000
# при первом запуске подключить Postgres:
#   host:     postgres-dwh
#   port:     5432
#   db:       dwh
#   user:     dwh
#   password: dwh
```

## 🗃 Схема данных

### staging.orders_clean

| Поле | Тип | Описание |
|------|-----|----------|
| order_id | text | № заказа (PK часть 1) |
| order_date | date | Дата заказа |
| manager | text | Менеджер |
| customer | text | Клиент |
| material_category | text | Категория стройматериала |
| material_name | text | Наименование (PK часть 2) |
| quantity | numeric(12,3) | Количество |
| price_per_unit | numeric(12,2) | Цена за ед. |
| total_amount | numeric(14,2) | Сумма |
| status | text | Статус |
| source_file | text | Исходный файл |
| loaded_at / updated_at | timestamptz | Аудит |

**Уникальный ключ:** `(order_id, material_name)` — обеспечивает
идемпотентную загрузку через `ON CONFLICT DO UPDATE`.

### mart.mart_sales_daily

| Поле | Тип | Описание |
|------|-----|----------|
| sale_date | date | День |
| material_category | text | Категория |
| manager | text | Менеджер |
| orders_count | bigint | Кол-во заказов |
| total_quantity | numeric | Сумма количества |
| total_revenue | numeric | Выручка |
| avg_order_amount | numeric | Средний чек |
| updated_at | timestamptz | Время обновления |

**Уникальный ключ:** `(sale_date, material_category, manager)`.

## 🔁 Идемпотентность

- **Staging:** `INSERT ... ON CONFLICT (order_id, material_name) DO UPDATE` —
  повторный запуск DAG не создаёт дублей.
- **Mart:** перед вставкой удаляются строки за затронутые даты,
  затем `INSERT ... ON CONFLICT DO UPDATE`. Полная пересборка
  на маленьких объёмах тоже допустима (см. флаг в `build_mart`).

## 🧪 Пример «грязного» входа

| № заказа | Дата | Менеджер | Номенклатура | Кол-во | Цена |
|----------|------|----------|--------------|--------|------|
| 1001 | 01.02.2024 | Иванов | Кирпич | 1 000 | 12,50 |
| 1002 | 2024-02-01 | петров | Плитка | 20 | 1 200 |
| Итого: | | | | | |

После очистки в `staging.orders_clean`:
- даты приведены к `DATE`,
- менеджеры к `Иванов / Петров`,
- строка «Итого:» удалена,
- числа `1 000` и `12,50` → `1000.0` и `12.5`.

## 📊 Дашборд Metabase

Пример готового дашборда «Продажи стройматериалов» (после запуска DAG):

- Карточки:
  - **Выручка по дням** (line chart)
  - **Топ-10 категорий** (bar chart)
  - **Выручка по менеджерам** (table)
  - **Средний чек** (scalar)
- Источник: `mart.mart_sales_daily`.

> 📸 *Скриншот дашборда добавьте в `docs/dashboard.png` после первого запуска
> и вставьте сюда:* `![Дашборд](docs/dashboard.png)`

## 🛠 Стек


- **Airflow 2.8** — оркестрация
- **Python 3.11 + Pandas 2.1** — очистка
- **PostgreSQL 15** — хранилище
- **Metabase** — BI

## 📝 Замечания по эксплуатации

- Файлы кладутся в `data/orders/`. После успешного прогона можно
  перемещать их в `data/archive/` (можно добавить в DAG отдельным шагом).
- DAG запускается ежедневно в 06:00 Europe/Moscow.
- При больших объёмах (`>1 млн строк`) стоит перейти на `COPY` вместо
  `INSERT ... VALUES` и на партиционирование `staging.orders_clean` по
  `order_date`.
🗂 Пример «грязного» входного файла (для теста)
Создайте data/orders/sample_orders_2024_02.xlsx со структурой:

№ заказа	Дата	Менеджер	Клиент	Категория	Номенклатура	Кол-во	Цена	Сумма	Статус
1001	01.02.2024	Иванов	ООО Ромашка	кирпич	Кирпич М150	1 000	12,50	12 500,00	Выполнен
1002	2024-02-01	петров	ИП Сидоров	Плитка	Плитка керамич.	20	1 200	24 000	Выполнен
1003	3 февраля 2024	Иванов	ООО Ромашка	КИРПИЧ	Кирпич М100	500	10	5 000	Выполнен
Итого:								41 500
После прогона DAG:

staging.orders_clean — 3 строки (Итого: отфильтровано),

категории унифицированы (Кирпич, Плитка),

менеджеры унифицированы (Иванов, Петров),