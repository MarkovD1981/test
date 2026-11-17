Проект 3: Стриминг остатков со складов в ClickHouse для real-time дашборда
Бизнес-проблема: У компании несколько складов строительных материалов. Менеджеры хотят видеть актуальные остатки (цемент, кирпич, арматура) в реальном времени, чтобы не продавать то, чего нет, и быстро реагировать на всплески спроса.

Архитектура и стек:

Источник: Генератор событий (Python) имитирует изменения остатков на складах (приход/расход).

Стриминг: Apache Kafka (Docker) → ClickHouse (Kafka Engine или прямой insert через Python).

Хранилище: ClickHouse (таблица stock_movements с движком ReplacingMergeTree для дедупликации).

Витрина: Материализованное представление в ClickHouse для агрегации текущих остатков по складам и SKU.

Визуализация: Grafana (плагин ClickHouse) для real-time мониторинга остатков.

Что демонстрирует: Навык работы с потоковыми данными, ClickHouse (движки таблиц, материализованные представления), интеграция Kafka → ClickHouse, визуализация временных рядов в Grafana .

Структура репозитория:


text
/realtime-stock-clickhouse
├── producer/stock_producer.py
├── clickhouse/init.sql (Kafka Engine + MV)
├── docker-compose.yml (Kafka + ClickHouse + Grafana)
└── README.md (описание потока данных, скриншот Grafana)
       напиши код всех файлов этого проекта
Проект: Realtime Stock ClickHouse
Ниже — полный код всех файлов проекта.

📁 docker-compose.yml
yaml
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    container_name: zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"
    networks:
      - stock-net

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    container_name: kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
      - "29092:29092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    networks:
      - stock-net
    healthcheck:
      test: ["CMD", "kafka-broker-api-versions", "--bootstrap-server", "localhost:9092"]
      interval: 10s
      timeout: 10s
      retries: 10

  clickhouse:
    image: clickhouse/clickhouse-server:23.8
    container_name: clickhouse
    ports:
      - "8123:8123"   # HTTP
      - "9000:9000"   # Native
    ulimits:
      nofile:
        soft: 262144
        hard: 262144
    environment:
      CLICKHOUSE_DB: stock
      CLICKHOUSE_USER: default
      CLICKHOUSE_PASSWORD: ""
      CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT: 1
    volumes:
      - ./clickhouse/init.sql:/docker-entrypoint-initdb.d/init.sql
      - clickhouse-data:/var/lib/clickhouse
    networks:
      - stock-net
    healthcheck:
      test: ["CMD", "wget", "--spider", "-q", "http://localhost:8123/ping"]
      interval: 10s
      timeout: 5s
      retries: 10

  grafana:
    image: grafana/grafana:10.2.0
    container_name: grafana
    depends_on:
      - clickhouse
    ports:
      - "3000:3000"
    environment:
      GF_SECURITY_ADMIN_USER: admin
      GF_SECURITY_ADMIN_PASSWORD: admin
      GF_INSTALL_PLUGINS: grafana-clickhouse-datasource
      GF_USERS_ALLOW_SIGN_UP: "false"
    volumes:
      - grafana-data:/var/lib/grafana
    networks:
      - stock-net

  producer:
    build:
      context: ./producer
      dockerfile: Dockerfile
    container_name: stock_producer
    depends_on:
      kafka:
        condition: service_healthy
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:29092
      KAFKA_TOPIC: stock_movements
      EVENTS_PER_SECOND: "3"
    networks:
      - stock-net
    restart: unless-stopped

networks:
  stock-net:
    driver: bridge

volumes:
  clickhouse-data:
  grafana-data:
📁 clickhouse/init.sql
sql
-- =========================================================
-- 1. Создаём БД
-- =========================================================
CREATE DATABASE IF NOT EXISTS stock;

-- =========================================================
-- 2. Kafka Engine: читаем сообщения из топика stock_movements
--    Формат JSONEachRow, одно сообщение = одна строка
-- =========================================================
CREATE TABLE IF NOT EXISTS stock.stock_movements_kafka
(
    event_id      String,
    warehouse_id  UInt32,
    sku           String,
    quantity      Int32,        -- + приход, - расход
    event_time    DateTime,
    operation     String        -- 'inbound' | 'outbound'
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'stock_movements',
    kafka_group_name = 'clickhouse_stock_consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 2,
    kafka_max_block_size = 1000,
    kafka_poll_timeout_ms = 1000,
    kafka_skip_broken_messages = 100;

-- =========================================================
-- 3. Целевая таблица с ReplacingMergeTree
--    Дедупликация по event_id: если событие придёт дважды,
--    ReplacingMergeTree оставит последнюю версию
-- =========================================================
CREATE TABLE IF NOT EXISTS stock.stock_movements
(
    event_id      String,
    warehouse_id  UInt32,
    sku           String,
    quantity      Int32,
    event_time    DateTime,
    operation     LowCardinality(String),
    ingested_at   DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_id, event_time);

-- =========================================================
-- 4. Materialized View: Kafka -> stock_movements
--    Просто переливает события из Kafka в целевую таблицу
-- =========================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS stock.mv_kafka_to_movements
TO stock.stock_movements
AS
SELECT
    event_id,
    warehouse_id,
    sku,
    quantity,
    event_time,
    operation,
    now() AS ingested_at
FROM stock.stock_movements_kafka;

-- =========================================================
-- 5. Витрина: агрегированный текущий остаток
--    Считается как сумма всех quantity по (warehouse_id, sku)
--    Хранится как AggregatingMergeTree для инкрементального обновления
-- =========================================================
CREATE TABLE IF NOT EXISTS stock.stock_balance_agg
(
    warehouse_id  UInt32,
    sku           String,
    total_quantity AggregateFunction(sum, Int32),
    last_event_at  SimpleAggregateFunction(max, DateTime)
)
ENGINE = AggregatingMergeTree()
ORDER BY (warehouse_id, sku);

-- =========================================================
-- 6. Materialized View: movements -> stock_balance_agg
--    Каждое новое событие инкрементально обновляет баланс
-- =========================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS stock.mv_movements_to_balance
TO stock.stock_balance_agg
AS
SELECT
    warehouse_id,
    sku,
    sumState(quantity)        AS total_quantity,
    maxSimpleState(event_time) AS last_event_at
FROM stock.stock_movements
GROUP BY warehouse_id, sku;

-- =========================================================
-- 7. Обычное (non-materialized) представление — удобно
--    читать итоговые остатки (мерджит агрегаты)
-- =========================================================
CREATE VIEW IF NOT EXISTS stock.stock_balance_current AS
SELECT
    warehouse_id,
    sku,
    sumMerge(total_quantity) AS quantity,
    maxMerge(last_event_at)  AS last_event_at
FROM stock.stock_balance_agg
GROUP BY warehouse_id, sku;

-- =========================================================
-- 8. Тестовая таблица для ручной проверки (опционально)
-- =========================================================
CREATE TABLE IF NOT EXISTS stock.raw_events
(
    event_id      String,
    warehouse_id  UInt32,
    sku           String,
    quantity      Int32,
    event_time    DateTime,
    operation     LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, event_id);
📁 producer/stock_producer.py
python
#!/usr/bin/env python3
"""
Генератор событий изменения остатков на складах.
Публикует JSON-сообщения в Kafka-топик stock_movements.
"""

import json
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


# ---------- Конфигурация ----------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock_movements")
EVENTS_PER_SECOND = float(os.getenv("EVENTS_PER_SECOND", "3"))

# Справочники
WAREHOUSES = [
    {"id": 1, "name": "Склад Москва-Север"},
    {"id": 2, "name": "Склад Москва-Юг"},
    {"id": 3, "name": "Склад Санкт-Петербург"},
    {"id": 4, "name": "Склад Екатеринбург"},
]

SKUS = [
    {"sku": "CEMENT-M500-50KG", "name": "Цемент М500, 50 кг"},
    {"sku": "CEMENT-M400-50KG", "name": "Цемент М400, 50 кг"},
    {"sku": "BRICK-RED-STD",    "name": "Кирпич красный полнотелый"},
    {"sku": "BRICK-WHITE-SIL",  "name": "Кирпич силикатный белый"},
    {"sku": "REBAR-A500-12MM",  "name": "Арматура А500С 12 мм"},
    {"sku": "REBAR-A500-16MM",  "name": "Арматура А500С 16 мм"},
    {"sku": "SAND-QUARRY-1T",   "name": "Песок карьерный, 1 т"},
    {"sku": "GRAVEL-5-20-1T",   "name": "Щебень 5-20, 1 т"},
]

# Диапазоны объёмов (в единицах учёта)
QTY_RANGE = {
    "inbound":  (10, 200),
    "outbound": (1, 80),
}


_stop = False


def _handle_signal(signum, frame):
    global _stop
    _stop = True
    print(f"[producer] Получен сигнал {signum}, завершаемся...")


def connect_producer() -> KafkaProducer:
    """Подключается к Kafka с ретраями."""
    for attempt in range(1, 31):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=5,
                linger_ms=20,
                compression_type="gzip",
            )
            print(f"[producer] Подключён к Kafka: {KAFKA_BOOTSTRAP}")
            return producer
        except NoBrokersAvailable:
            print(f"[producer] Kafka недоступна, попытка {attempt}/30, ждём 2с...")
            time.sleep(2)
    raise RuntimeError("Не удалось подключиться к Kafka")


def generate_event() -> dict:
    """Генерирует одно событие прихода/расхода."""
    warehouse = random.choice(WAREHOUSES)
    sku = random.choice(SKUS)
    operation = random.choices(["inbound", "outbound"], weights=[0.45, 0.55])[0]

    lo, hi = QTY_RANGE[operation]
    qty = random.randint(lo, hi)
    signed_qty = qty if operation == "inbound" else -qty

    return {
        "event_id":      str(uuid.uuid4()),
        "warehouse_id":  warehouse["id"],
        "sku":           sku["sku"],
        "quantity":      signed_qty,
        "event_time":    datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "operation":     operation,
    }


def main():
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    print(f"[producer] Старт. Kafka={KAFKA_BOOTSTRAP} topic={KAFKA_TOPIC} "
          f"rate={EVENTS_PER_SECOND} events/sec")

    producer = connect_producer()
    interval = 1.0 / max(EVENTS_PER_SECOND, 0.1)
    sent = 0
    t0 = time.time()

    try:
        while not _stop:
            event = generate_event()

            # Ключ = warehouse_id:sku — гарантирует порядок
            # событий для конкретного товара на конкретном складе
            key = f"{event['warehouse_id']}:{event['sku']}"

            producer.send(KAFKA_TOPIC, key=key, value=event)
            sent += 1

            if sent % 50 == 0:
                elapsed = time.time() - t0
                print(f"[producer] отправлено={sent} "
                      f"rate={sent/elapsed:.1f} ev/s")

            time.sleep(interval)
    finally:
        print("[producer] flush()...")
        producer.flush(timeout=10)
        producer.close(timeout=10)
        print(f"[producer] Остановлен. Всего отправлено: {sent}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[producer] FATAL: {e}", file=sys.stderr)
        sys.exit(1)
📁 producer/Dockerfile
dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY stock_producer.py .

CMD ["python", "-u", "stock_producer.py"]
📁 producer/requirements.txt
txt
kafka-python==2.0.2
📁 grafana/provisioning/datasources/clickhouse.yml (опционально, авто-подключение)
yaml
apiVersion: 1

datasources:
  - name: ClickHouse
    type: grafana-clickhouse-datasource
    access: proxy
    url: http://clickhouse:8123
    isDefault: true
    jsonData:
      host: clickhouse
      port: 9000
      protocol: native
      defaultDatabase: stock
      username: default
    secureJsonData:
      password: ""
    editable: true
Если добавите этот файл — смонтируйте его в grafana:

yaml
volumes:
  - grafana-data:/var/lib/grafana
  - ./grafana/provisioning:/etc/grafana/provisioning
📁 README.md
markdown
# Realtime Stock ClickHouse

Стриминг остатков со складов строительных материалов в ClickHouse
для real-time дашборда в Grafana.

## Бизнес-проблема

У компании несколько складов (цемент, кирпич, арматура). Менеджеры
хотят видеть **актуальные остатки в реальном времени**, чтобы:

- не продавать то, чего нет;
- быстро реагировать на всплески спроса;
- видеть, какой склад что отгружает прямо сейчас.

## Архитектура

```
Python producer ──► Kafka (stock_movements) ──► ClickHouse Kafka Engine
                                                     │
                                                     ▼
                                          stock.stock_movements
                                          (ReplacingMergeTree,
                                           дедуп по event_id)
                                                     │
                                                     ▼
                                     Materialized View mv_movements_to_balance
                                                     │
                                                     ▼
                                        stock.stock_balance_agg
                                        (AggregatingMergeTree)
                                                     │
                                                     ▼
                                        stock.stock_balance_current (VIEW)
                                                     │
                                                     ▼
                                                  Grafana
```

## Компоненты

| Компонент | Назначение |
|-----------|-----------|
| `producer/stock_producer.py` | Генерирует события прихода/расхода (~3/сек) и шлёт в Kafka |
| Kafka | Топик `stock_movements`, ключ `warehouse_id:sku` |
| ClickHouse Kafka Engine | Читает топик, батчами вливает в целевую таблицу |
| `stock_movements` | ReplacingMergeTree, дедуп по `event_id` |
| `stock_balance_agg` | AggregatingMergeTree, инкрементальный агрегат |
| `stock_balance_current` | VIEW — «слитый» актуальный остаток |
| Grafana | Дашборд с таблицей и графиком остатков |

## Быстрый старт

```bash
docker compose up -d --build
```

Проверить, что сервисы живы:

```bash
docker compose ps
docker compose logs -f producer
```

- ClickHouse HTTP: http://localhost:8123/play
- Grafana: http://localhost:3000 (admin / admin)

## Проверка данных в ClickHouse

```bash
# в контейнере clickhouse
docker exec -it clickhouse clickhouse-client --database stock

# сколько событий долетело
SELECT count() FROM stock.stock_movements;

# актуальные остатки
SELECT * FROM stock.stock_balance_current
ORDER BY warehouse_id, sku
LIMIT 20;

# топ-10 SKU по суммарному остатку
SELECT sku, sum(quantity) AS total
FROM stock.stock_balance_current
GROUP BY sku
ORDER BY total DESC
LIMIT 10;
```

## Настройка Grafana

1. Откройте http://localhost:3000, войдите `admin` / `admin`.
2. **Connections → Data sources → Add data source → ClickHouse**.
   - Host: `clickhouse`
   - Port: `9000`
   - Protocol: `native`
   - Database: `stock`
   - User: `default`
   - Password: *(пусто)*
   - **Save & test** → должно быть `Data source is working`.
3. **Dashboards → New → New dashboard → Add visualization → ClickHouse**.

### Панель 1 — Таблица текущих остатков

SQL:
```sql
SELECT
    warehouse_id,
    sku,
    quantity,
    last_event_at
FROM stock.stock_balance_current
ORDER BY last_event_at DESC
LIMIT 100;
```
Формат: **Table**.

### Панель 2 — График суммарного остатка по SKU (топ-10)

SQL:
```sql
SELECT
    toStartOfMinute(event_time) AS t,
    sku,
    sum(quantity) OVER (PARTITION BY sku ORDER BY event_time) AS running_qty
FROM stock.stock_movements
WHERE event_time > now() - INTERVAL 30 MINUTE
ORDER BY t;
```
Формат: **Time series**, Legend = `{{sku}}`.

### Панель 3 — Счётчики (Stat)

```sql
SELECT
    sum(quantity > 0 ? quantity : 0)  AS inbound_qty,
    sum(quantity < 0 ? -quantity : 0) AS outbound_qty,
    count()                           AS events_total
FROM stock.stock_movements
WHERE event_time > now() - INTERVAL 1 HOUR;
```

### Панель 4 — Heatmap активности складов

```sql
SELECT
    toStartOfMinute(event_time) AS t,
    warehouse_id,
    count() AS events
FROM stock.stock_movements
WHERE event_time > now() - INTERVAL 15 MINUTE
GROUP BY t, warehouse_id
ORDER BY t;
```
Формат: **Heatmap**, Time = `t`, Bucket = `warehouse_id`, Value = `events`.

## Скриншот дашборда

> _Вставьте сюда скриншот своего дашборда Grafana
> (панели: таблица остатков, топ-10 SKU, счётчики, heatmap складов)._

## Что демонстрирует проект

- Потоковая обработка событий (Kafka → ClickHouse).
- Kafka Engine + Materialized View для «бесшовного» ingest.
- `ReplacingMergeTree` — дедупликация по `event_id`.
- `AggregatingMergeTree` + `sumState/sumMerge` — инкрементальные витрины.
- Real-time визуализация временных рядов в Grafana.
- Docker-оркестрация всего стека.

## Остановка

```bash
docker compose down          # остановить
docker compose down -v       # остановить и удалить данные (CH, Grafana)
```
Как запустить
bash
# 1. Разложить файлы по структуре
mkdir -p realtime-stock-clickhouse/{producer,clickhouse,grafana/provisioning/datasources}
# ... скопировать файлы ...

# 2. Поднять стек
cd realtime-stock-clickhouse
docker compose up -d --build

# 3. Смотреть логи продюсера
docker compose logs -f producer

# 4. Проверить данные в CH
docker exec -it clickhouse clickhouse-client --database stock \
  -q "SELECT count() FROM stock.stock_movements"

# 5. Открыть Grafana
open http://localhost:3000
На что обратить внимание
Порядок init.sql: docker-entrypoint-initdb.d выполняется только при первом старте ClickHouse. Если меняете SQL — docker compose down -v.

Kafka Engine сам не хранит данные — при перезапуске ClickHouse он продолжит читать с последнего оффсета консьюмер-группы clickhouse_stock_consumer.

ReplacingMergeTree не удаляет дубликаты мгновенно — они мерджатся в фоне. Для гарантированно чистого результата используйте SELECT ... FINAL или OPTIMIZE TABLE ... FINAL.

AggregatingMergeTree работает только с -State/-Merge функциями — не забывайте sumState в MV и sumMerge при чтении.