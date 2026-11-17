"""
ETL DAG с вынесенным SQL в отдельные файлы (dags/sql/).
Задачи 1-4 используют PostgresOperator с путями к .sql файлам.
Задачи 5-6 остаются Python-функциями, т.к. содержат логику, а не SQL.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from custom_operator import HelloOperator

# Абсолютный путь к папке sql (рядом с этим DAG-файлом)
SQL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql")


# ---------------------------
# Python-функции для задач
# ---------------------------

def extract_and_log(**context):
    """Задача 5: читает данные из Postgres и логирует их."""
    hook = PostgresHook(postgres_conn_id='my_postgres_db')
    records = hook.get_records("SELECT id, name FROM test_users ORDER BY id;")
    for row in records:
        context['ti'].log.info(f"Extracted row: id={row[0]}, name={row[1]}")
    return len(records)


def validate_data(**context):
    """Задача 6: проверяет, что данные соответствуют ожиданиям."""
    ti = context['ti']
    count = ti.xcom_pull(task_ids='extract_data')
    if count is None or count < 3:
        raise ValueError(f"Ожидалось минимум 3 записи, получено: {count}")
    ti.log.info(f"Валидация пройдена: {count} записей.")


# ---------------------------
# Параметры DAG
# ---------------------------

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='etl_users_dag',
    default_args=default_args,
    description='ETL-пайплайн для работы с таблицей users (SQL вынесен в /sql)',
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'postgres', 'example'],
    template_searchpath=[SQL_DIR],  # <-- ищем .sql файлы здесь
) as dag:

    # 1. Создание таблицы
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='my_postgres_db',
        sql='create_table.sql',
    )

    # 2. Очистка перед загрузкой (идемпотентность)
    truncate_table = PostgresOperator(
        task_id='truncate_table',
        postgres_conn_id='my_postgres_db',
        sql='truncate_table.sql',
    )

    # 3. Загрузка данных
    load_data = PostgresOperator(
        task_id='load_data',
        postgres_conn_id='my_postgres_db',
        sql='load_data.sql',
    )

    # 4. Трансформация: обновляем is_active для одной записи
    transform_data = PostgresOperator(
        task_id='transform_data',
        postgres_conn_id='my_postgres_db',
        sql='transform_data.sql',
    )

    # 5. Извлечение данных через Python + логирование
    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract_and_log,
        provide_context=True,
    )

    # 6. Валидация результата
    validate = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
        provide_context=True,
    )

    # (опционально) кастомный оператор из plugins
    say_hello = HelloOperator(
        task_id='say_hello',
        name='Airflow ETL',
    )

    # ---------------------------
    # Порядок выполнения
    # ---------------------------
    create_table >> truncate_table >> load_data >> transform_data >> extract_data >> validate >> say_hello