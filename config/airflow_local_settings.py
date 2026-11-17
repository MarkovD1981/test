# config/airflow_local_settings.py
from copy import deepcopy
from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG

# Копируем дефолтную конфигурацию
LOGGING_CONFIG = deepcopy(DEFAULT_LOGGING_CONFIG)

# Пример: изменение формата логов для задач
# (Убедитесь, что структура ключей соответствует вашей версии Airflow)
try:
    LOGGING_CONFIG['formatters']['airflow.task'] = {
        'format': '[%(asctime)s] {{%(filename)s:%(lineno)d}} %(levelname)s - %(message)s'
    }
except KeyError:
    # Если структура отличается, пропускаем изменение


    pass