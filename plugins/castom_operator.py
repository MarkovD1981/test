from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

class HelloOperator(BaseOperator):
    """
    Простой кастомный оператор, который логирует приветствие.
    """
    @apply_defaults
    def __init__(self, name: str, **kwargs):
        super().__init__(**kwargs)
        self.name = name

    def execute(self, context):
        message = f"Hello, {self.name}!"
        self.log.info(message)
        return message