from airflow.models.baseoperator import BaseOperator

# from airflow.operators.python_operator import PythonOperator


class SampleOperator(BaseOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def execute(self, context):
        # if self.deferrable is True:
        #     print(f"true - deferable {self.deferrable}")
        # elif self.deferrable is False:
        #     print(f"false - deferrable: {self.deferrable}")
        # else:
        print(f"nonono - deferrable: {self.deferrable}")
        return self.deferrable
