from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator




default_args = {"owner": "baaj2109",
                "depends_on_past": False,
                "start_date": datetime(2021, 1, 1),
                "email_on_failure": False,
                "email_on retry": False,
                "retries": 1,
                "retry_delay": timedelta(minutes = 5),
                "schedule_interval": "@daily"}
with DAG(dag_id = "stock", 
	      default_args = default_args
) as dag:
    task_1 = PythonOperator(
        task_id = "crawer_start",
        python_callable = 
        provide_context = True,
        dag = dag,
    )


