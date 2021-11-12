from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

from src import Crawer, StockModel


default_args = {
    "owner": "baaj2109",
    "depends_on_past": False,
    # "start_date": datetime(2021, 1, 1),
    "email_on_failure": False,
    "email_on retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes = 5),
    "schedule_interval": "@daily"
}

def start_craw_data():
    crawer = Crawer()
    crawer.process()


def start_train_model():
    model = StockModel()
    model.train()
    model.predict_and_save()

with DAG(
    dag_id = "stock", 
    default_args = default_args,
    schedule_interval = "0 18 * * *",
    start_dte = datetime(2021,1,1),
) as dag:
    get_today_data = PythonOperator(
        task_id = "crawer_start",
        python_callable = start_craw_data
        provide_context = True,
        # dag = dag,
    )


    start_training = PythonOperator(
        task_id = "model_start",
        python_callable = start_train_model,
        provide_context = True,
        # dag = dag
    )

    
get_today_data >> start_training





