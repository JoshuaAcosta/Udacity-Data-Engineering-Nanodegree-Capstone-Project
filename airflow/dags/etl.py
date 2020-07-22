from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'JoshuaAcosta',
    'depends_on_past': False,
    'catchup': False,
    'start_date': days_ago(2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='ETL_311complaint_dag',
          default_args=default_args,
          description='Extract, load and stage NYC 311, weather and city demographics data',
          schedule_interval=None)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

get_weather_data = BashOperator(
    task_id='get_NOAA_weather_data',
    bash_command='python get_weather_data.py',
    dag=dag
)

end_operator = DummyOperator(task_id='execution_complete',  dag=dag)

start_operator >> get_weather_data >> end_operator
