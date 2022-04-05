from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
import sys
import os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

# each Workflow/DAG must have a unique text identifier
WORKFLOW_DAG_ID = 'data_ingestion'

# start/end times are datetime objects
WORKFLOW_START_DATE = datetime(2022, 3, 28)

# schedule/retry intervals are timedelta objects
# here we execute the DAGs tasks every day
WORKFLOW_SCHEDULE_INTERVAL = timedelta(1)

# default arguments are applied by default to all tasks 
# in the DAG
WORKFLOW_DEFAULT_ARGS = {
    'owner': 'calix',
    'depends_on_past': False,
    'start_date': WORKFLOW_START_DATE,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# initialize the DAG
with DAG(
    dag_id=WORKFLOW_DAG_ID,
    start_date=WORKFLOW_START_DATE,
    schedule_interval=WORKFLOW_SCHEDULE_INTERVAL,
    default_args=WORKFLOW_DEFAULT_ARGS,
    catchup=False,
) as dag:

    start = DummyOperator(
        task_id="start"
    )
    end = DummyOperator(
        task_id="end"
    )

    t1 = BashOperator(
        task_id='data_ingestion',
        bash_command='python3 /mnt/c/data_engineering/datapipeline/airflow/dags/ingestion_step.py',
    )

    t2 = BashOperator(
        task_id='data_processing',
        bash_command='',  
    )

start >> t1 >> t2 >> end
