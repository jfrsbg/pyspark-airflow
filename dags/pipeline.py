from airflow.decorators import task, dag
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator

from spark_files.spark_session import spark
from spark_files.raw_to_staged import RawToStaged
from spark_files.staged_to_trusted import StagedToTrusted
from spark_files.trusted_to_refined import TrustedToRefined

default_args = {
    'owner': 'Airflow',
    'email': ['teste.jeferson@teste.com'],
    'email_on_failure' : False,
    'retries': 0
}

@task
def ingest():
    proccess = RawToStaged(spark)
    proccess.execute()

@task
def staged_to_trusted():
    proccess = StagedToTrusted(spark)
    proccess.execute()

@task
def trusted_to_refined():
    proccess = TrustedToRefined(spark)
    proccess.execute()

@dag(start_date=datetime(2022, 1, 1), schedule_interval="@daily",
    catchup=False, max_active_runs=1, dagrun_timeout=timedelta(minutes=5), 
    tags=['data_engineering_challenge'],
    default_args=default_args)

def proccess():
    start = DummyOperator(task_id="start")

    start >> ingest() >> staged_to_trusted() >> trusted_to_refined()


dag = proccess()