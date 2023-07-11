from datetime import datetime
from logging import getLogger
from pathlib import PosixPath

from airflow.operators.empty import EmptyOperator

from etl.common.etl_dag_pipeline import dag_pipeline_wrapper

log = getLogger(__name__)

DAG_NAME = PosixPath(__file__).stem

# just for example
etl_run_params = dict(  # noqa: C408
    run_id="a94a8fe5ccb19ba61c4c0873d391e987982fbbd3",
    run_created_dttm=datetime.now(),
    run_dag_name=DAG_NAME,
    run_processed_dttm=datetime.now()
)


task1 = EmptyOperator(
    task_id="task1",
    trigger_rule="all_success",
)

task2 = EmptyOperator(
    task_id="task2",
    trigger_rule="all_success",
)

task3 = EmptyOperator(
    task_id="task3",
    trigger_rule="all_success",
)

task4 = EmptyOperator(
    task_id="task4",
    trigger_rule="all_success",
)

task5 = EmptyOperator(
    task_id="task5",
    trigger_rule="all_success",
)

task6 = EmptyOperator(
    task_id="task6",
    trigger_rule="all_success",
)

task7 = EmptyOperator(
    task_id="task7",
    trigger_rule="all_success",
)

task8 = EmptyOperator(
    task_id="task8",
    trigger_rule="all_success",
)

task_list = [task1, task2, task3, task4, task5, task6, task7, task8]
task_dependencies = ([task1, task2], task3, [task4, task5, task6, task7], task8,)

dags_params = dict(  # noqa: C408
    start_date=datetime(year=2023, month=1, day=1),
    schedule_interval=None,
    catchup=False,
    description="Example DAG for ETL process",
)

dag = dag_pipeline_wrapper(
    dag_name=DAG_NAME,
    task_list=task_list,
    task_dep=task_dependencies,
    etl_run_params=etl_run_params,
    **dags_params,
)
