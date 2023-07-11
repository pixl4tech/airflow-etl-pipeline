from logging import getLogger
from typing import Tuple, List, Dict

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from etl.common.etl_service_task import ETLTaskManager

log = getLogger(__name__)
etl = ETLTaskManager()


def dag_pipeline_wrapper(dag_name: str, task_list: List, task_dep: Tuple, etl_run_params: Dict, **kwargs):

    with DAG(
        dag_id=dag_name,
        **kwargs,
    ) as dag:

        run_id = etl_run_params.get("run_id")

        create_new_etl_task = PythonOperator(
            task_id="create_new_etl_run",
            python_callable=etl.create_new_etl,
            op_kwargs=etl_run_params,
            provide_context=True,
            retries=0,
        )

        for task in task_list:
            dag.add_task(task)

        finish_group = EmptyOperator(
            task_id="finish_etl_group",
            trigger_rule="all_success",
        )

        finish_with_error_task = PythonOperator(
            task_id="finish_etl_with_error",
            python_callable=etl.finish_etl,
            op_kwargs={"run_id": run_id, "status_code": "F"},
            trigger_rule="one_failed",
            provide_context=True,
            retries=0,
        )

        finish_with_success_task = PythonOperator(
            task_id="finish_etl_with_success",
            python_callable=etl.finish_etl,
            op_kwargs={"run_id": run_id, "status_code": "S"},
            trigger_rule="all_success",
            provide_context=True,
            retries=0,
        )

        tasks_dependencies = create_new_etl_task
        for elem in task_dep:
            tasks_dependencies = tasks_dependencies >> elem  # noqa: WPS350

        tasks_dependencies >> finish_group >> [finish_with_error_task, finish_with_success_task]

    return dag
