# airflow-etl-pipeline
    
An example implementation of a wrapper for airflow ETL processes.

![Airflow ETL pipeline example](/img/screen.jpg)

### Decorating DAG airflow
  
This example shows how to implement an airflow DAG wrapper for ETL process logging purposes. It can also be used for testing or other process decoration purposes.
   
In this example, it is necessary to log the creation of a new process launch, as well as the termination of the process. In this case, the DAG may have several parallel tasks, some of which may fail.   
It is necessary to track errors in parallel processes and, in case of errors, terminate the launch with an error code, and in case of successful completion of all tasks, terminate the launch with a success code.   
   
For these purposes, several service tasks are implemented:
* create_new_etl_run - create a new row in the ETL registry
* finish_etl_with_error - update error status for ETL process in the ETL registry
* finish_etl_with_success - update success status for ETL process in the ETL registry
* finish_etl_group - dummy task for grouping finish tasks
  
In order to avoid duplication of tasks in each new DAG, you can implement a wrapper with such tasks.
  
### Example project
   
Structure:  
* etl.common.etl_dag_pipeline.py - Wrapper for building a pipeline
* etl.common.etl_service_task.py - Service dummy procedures for filling the launch register
* etl.example.etl_dag_example_etl_process.py - An example implementation of ETL process with DAG wrapper

### Usage

Just describe your task without mapping to a DAG:

```python
# ...

task8 = EmptyOperator(  
    task_id="task8",  
    trigger_rule="all_success",  
)  
  
task_list = [task1, task2, task3, task4, task5, task6, task7, task8]  
task_dependencies = ([task1, task2], task3, [task4, task5, task6, task7], task8,)
```

Also define a **task_list** with all tasks and **task_dependencies** tuple object, which include lists objects, where each list of tasks - it's list of the parallel running tasks or just task object - it's sequential execution.   
   
The visualization of this process is shown in the image below:

![Airflow ETL pipeline example](/img/screen2.png)

Use procedure **dag_pipeline_wrapper** to get DAG with wrapper:

```python
from etl.common.etl_dag_pipeline import dag_pipeline_wrapper

dags_params = dict(
    start_date=datetime(year=2023, month=1, day=1),  
    schedule_interval=None,  
    catchup=False,  
    description="Example DAG for ETL process",  
)  
  
dag = dag_pipeline_wrapper(  
    dag_name=DAG_NAME,  
    task_list=task_list,  
    task_dep=task_dependencies,  
    **dags_params,  
)
```

Now, you can use this wrapper for each new DAG just define dag_name, task_list and task_dep vars.  
  
Full display example in file - etl.example.etl_dag_example_etl_process.py