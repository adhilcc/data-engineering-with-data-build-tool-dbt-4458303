from pendulum import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
from airflow.models import Variable
from cosmos import DbtTaskGroup, RenderConfig
from cosmos.config import ProfileConfig, ProjectConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from pathlib import Path
import os

def on_failure_callback(context,SVC_NAME):
    svc=SVC_NAME
    task=context.get("task_instance").task_id
    dag=context.get("task_instance").dag_id
    ti=context.get("task_instance")
    exec_date=context.get("execution_date")
    dag_run = context.get('dag_run')
    log_url = context.get("task_instance").log_url
    msg = f""" 
            SVC: {svc}
            Dag: {dag}
            Task: {task}
            DagRun: {dag_run}
            TaskInstance: {ti}
            Log Url: {log_url} 
            Execution Time: {exec_date} 
            """
    print(msg)

profile_config = ProfileConfig(
    profile_name="nyc_parking_violations",
    target_name="dev",
    profiles_yml_filepath = "/appz/home/airflow/dags/data-engineering/nyc_parking_violations/profiles.yml",
)


readme_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'README.md')
with open(readme_path, 'r') as file:
    readme_content = file.read()

with DAG(
    dag_id="nyc-data-engg",
    start_date=datetime(2025, 6, 12),
    schedule='0 0/12 * * *',
    tags=["DE-dag"],
    doc_md=readme_content,
    default_args = {
    "owner": "airflow"
    },
    catchup=False,
):
   

    seeds_tg = DbtTaskGroup(
        project_config=ProjectConfig(
        Path("/appz/home/airflow/dags/data-engineering"),
    ),
        operator_args={
            "append_env": True,
        },
        profile_config=profile_config,
        execution_config=ExecutionConfig(
        dbt_executable_path="/dbt_venv/bin/dbt",
    ),
        render_config=RenderConfig(
        select=["path:data/"],
    ),
        default_args={"retries": 2},
        group_id = "dbt_seeds_group"
    )


  
    stg_bronze = DbtTaskGroup(
        project_config=ProjectConfig(
        Path("/appz/home/airflow/dags/data-engineering/nyc_parking_violations"),
    ),
        operator_args={
            "append_env": True,
        },
        profile_config=profile_config,
        execution_config=ExecutionConfig(
        dbt_executable_path="/dbt_venv/bin/dbt",
    ),
        render_config=RenderConfig(
        select=["path:models/bronze/"],
    ),
        default_args={"retries": 1,
                     'on_failure_callback': lambda context: on_failure_callback(context,"TEST_SVC_NAME"),},
        group_id = "dbt_stg_group"
    )


  
    stg_silver = DbtTaskGroup(
        project_config=ProjectConfig(
        Path("/appz/home/airflow/dags/data-engineering/nyc_parking_violations"),
    ),
        operator_args={
            "append_env": True,
        },
        profile_config=profile_config,
        execution_config=ExecutionConfig(
        dbt_executable_path="/dbt_venv/bin/dbt",
    ),
        render_config=RenderConfig(
        select=["path:models/silver/"],
    ),
        default_args={"retries": 1,
                     'on_failure_callback': lambda context: on_failure_callback(context,"TEST_SVC_NAME"),},
        group_id = "dbt_stg_group"
    )

  

    stg_gold = DbtTaskGroup(
        project_config=ProjectConfig(
        Path("/appz/home/airflow/dags/data-engineering/nyc_parking_violations"),
    ),
        operator_args={
            "append_env": True,
        },
        profile_config=profile_config,
        execution_config=ExecutionConfig(
        dbt_executable_path="/dbt_venv/bin/dbt",
    ),
        render_config=RenderConfig(
        select=["path:models/gold/"],
    ),
        default_args={"retries": 1,
                     'on_failure_callback': lambda context: on_failure_callback(context,"TEST_SVC_NAME"),},
        group_id = "dbt_stg_group"
    )


  
    dbt_tg = DbtTaskGroup(
        project_config=ProjectConfig(
        Path("/appz/home/airflow/dags/jaffle_shop"),
    ),
        operator_args={
            "append_env": True,
           # "queue": "on-premises",
        },
        profile_config=profile_config,
        execution_config=ExecutionConfig(
        dbt_executable_path="/dbt_venv/bin/dbt",
    ),
        render_config=RenderConfig(
        exclude=["path:models/bronze/","path:data/","path:models/silver/","path:models/gold/"],
    ),
        default_args={"retries": 2},
    )
    
   
   
    e2 = EmptyOperator(task_id="post_dbt")
    
    seeds_tg >> stg_bronze >> stg_silver >> stg_gold >> dbt_tg >> e2
