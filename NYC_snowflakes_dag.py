from pendulum import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from cosmos import DbtTaskGroup, RenderConfig
from cosmos.config import ProfileConfig, ProjectConfig, ExecutionConfig
from functools import partial
from pathlib import Path
import os


env_vars = {
    "SNOWFLAKE_PRIVATE_KEY": os.environ.get("SNOWFLAKE_PRIVATE_KEY"),
    "SNOWFLAKE_PASSPHRASE": os.environ.get("SNOWFLAKE_PASSPHRASE"),
}

# Custom failure callback
def on_failure_callback(context, SVC_NAME):
    task = context.get("task_instance").task_id
    dag = context.get("task_instance").dag_id
    exec_date = context.get("execution_date")
    dag_run = context.get("dag_run")
    log_url = context.get("task_instance").log_url
    print(f"""
        SVC: {SVC_NAME}
        Dag: {dag}
        Task: {task}
        DagRun: {dag_run}
        Execution Time: {exec_date}
        Log Url: {log_url}
    """)

# Cosmos dbt profile config
profile_config = ProfileConfig(
    profile_name="nyc_parking_violations",
    target_name="snowflake_dev",
    profiles_yml_filepath="/appz/home/airflow/dags/data-engineering/nyc_parking_violations/profiles.yml",
)

failure_callback_partial = partial(on_failure_callback, SVC_NAME="SNOWFLAKE_SVC")

with DAG(
    dag_id="nyc_snowflake_dag",
    start_date=datetime(2025, 6, 12),
    schedule_interval="@daily",
    catchup=False,
    tags=["snowflake", "dbt"],
    default_args={"owner": "airflow"},
) as dag:

    seeds_tg = DbtTaskGroup(
        group_id="dbt_seeds_snowflake",
        project_config=ProjectConfig(
            Path("/appz/home/airflow/dags/data-engineering/nyc_parking_violations")
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path="/dbt_venv/bin/dbt"),
        render_config=RenderConfig(select=["path:seeds/"]),
        operator_args={"env": env_vars, "append_env": True},
    )

    bronze = DbtTaskGroup(
        group_id="dbt_bronze_snowflake",
        project_config=ProjectConfig(
            Path("/appz/home/airflow/dags/data-engineering/nyc_parking_violations")
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path="/dbt_venv/bin/dbt"),
        render_config=RenderConfig(select=["path:models/bronze/"]),
        operator_args={"env": env_vars, "append_env": True},
    )

    silver = DbtTaskGroup(
        group_id="dbt_silver_snowflake",
        project_config=ProjectConfig(
            Path("/appz/home/airflow/dags/data-engineering/nyc_parking_violations")
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path="/dbt_venv/bin/dbt"),
        render_config=RenderConfig(select=["path:models/silver/"]),
        operator_args={"env": env_vars, "append_env": True},
    )

    gold = DbtTaskGroup(
        group_id="dbt_gold_snowflake",
        project_config=ProjectConfig(
            Path("/appz/home/airflow/dags/data-engineering/nyc_parking_violations")
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path="/dbt_venv/bin/dbt"),
        render_config=RenderConfig(select=["path:models/gold/"]),
        operator_args={"env": env_vars, "append_env": True},
    )

    final = EmptyOperator(task_id="final_snowflake")

    seeds_tg >> bronze >> silver >> gold >> final
