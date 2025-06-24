
from pendulum import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from cosmos import DbtTaskGroup, RenderConfig
from cosmos.config import ProfileConfig, ProjectConfig, ExecutionConfig
from functools import partial
from pathlib import Path

# ✅ Failure callback (same as before)
def on_failure_callback(context, SVC_NAME):
    svc = SVC_NAME
    task = context.get("task_instance").task_id
    dag = context.get("task_instance").dag_id
    ti = context.get("task_instance")
    exec_date = context.get("execution_date")
    dag_run = context.get("dag_run")
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

# ✅ Profile config for Snowflake
profile_config = ProfileConfig(
    profile_name="nyc_parking_violations",
    target_name="snowflake_dev",
    profiles_yml_filepath="/appz/home/airflow/dags/data-engineering/nyc_parking_violations/profiles.yml",
)

# ✅ Read README.md as doc
readme_path = Path(__file__).parent / 'README.md'
readme_content = readme_path.read_text()

# ✅ Failure callback with service name
failure_callback_partial = partial(on_failure_callback, SVC_NAME="SNOWFLAKE_SVC")

with DAG(
    dag_id="nyc-data-engg-snowflake",
    start_date=datetime(2025, 6, 12),
    schedule_interval="0 */12 * * *",
    tags=["DE-dag", "snowflake"],
    doc_md=readme_content,
    default_args={"owner": "airflow"},
    catchup=False,
) as dag:

    # ✅ dbt seed
    seeds_tg = DbtTaskGroup(
        group_id="dbt_seeds_group",
        project_config=ProjectConfig(
            Path("/appz/home/airflow/dags/data-engineering/nyc_parking_violations")
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path="/dbt_venv/bin/dbt"
        ),
        render_config=RenderConfig(
            select=["path:seeds/"]
        ),
        operator_args={"append_env": True},  # ⛔ Do NOT pass env manually
        default_args={"retries": 2}
    )

    stg_bronze = DbtTaskGroup(
        group_id="dbt_stg_group1",
        project_config=ProjectConfig(
            Path("/appz/home/airflow/dags/data-engineering/nyc_parking_violations")
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path="/dbt_venv/bin/dbt"
        ),
        render_config=RenderConfig(
            select=["path:models/bronze/"]
        ),
        operator_args={"append_env": True},
        default_args={
            "retries": 1,
            "on_failure_callback": failure_callback_partial
        }
    )

    stg_silver = DbtTaskGroup(
        group_id="dbt_stg_group2",
        project_config=ProjectConfig(
            Path("/appz/home/airflow/dags/data-engineering/nyc_parking_violations")
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path="/dbt_venv/bin/dbt"
        ),
        render_config=RenderConfig(
            select=["path:models/silver/"]
        ),
        operator_args={"append_env": True},
        default_args={
            "retries": 1,
            "on_failure_callback": failure_callback_partial
        }
    )

    stg_gold = DbtTaskGroup(
        group_id="dbt_stg_group3",
        project_config=ProjectConfig(
            Path("/appz/home/airflow/dags/data-engineering/nyc_parking_violations")
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path="/dbt_venv/bin/dbt"
        ),
        render_config=RenderConfig(
            select=["path:models/gold/"]
        ),
        operator_args={"append_env": True},
        default_args={
            "retries": 1,
            "on_failure_callback": failure_callback_partial
        }
    )

    post_dbt = EmptyOperator(task_id="post_dbt")

    seeds_tg >> stg_bronze >> stg_silver >> stg_gold >> post_dbt
