import os
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv

dbt_env_path = os.path.join(os.environ['AIRFLOW_HOME'], 'dbt_project', 'dbt.env')
load_dotenv(dbt_env_path)

airflow_home = os.environ['AIRFLOW_HOME']

# Define paths
PATH_TO_DBT_PROJECT = f'{airflow_home}/dbt_project'
PATH_TO_DBT_VENV = f'{airflow_home}/dbt_venv/bin/activate'

default_args = {
    "owner": "slaterc3",
    "retries": 0,
    "execution_timeout": timedelta(hours=1),
}

@dag(
    start_date=datetime(2024, 1, 1),
    schedule='@once',
    catchup=False,
    default_args=default_args,
)
def dbt_wap_dag():
    pre_dbt_workflow = EmptyOperator(task_id="pre_dbt_workflow")

    dbt_build = BashOperator(
        task_id='dbt_build',
        bash_command=f'dbt build --profiles-dir {PATH_TO_DBT_PROJECT} --profile my-snowflake-db -s +fact_visits stg_haunted_house_tickets stg_customer_feedbacks',
        cwd=PATH_TO_DBT_PROJECT,
    )

    post_dbt_workflow = EmptyOperator(task_id="post_dbt_workflow", trigger_rule="all_done")

    pre_dbt_workflow >> dbt_build >> post_dbt_workflow

dbt_wap_dag()
