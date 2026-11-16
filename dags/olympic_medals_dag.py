from datetime import datetime
import random
import time

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
#from airflow.sensors.sql import SqlSensor
from airflow.providers.common.sql.sensors.sql import SqlSensor


def pick_medal():
    medal = random.choice(["Bronze", "Silver", "Gold"])
    return medal


def branch_medal(**context):
    medal = context["ti"].xcom_pull(task_ids="pick_medal")

    if medal == "Bronze":
        return "calc_bronze"

    if medal == "Silver":
        return "calc_silver"

    return "calc_gold"


def generate_delay():
    #time.sleep(10)
    time.sleep(35)  # Simulate a delay of 35 seconds to test the sensor's timeout behavior

dag = DAG(
    dag_id="olympic_medals_dag",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    schedule=None,
)

create_table = SQLExecuteQueryOperator(
    task_id="create_table",
    conn_id="GOIT_MYSQL_DB",
    sql="""
    CREATE TABLE IF NOT EXISTS medal_statistics (
        id INT AUTO_INCREMENT PRIMARY KEY,
        medal_type VARCHAR(20),
        count INT,
        created_at DATETIME
    );
    """,
    dag=dag,
)

pick_medal_task = PythonOperator(
    task_id="pick_medal",
    python_callable=pick_medal,
    dag=dag,
)

branch_task = BranchPythonOperator(
    task_id="pick_medal_task",
    python_callable=branch_medal,
    dag=dag,
)

calc_bronze = SQLExecuteQueryOperator(
    task_id="calc_bronze",
    conn_id="GOIT_MYSQL_DB",
    sql="""
    INSERT INTO medal_statistics (medal_type, count, created_at)
    SELECT
        'Bronze',
        COUNT(*),
        NOW()
    FROM athlete_event_results
    WHERE medal = 'Bronze';
    """,
    dag=dag,
)

calc_silver = SQLExecuteQueryOperator(
    task_id="calc_silver",
    conn_id="GOIT_MYSQL_DB",
    sql="""
    INSERT INTO medal_statistics (medal_type, count, created_at)
    SELECT
        'Silver',
        COUNT(*),
        NOW()
    FROM athlete_event_results
    WHERE medal = 'Silver';
    """,
    dag=dag,
)

calc_gold = SQLExecuteQueryOperator(
    task_id="calc_gold",
    conn_id="GOIT_MYSQL_DB",
    sql="""
    INSERT INTO medal_statistics (medal_type, count, created_at)
    SELECT
        'Gold',
        COUNT(*),
        NOW()
    FROM athlete_event_results
    WHERE medal = 'Gold';
    """,
    dag=dag,
)

delay_task = PythonOperator(
    task_id="generate_delay",
    python_callable=generate_delay,
    trigger_rule="none_failed_min_one_success",
    dag=dag,
)

check_for_correctness = SqlSensor(
    task_id="check_for_correctness",
    conn_id="GOIT_MYSQL_DB",
    sql="""
    SELECT COUNT(*)
    FROM medal_statistics
    WHERE created_at >= NOW() - INTERVAL 30 SECOND;
    """,
    poke_interval=5,
    timeout=30,
    mode="poke",
    dag=dag,
)

create_table >> pick_medal_task >> branch_task

branch_task >> calc_bronze >> delay_task
branch_task >> calc_silver >> delay_task
branch_task >> calc_gold >> delay_task

delay_task >> check_for_correctness
