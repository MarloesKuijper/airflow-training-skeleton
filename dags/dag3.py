import airflow
from airflow import DAG
from datetime import datetime
from airflow.operators.slack_operator import SlackAPIPostOperator
from bigquery_get_data import BigQueryGetDataOperator

dag = DAG(
    dag_id='cool_id',
    schedule_interval='@daily',
    default_args={
        'owner': 'GoDataDriven',
        'start_date': airflow.utils.dates.days_ago(14)
    }
)

query = """SELECT
          committer.name,
          COUNT(*) AS count
        FROM
          [bigquery-public-data.github_repos.commits] AS commits
        WHERE
          DATE(committer.date) = '{{ ds }}'
          AND repo_name LIKE '%airflow%'
        GROUP BY
          committer.name
        order by count desc
        LIMIT 5"""

bq_fetch_data = BigQueryGetDataOperator(
    task_id='bq_fetch_data',
    sql=query,
    dag=dag
)

from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable


def send_to_slack_func(**context):
    task_id = 'top3_committers_airflow'
    text = context['task_instance'].xcom_pull(task_ids='bq_fetch_data')
    channel = "general"

    operator = SlackAPIPostOperator(
        task_id=task_id,
        token=Variable.get("secret_token"),
        channel=channel,
        username="Darth Mother",
        text=text,
        icon_url="https://media.makeameme.org/created/i-am-your-5c16f0.jpg"

    )

    return operator.execute(context=context)


send_to_slack = PythonOperator(
    task_id='send_to_slack',
    python_callable=send_to_slack_func,
    provide_context=True,
    dag=dag,
)

bq_fetch_data >> send_to_slack
