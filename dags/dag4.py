import airflow
from airflow import DAG
from datetime import datetime
from airflow.operators.slack_operator import SlackAPIPostOperator
from bigquery_get_data import BigQueryGetDataOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable, Connection


from airflow_training.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator

dag = DAG(
    dag_id='cool_id_2',
    schedule_interval='@daily',
    default_args={
        'owner': 'GoDataDriven',
        'start_date': airflow.utils.dates.days_ago(14)
    }
)


pgsl_to_gcs = PostgresToGoogleCloudStorageOperator(
    task_id="postgres_to_gcs",
    postgres_conn_id='postgres_conn',
    sql="SELECT * FROM public.land_registry_price_paid_uk WHERE transfer_date = '{{ ds }}'",
    bucket="marloes_bucket",
    filename="test_file",
    dag=dag,
)

