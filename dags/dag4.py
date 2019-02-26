import airflow
from airflow import DAG
from datetime import datetime

from airflow.models import Variable, Connection

from tempfile import NamedTemporaryFile
from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook


from airflow_training.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator

from airflow.contrib.operators.dataproc_operator import (DataprocClusterCreateOperator, DataprocClusterDeleteOperator, DataProcPySparkOperator,)

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
    filename="land_registry_uk/{{ ds }}/house_pricing.json",
    dag=dag,
)


class HttpToGcsOperator(BaseOperator):
    """
    Calls an endpoint on an HTTP system to execute an action
    :param http_conn_id: The connection to run the operator against
    :type http_conn_id: string
    :param endpoint: The relative part of the full url. (templated)
    :type endpoint: string
    :param gcs_path: The path of the GCS to store the result
    :type gcs_path: string
    """
    template_fields = ('endpoint', 'gcs_path',)
    template_ext = ()
    ui_color = '#f4a460'
    @apply_defaults
    def __init__(self,
                 gcs_conn_id,
                 http_conn_id,
                 gcs_path,
                 bucket,
                 endpoint,
                 *args, **kwargs):
        super(HttpToGcsOperator, self).__init__(*args, **kwargs)
        self.gcs_conn_id = gcs_conn_id
        self.gcs_path = gcs_path
        self.http_conn_id = http_conn_id
        self.bucket = bucket
        self.endpoint = endpoint
        self.method = 'GET'

    def execute(self, context):
        http = HttpHook(self.method, http_conn_id=self.http_conn_id)
        response = http.run(self.endpoint)
        self.log.info(response.text)

        with NamedTemporaryFile() as tmp_file:
            tmp_file.write(response.content)
            tmp_file.flush()

            hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=self.gcs_conn_id)
            hook.upload(bucket=self.bucket, object=self.gcs_path, filename=tmp_file.name)


for target_currency in ['EUR', 'USD']:
    HttpToGcsOperator(
        task_id='get_currency_' + str(target_currency),
        # when there are multiple options (E.g. in a loop), make task_id parameterized
        gcs_conn_id='postgres_conn',
        gcs_path="currency/{{ ds }}/" + target_currency + ".json",
        http_conn_id='http_new',
        bucket='marloes_bucket',
        endpoint="/convert-currency?date={{ ds }}&from=GBP&to={{ target_currency }}",
        dag=dag,
    )


PROJECT_ID = 'airflowbolcom-9362d2a84f6f553b'

dataproc_create_cluster = DataprocClusterCreateOperator(
    task_id="create_dataproc",
    cluster_name="pricing-analysis-{{ ds }}",
    project_id=PROJECT_ID,
    num_workers=2,
    zone="europe-west4-a",
    dag=dag,)

compute_aggregates = DataProcPySparkOperator(
    task_id='compute_aggregates',
    main="gs://europe-west1-training-airfl-a31ccad6-bucket/dags/build_statistics.py",
    cluster_name='pricing-analysis-{{ ds }}',
    arguments=[
        "gs://marloes_bucket/land_registry_uk/{{ ds }}/*.json",
        "gs://marloes_bucket/currency/{{ ds }}/*.json",
        "gs://marloes_bucket/statistics/{{ ds }}/"
    ],
    dag=dag,
)