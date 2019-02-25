import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

dag = DAG(
    dag_id="hello_airflow",
    default_args={
        "owner": "godatadriven",
        "start_date": airflow.utils.dates.days_ago(3),
    },
)

print_exec_date = BashOperator(
    task_id="print_exec_date", bash_command="echo {{ execution_date }}", dag=dag
)

dummy_end = DummyOperator(
    task_id="end", bash_command="", dag=dag
)

sleeping = [BashOperator(
    task_id="sleeping_" + str(i), bash_command="sleep " + str(i), dag=dag
) for i in [1, 5, 10]]

print_exec_date >> sleeping >> dummy_end

