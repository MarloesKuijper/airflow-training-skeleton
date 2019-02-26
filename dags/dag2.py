import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator


args = {"owner": "godatadriven", "start_date": airflow.utils.dates.days_ago(14)}

dag = DAG(
    dag_id="exercise3",
    default_args=args,
    description="Demo DAG showing BranchPythonOperator.",
    schedule_interval="0 0 * * *",
)


def print_weekday(execution_date, **context):
    print(execution_date.strftime("%a"))


print_weekday = PythonOperator(
        task_id="print_weekday",
        python_callable=print_weekday,
        provide_context=True,
        dag=dag,

)


def pick_a_person(execution_date, **context):
    weekday_person_to_email = {
        0: "Bob",  # Monday
        1: "Joe",  # Tuesday
        2: "Alice",  # Wednesday
        3: "Joe",  # Thursday
        4: "Alice",  # Friday
        5: "Alice",  # Saturday
        6: "Alice",  # Sunday
    }
    return "email_" + str(weekday_person_to_email[execution_date.weekday()])


branching = BranchPythonOperator(
    task_id='branching',
    python_callable=pick_a_person,
    provide_context=True,
    dag=dag)

email = [DummyOperator(task_id="email_" + i, dag=dag) for i in ["Bob", "Joe", "Alice"]]

final_task = DummyOperator(task_id="final_task", dag=dag, trigger_rule='one_success')

print_weekday >> branching >> email >> final_task
