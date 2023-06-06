try:
    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    print("All Dag modules are ok ... ")
except Exception as e:
    print("Error {} ". format(e))


def first_function_execute(**kwargs):
    var = kwargs.get("name", "Did not get the key!")
    return "Hello World" + var

with DAG(
    dag_id="first_dag",
    schedule_interval="@daily",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": datetime(2023, 6, 6)
    },
    catchup=False
) as d:
    first_function_execute = PythonOperator(
        task_id="first_function_execute",
        python_callable=first_function_execute,
        op_kwargs={"name": "Ivelin Ivanov"}
    )
