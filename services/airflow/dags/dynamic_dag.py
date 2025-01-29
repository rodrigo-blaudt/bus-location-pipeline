from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Define the default arguments for the DAG
default_args = {
    'owner': 'rodrigo',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'dynamic_dag_example',
    default_args=default_args,
    description='A simple dynamic DAG example',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# List of task names to be created dynamically
task_names = ['task_1', 'task_2', 'task_3', 'task_4']


# Function to be executed by the PythonOperator
def print_task_name(task_name):
    print(f"Executing task: {task_name}")


# Create tasks dynamically
for task_name in task_names:
    task = PythonOperator(
        task_id=task_name,
        python_callable=print_task_name,
        op_kwargs={'task_name': task_name},
        dag=dag,
    )

# Define the start and end tasks
start_task = DummyOperator(task_id='start', dag=dag)
end_task = DummyOperator(task_id='end', dag=dag)

# Set the dependencies
start_task >> [task for task in dag.tasks if task.task_id in task_names] >> end_task
