import datetime as dt
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'me',
    'start_date': dt.datetime(2018, 6, 8),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}
submit_config = {
    'conf': {
        'spark.kubernetes.container.image': 'spark:latest'
    },
    'total_executor_cores': 2,
    'executor_cores': 1,
    'executor_memory': '1g',
    'name': '{{ task_instance.task_id }}',
    'num_executors': 2,
    'verbose': True,
    'driver_memory': '1g',
}
executor_config = {
    "KubernetesExecutor": {
    "image": "helm:latest"
    }
}

dag = DAG(
    dag_id='spark', default_args=default_args,
    schedule_interval='@once'
)


spawn_spark = BashOperator(
    task_id="spawn_spark", 
    dag=dag, 
    bash_command="helm init --service-account=tiller --upgrade --client-only && helm install --name spark stable/spark --version=0.1.13 --namespace=spark",
    executor_config=executor_config
    )


delete_spark = BashOperator(
    task_id="delete_spark", 
    dag=dag, 
    bash_command="sleep 300 ; helm init --client-only && helm delete --purge spark",
    executor_config=executor_config)

spawn_spark  >> delete_spark
