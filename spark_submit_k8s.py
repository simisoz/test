import datetime as dt
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'me',
    'start_date': dt.datetime(2018, 6, 8),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}
submit_config = {
    'conf': {
        'spark.kubernetes.container.image': 'spark23:latest'
    },
    'total_executor_cores': 4,
    'executor_cores': 1,
    'executor_memory': '1g',
    'name': '{{ task_instance.task_id }}',
    'num_executors': 4,
    'verbose': True,
    'driver_memory': '1g',
}
executor_config = {
    "KubernetesExecutor": {
    "image": "helm:latest"
    }
}

dag = DAG(
    dag_id='spark_submit', default_args=default_args,
    schedule_interval='@once'
)
compute_pi = SparkSubmitOperator(
    task_id='computepi',
    conn_id='spark_k8s_cluster',
    application='local:///usr/local/spark/examples/jars/spark-example_2.11-2.3.0.jar',
    java_class='org.apache.spark.examples.WordCount',
    dag=dag,
    executor_config={"KubernetesExecutor": {"image": "spark23:latest"}},
    **submit_config
)

compute_pi