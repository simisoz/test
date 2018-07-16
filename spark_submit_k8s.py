from airflow import DAG
from airflow.operators.sensors import S3KeySensor
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'simiso',
    'depends_on_past': False,
    'start_date': datetime(2018, 7, 16),
    'email': ['simiso.zwane@rmb.co.za'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
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

dag = DAG('spark_submit', default_args=default_args, schedule_interval='@once')

start = BashOperator(
    task_id='start',
    bash_command='echo "hello, it should work" > textfile.txt',
    dag=dag)

compute_pi = SparkSubmitOperator(
    task_id='computepi',
    conn_id='spark_k8s_cluster',
    application='local:///usr/local/spark/examples/jars/spark-example_2.11-2.3.0.jar',
    java_class='org.apache.spark.examples.SparkPi',
    dag=dag,
    executor_config={"KubernetesExecutor": {"image": "spark23:latest"}},
    **submit_config
)

start >> compute_pi
