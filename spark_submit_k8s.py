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
    dag_id='spark_submit', default_args=default_args,
    schedule_interval='@once'
)
submit_computepi_on_k8s_submitoperator = SparkSubmitOperator(
    task_id='submit_computepi_on_k8s_submitoperator',
    conn_id='spark_k8s_connection',
    application='local:///usr/local/spark/examples/jars/spark-example_2.11-2.3.0.jar',
    java_class='org.apache.spark.examples.WordCount',
    dag=dag,
    executor_config={"KubernetesExecutor": {"image": "spark:latest"}},
    **submit_config
)

submit_computepi_on_k8s_bash = BashOperator(
    task_id="submit_computepi_on_k8s_bash", dag=dag, bash_command="spark-submit --class org.apache.spark.examples.SparkPi --master k8s://https://192.168.39.69:8443 --deploy-mode cluster --executor-memory 1G --num-executors 3 --conf spark.kubernetes.container.image=spark:latest  local:///$SPARK_HOME/examples/jars/spark-example_2.11-2.3.0.jar",
    executor_config = {"KubernetesExecutor": {
        "image": "spark:latest",
        "namespace": "airflow"}}
)
submit_computepi_on_k8s_bash >> submit_computepi_on_k8s_submitoperator
