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
    dag_id='spark', default_args=default_args,
    schedule_interval='@once'
)


# spawn_spark = BashOperator(
#     task_id="spawn_spark", dag=dag, bash_command="sleep 30 && helm init --client-only && helm install --name spark stable/spark:0.1.14",
#     executor_config=executor_config)

# t2 = BashOperator(
#     task_id="process_spark", dag=dag, bash_command="spark-submit --class org.apache.spark.examples.SparkPi --master k8s://https://192.168.39.191:8443 --deploy-mode cluster --executor-memory 1G --num-executors 3 --conf spark.kubernetes.container.image=spark23:latest  local:///$SPARK_HOME/examples/jars/spark-example_2.11-2.3.0.jar",
#     executor_config = {"KubernetesExecutor": {
#         "image": "spark23:latest",
#         "namespace": "airflow-ke"}}
# )

compute_pi = SparkSubmitOperator(
    task_id='computepi',
    conn_id='spark_k8s_cluster',
    application='local:///usr/local/spark/examples/jars/spark-example_2.11-2.3.0.jar',
    java_class='org.apache.spark.examples.WordCount',
    dag=dag,
    executor_config={"KubernetesExecutor": {"image": "spark23:latest"}},
    **submit_config
)


# delete_spark = BashOperator(
#     task_id="delete_spark", dag=dag, bash_command="helm init --client-only && helm delete --purge spark",
#     executor_config=executor_config)

compute_pi 
