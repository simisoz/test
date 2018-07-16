# import datetime as dt
# from airflow import DAG
# from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
# from airflow.operators.bash_operator import BashOperator
# from airflow.operators.sensors import S3KeySensor

# default_args = {
#     'owner': 'me',
#     'start_date': dt.datetime(2018, 6, 8),
#     'retries': 1,
#     'retry_delay': dt.timedelta(minutes=5),
# }
# submit_config = {
#     'conf': {
#         'spark.kubernetes.container.image': 'spark23:latest'
#     },
#     'total_executor_cores': 4,
#     'executor_cores': 1,
#     'executor_memory': '1g',
#     'name': '{{ task_instance.task_id }}',
#     'num_executors': 4,
#     'verbose': True,
#     'driver_memory': '1g',
# }
# executor_config = {
#     "KubernetesExecutor": {
#         "image": "helm:latest"
#     }
# }

# dag = DAG(
#     dag_id='spark', default_args=default_args,
#     schedule_interval='@once'
# )


# # spawn_spark = BashOperator(
# #     task_id="spawn_spark", dag=dag, bash_command="sleep 30 && helm init --client-only && helm install --name spark stable/spark:0.1.14",
# #     executor_config=executor_config)

# # t2 = BashOperator(
# #     task_id="process_spark", dag=dag, bash_command="spark-submit --class org.apache.spark.examples.SparkPi --master k8s://https://192.168.39.191:8443 --deploy-mode cluster --executor-memory 1G --num-executors 3 --conf spark.kubernetes.container.image=spark23:latest  local:///$SPARK_HOME/examples/jars/spark-example_2.11-2.3.0.jar",
# #     executor_config = {"KubernetesExecutor": {
# #         "image": "spark23:latest",
# #         "namespace": "airflow-ke"}}
# # )
# sensor = S3KeySensor(
#     task_id='watch_s3_bucket',
#     bucket_key='*.JPEG',
#     wildcard_match=True,
#     bucket_name='image',
#     s3_conn_id='minio',
#     timeout=18*60*60,
#     poke_interval=20,
#     dag=dag)

# # s3_files = S3ListOperator(
# #     task_id='list_bucket',
# #     bucket='images',
# #     prefix='/',
# #     delimiter=',',
# #     aws_conn_id='minio'
# # )

# sensor


# # s3_files

# # compute_pi = SparkSubmitOperator(
# #     task_id='computepi',
# #     conn_id='spark_k8s_cluster',
# #     application='local:///usr/local/spark/examples/jars/spark-example_2.11-2.3.0.jar',
# #     java_class='org.apache.spark.examples.SparkPi',
# #     dag=dag,
# #     executor_config={"KubernetesExecutor": {"image": "spark23:latest"}},
# #     **submit_config
# # )


# # delete_spark = BashOperator(
# #     task_id="delete_spark", dag=dag, bash_command="helm init --client-only && helm delete --purge spark",
# #     executor_config=executor_config)

# # compute_pi

from airflow import DAG
from airflow.operators import SimpleHttpOperator, HttpSensor,   BashOperator, EmailOperator, S3KeySensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2016, 11, 1),
    'email': ['something@here.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('s3_dag_test', default_args=default_args, schedule_interval='@once')

t1 = BashOperator(
    task_id='bash_test',
    bash_command='echo "hello, it should work" > s3_conn_test.txt',
    dag=dag)

sensor = S3KeySensor(
    task_id='watch_s3_bucket',
    bucket_key='*.JPEG',
    wildcard_match=True,
    bucket_name='image',
    s3_conn_id='minio',
    timeout=18*60*60,
    poke_interval=20,
    dag=dag)

t1 >> sensor
