import datetime as dt
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.slack_operator import SlackAPIPostOperator
from airflow.operators.python_operator import PythonOperator
# from kubernetes import client, config

default_args = {
    'owner': 'me',
    'start_date': dt.datetime(2018, 6, 8),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
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


def spark_k8sservices(**context):
    api_instance = client.CoreV1Api(client.ApiClient(config.load_incluster_config()))
    return [(svc.spec.selector, svc.spec.ports[0].node_port) for svc in api_instance.items]


start_notification = SlackAPIPostOperator(
    dag=dag, task_id='start_notification',
    token="xoxp-108470454706-107763802528-394935179685-7cafc5ed8dab3748ce2cc815c49e8cb5",
    channel="#airflow", text='Creating a spark cluster!',
    icon_url='https://airflow.apache.org/_images/pin_large.png')

spawn_spark = BashOperator(
    task_id="spawn_spark",
    dag=dag,
    bash_command="sleep 30 && helm init --client-only && helm install --name spark stable/spark --version=0.1.13 --namespace=spark",
    executor_config=executor_config)

spark_k8sservices = PythonOperator(
    task_id='spark_k8sservices',
    dag=dag,
    python_callable=spark_k8sservices,
    executor_config=executor_config)

send_connections = SlackAPIPostOperator(
    dag=dag, task_id='send_connections',
    provide_context=True,
    token="xoxp-108470454706-107763802528-394935179685-7cafc5ed8dab3748ce2cc815c49e8cb5",
    channel="#airflow", text="{{ ti.xcom_pull(task_ids='spark_k8sservices') }}",
    icon_url='https://airflow.apache.org/_images/pin_large.png')


delete_spark = BashOperator(
    task_id="delete_spark", dag=dag, bash_command="helm init --client-only && helm delete --purge spark",
    executor_config=executor_config)

start_notification >> spawn_spark >> spark_k8sservices >> send_connections >> delete_spark
