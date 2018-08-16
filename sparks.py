import datetime as dt
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from kubernetes import client, config

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


spawn_spark = BashOperator(
    task_id="spawn_spark", 
    dag=dag, 
    bash_command="sleep 30 && helm init --client-only && helm install --name spark stable/spark --version=0.1.13 --namespace=spark",
    executor_config=executor_config
    )

# t2 = BashOperator(
#     task_id="process_spark", dag=dag, bash_command="spark-submit --class org.apache.spark.examples.SparkPi --master k8s://https://192.168.39.191:8443 --deploy-mode cluster --executor-memory 1G --num-executors 3 --conf spark.kubernetes.container.image=spark23:latest  local:///$SPARK_HOME/examples/jars/spark-example_2.11-2.3.0.jar",
#     executor_config = {"KubernetesExecutor": {
#         "image": "spark23:latest",
#         "namespace": "airflow-ke"}}
# )

def spark_k8sservices(**context):
    api_instance = client.CoreV1Api(client.ApiClient(config.load_incluster_config()))
    api_response = api_instance.list_namespaced_service(namespace="spark", watch=False)
    # all_services = [ sc.api_call("chat.postMessage",channel="#airflow", text=str("Load Balancer Port",svc.spec.ports[0].node_port)) for svc in api_response.items]
    return [(svc.spec.selector, svc.spec.ports[0].node_port) for svc in api_response.items]

spark_k8sservices = PythonOperator(
    task_id='spark_k8sservices',
    dag=dag,
    python_callable=spark_k8sservices,
    executor_config=executor_config)


delete_spark = BashOperator(
    task_id="delete_spark", 
    dag=dag, 
    bash_command="sleep 300 ;helm init --client-only && helm delete --purge spark",
    executor_config=executor_config)

spawn_spark >> spark_k8sservices >> delete_spark
