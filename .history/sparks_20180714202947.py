# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from airflow.operators.bash_operator import BashOperator
from airflow import DAG
import datetime as dt


default_args = {
    'owner': 'me',
    'start_date': dt.datetime(2018, 6, 8),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

dag = DAG(
    dag_id='spark', default_args=default_args,
    schedule_interval='@once'
)


t1 = BashOperator(
    task_id="spawn_spark", dag=dag, bash_command="sleep 30 && helm init --client-only && helm install --name spark stable/spark",
    executor_config={"KubernetesExecutor": {
        "image": "helm:latest"}}
)

t3 = BashOperator(
    task_id="delete_spark", dag=dag, bash_command="helm init --client-only && helm delete --purge spark",
    executor_config={"KubernetesExecutor": {
        "image": "helm:latest"}}
)

t2 = BashOperator(
    task_id="process_spark", dag=dag, bash_command="spark-submit --class org.apache.spark.examples.SparkPi --master https://192.168.39.191:8443 --deploy-mode cluster --executor-memory 1G --num-executors 3 $SPARK_HOME/examples/jars/spark-example_2.11-2.3.0.jar",
    executor_config={"KubernetesExecutor": {
        "image": "spark23:latest"}}
)


t1 >> t2

