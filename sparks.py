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
    'start_date': dt.datetime(2018, 4, 4),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

dag = DAG(
    dag_id='spark', default_args=default_args,
    schedule_interval='0 0 * * *'
)


t1 = BashOperator(
    task_id="spawn_spark", dag=dag, bash_command="sleep 30 && helm init --client-only && helm install --name spark http://filled-echidna-helmet-ch:1323/charts/spark-2.3.0.tgz",
    executor_config={"KubernetesExecutor": {
        "image": "lachlanevenson/k8s-helm:latest"}}
)

t2 = BashOperator(
    task_id="process_spark", dag=dag, bash_command="sleep 60 && spark-submit --conf spark.driver.host=$(hostname -i) --master spark://spark-0.spark:7077 /tmp/dags/wordcount.py hdfs://hadoop-0.hadoop:9000/data/README.txt hdfs://hadoop-0.hadoop:9000/data/wordcount || sleep 600",
    executor_config={"KubernetesExecutor": {
        "image": "airflow/spark:latest"}}
)


t1 >> t2

