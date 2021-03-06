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

from __future__ import print_function
import datetime as dt
import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
import os


default_args = {
    'owner': 'me',
    'start_date': dt.datetime(2018, 7, 10),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

dag = DAG(
    dag_id='example_kubernetes_executor', default_args=default_args,
    schedule_interval='0 0 * * *'
)


def print_stuff():
    print("This is so cool!")


def use_zip_binary():
    rc = os.system("zip")
    assert rc == 0


# You don't have to use any special KubernetesExecutor configuration if you don't want to
start_task = PythonOperator(
    task_id="start_task", python_callable=print_stuff, dag=dag
)

# But you can if you want to
one_task = PythonOperator(
    task_id="one_task", python_callable=print_stuff, dag=dag,
    executor_config={"KubernetesExecutor": {"image": "airflow:latest"}}
)

# Use the zip binary, which is only found in this special docker image
two_task = PythonOperator(
    task_id="two_task", python_callable=use_zip_binary, dag=dag,
    executor_config={"KubernetesExecutor": {"image": "airflow:latest"}}
)

# Limit resources on this operator/task
three_task = PythonOperator(
    task_id="three_task", python_callable=print_stuff, dag=dag,
    executor_config={
        "KubernetesExecutor": {"request_memory": "128Mi", "limit_memory": "128Mi"}}
)

start_task.set_downstream([one_task, two_task, three_task])
