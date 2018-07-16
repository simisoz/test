from airflow import DAG
from airflow.operators.sensors import S3KeySensor
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
os.environ['S3_USE_SIGV4'] = 'True'
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 7, 16),
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
    aws_conn_id='s3_minio',
    timeout=18*60*60,
    poke_interval=20,
    dag=dag)

t1 >> sensor
