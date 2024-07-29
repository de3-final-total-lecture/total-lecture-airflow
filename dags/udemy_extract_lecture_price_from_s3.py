from airflow import DAG
# from airflow import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from tenacity import retry, stop_after_attempt, wait_exponential
from datetime import timedelta

from pyudemy.udemy import UdemyAffiliate
import pendulum
import time
import requests
import logging
import json
import requests

kst = pendulum.timezone("Asia/Seoul")

def udemy_api(CLIENT_ID, CLIENT_SECRET, course_id):
    udemy = UdemyAffiliate(CLIENT_ID, CLIENT_SECRET)
    detail = udemy.course_detail(course_id)
    return int(detail['price_detail']['amount'])

def get_udemy_json_files_from_s3(bucket_name, prefix):
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
    keys = s3_hook.list_keys(bucket_name, prefix=prefix)
    udemy_json_files = [key for key in keys if key.startswith("udemy") and key.endswith(".json")]
    return udemy_json_files

# 지정한 특정 JSON파일만 서칭
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def read_json_file_from_s3(bucket_name, key):
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
    content = s3_hook.read_key(key, bucket_name)
    return json.loads(content)

def _get_lecture_price(sort_word, **context):
    CLIENT_ID = Variable.get('Udemy_CLIENT_ID')
    CLIENT_SECRET = Variable.get('Udemy_CLIENT_SECRET')

    execution_date = context["execution_date"]
    korean_time = execution_date + timedelta(hours=9)
    today = korean_time.strftime("%m-%d")

    mysql_hook = MySqlHook(mysql_conn_id="mysql_conn")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
    bucket_name = "team-jun-1-bucket"
    prefix = f"product/{today}/{sort_word}"

    json_files = get_udemy_json_files_from_s3(bucket_name, prefix)

    for json_file in json_files:
        file_content = s3_hook.read_key(json_file, bucket_name)
        data = json.loads(file_content)

        course_id = data.get("course_id")
        lecture_id = data.get("content", {}).get("lecture_id")

        price = udemy_api(CLIENT_ID, CLIENT_SECRET, course_id)

        insert_lecture_price_history_query = (
            "INSERT INTO Lecture_price_history (lecture_id, price) VALUES (%s, %s)"
        )   
        mysql_hook.run(
            insert_lecture_price_history_query, parameters=(lecture_id, price)
        )
        time.sleep(0.5)


default_args = {
    "owner": "airflow",
    "start_date": kst.convert(days_ago(1)),
}

with DAG(
    "udemy_get_lecture_price",
    default_args=default_args,
    description="DAG to get lecture price.",
    schedule_interval=None,
) as dag:
    extract_lecture_price_most = PythonOperator(
        task_id="extract_lecture_price_most",
        python_callable=_get_lecture_price,
        op_kwargs={'sort_word':'RECENT'},
        provide_context=True,
    )
    extract_lecture_price_new = PythonOperator(
        task_id="extract_lecture_price_new",
        python_callable=_get_lecture_price,
        op_kwargs={'sort_word':'RECOMMEND'},
        provide_context=True,
    )

    extract_lecture_price_most >> extract_lecture_price_new