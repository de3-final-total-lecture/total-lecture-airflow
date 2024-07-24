from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago
from tenacity import retry, stop_after_attempt, wait_exponential
from datetime import timedelta

import json
import logging
import pendulum

kst = pendulum.timezone("Asia/Seoul")
# 기본 설정
default_args = {
    "owner": "zjacom",
    "depends_on_past": False,
    "start_date": kst.convert(days_ago(1)),
}


def get_all_json_files_from_s3(bucket_name, prefix=""):
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
    keys = s3_hook.list_keys(bucket_name, prefix=prefix)
    json_files = [
        key for key in keys if key.startswith("inflearn") and key.endswith(".json")
    ]
    return json_files


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def read_json_file_from_s3(bucket_name, key):
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
    content = s3_hook.read_key(key, bucket_name)
    return json.loads(content)


def process_s3_json_files(**context):
    execution_date = context["execution_date"]
    korean_time = execution_date
    today = korean_time.strftime("%m-%d")

    mysql_hook = MySqlHook(mysql_conn_id="mysql_conn")
    bucket_name = "team-jun-1-bucket"
    prefix = f"product/{today}"

    # 모든 JSON 파일 목록 가져오기
    json_files = get_all_json_files_from_s3(bucket_name, prefix)

    # 각 JSON 파일 읽기 및 처리
    for json_file in json_files:
        json_content = read_json_file_from_s3(bucket_name, json_file)
        # 여기에서 json_content를 처리하는 로직 추가
        data = json_content["content"]
        lecture_id = data["lecture_id"]
        main_category, mid_category = (
            json_content["main_category"],
            json_content["mid_category"],
        )
        tags = data["tag"]  # 리스트

        get_categories_query = "SELECT category_id, sub_category_name FROM Category WHERE main_category_name = %s and mid_category_name = %s;"
        # 쿼리 실행 및 결과 가져오기
        result = mysql_hook.get_records(
            get_categories_query, parameters=(main_category, mid_category)
        )

        for category_id, sub_category in result:
            for tag in tags:
                if sub_category == tag:
                    insert_category_conn_query = "INSERT INTO Category_conn (lecture_id, category_id) VALUES (%s, %s)"
                    mysql_hook.run(
                        insert_category_conn_query, parameters=(lecture_id, category_id)
                    )


with DAG(
    "inflearn_create_category_conn",
    default_args=default_args,
    description="DAG to insert inflearn category conn data",
    schedule_interval=None,
) as dag:

    process_files = PythonOperator(
        task_id="process_s3_json_files",
        python_callable=process_s3_json_files,
        provide_context=True,
    )

    process_files
