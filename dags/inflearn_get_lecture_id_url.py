from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from datetime import timedelta

import pendulum

import time
import requests
import logging
import json


def upload_json_to_s3(data, bucket_name, key):
    # S3Hook 초기화
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    # JSON 데이터를 문자열로 변환
    json_string = json.dumps(data, ensure_ascii=False, indent=4)

    # S3에 JSON 파일 업로드
    s3_hook.load_string(
        string_data=json_string, key=key, bucket_name=bucket_name, replace=True
    )


def _get_key_words_from_s3():
    url = "https://online-lecture.s3.ap-northeast-2.amazonaws.com/encoded_keywords.json"
    response = requests.get(url)
    response.raise_for_status()
    json_data = response.json()
    return json_data


def _extract_lecture_id_url(**context):
    execution_date = context["execution_date"]
    korean_time = execution_date
    today = korean_time.strftime("%m-%d")
    logging.info(today)

    dic = context["ti"].xcom_pull(task_ids="get_key_words")
    data = {}

    for keyword in dic["keywords"]:
        for sort_type, count in [("RECOMMEND", 100), ("RECENT", 20)]:
            logging.info(f"{keyword}으로 검색한 결과를 {sort_type}순으로 가져옵니다.")
            url = f"https://www.inflearn.com/courses/client/api/v1/course/search?isDiscounted=false&isNew=false&keyword={keyword}&pageNumber=1&pageSize={count}&sort={sort_type}&types=ONLINE"
            data = parsing_lecture_id_url(url, sort_type, keyword, data)
            time.sleep(0.5)
    s3_key = f"raw_data/URL/{today}/inflearn_id.json"
    upload_json_to_s3(data, "team-jun-1-bucket", s3_key)


def parsing_lecture_id_url(url, sort_type, keyword, data):
    response = requests.get(url)
    response.raise_for_status()
    response_data = response.json()

    response_data = response_data["data"]
    if response_data["totalCount"] == 0:
        return data

    lectures = response_data["items"]
    for lecture in lectures:
        lecture = lecture["course"]
        lecture_id = lecture["id"]
        slug = lecture["slug"]
        lecture_url = f"https://www.inflearn.com/course/{slug}"
        data[lecture_id] = {
            "keyword": keyword,
            "sort_type": sort_type,
            "lecture_url": lecture_url,
        }
    return data


kst = pendulum.timezone("Asia/Seoul")
default_args = {
    "owner": "zjacom",
    "start_date": kst.convert(days_ago(1)),
    "retries": 3,
}

with DAG(
    "inflearn_get_lecture_id_url",
    default_args=default_args,
    description="DAG to load inflearn lecture id and url.",
    schedule_interval=None,
) as dag:
    get_key_words_from_s3 = PythonOperator(
        task_id="get_key_words", python_callable=_get_key_words_from_s3
    )

    extract_lecture_id_url = PythonOperator(
        task_id="extract_lecture_id_url",
        python_callable=_extract_lecture_id_url,
        provide_context=True,
    )

    get_key_words_from_s3 >> extract_lecture_id_url
