from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from datetime import timedelta

import pendulum

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

import time
import requests
import logging
import json
import re


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

    chrome_options = Options()
    chrome_options.add_argument("--ignore-ssl-errors=yes")
    chrome_options.add_argument("--ignore-certificate-errors")

    user_agent = "userMozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36"
    chrome_options.add_argument(f"user-agent={user_agent}")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--headless")

    remote_webdriver = "remote_chromedriver"
    with webdriver.Remote(
        f"{remote_webdriver}:4444/wd/hub", options=chrome_options
    ) as driver:
        for keyword in dic["keywords"]:
            for sort_type in ["RECENT", "RECOMMEND"]:
                count = 0
                for page_number in range(1, 10):
                    url = f"https://www.inflearn.com/courses?s={keyword}&sort={sort_type}&page_number={page_number}"
                    driver.get(url)

                    time.sleep(0.5)
                    soup = BeautifulSoup(driver.page_source, "html.parser")

                    is_exist = soup.find("div", {"class": "mantine-ia4qn"})

                    if is_exist and is_exist.text == "찾는 조건의 강의가 없어요.":
                        logging.info(
                            f"{keyword}로 찾을 수 있는 강의가 더 이상 없습니다."
                        )
                        break

                    lectures = soup.find_all(
                        "ul", {"class": "css-y21pja mantine-1avyp1d"}
                    )
                    up_lectures = lectures[0].find_all(
                        "li", {"class": "css-8atqhb mantine-1avyp1d"}
                    )
                    count, data = parsing_lecture_id_url(
                        keyword, count, up_lectures, sort_type, data
                    )
                    if len(lectures) > 1:
                        down_lectures = lectures[1].find_all(
                            "li", {"class": "mantine-1avyp1d"}
                        )
                        count, data = parsing_lecture_id_url(
                            keyword, count, down_lectures, sort_type, data
                        )

    s3_key = f"raw_data/URL/{today}/inflearn_id.json"
    upload_json_to_s3(data, "team-jun-1-bucket", s3_key)


def parsing_lecture_id_url(keyword, count, lectures, sort_type, data):
    for lecture in lectures:
        count += 1
        if sort_type == "RECOMMEND" and count > 100:
            break
        if sort_type == "RECENT" and count > 20:
            break
        a_tag = lecture.find("a")
        lecture_url = a_tag["href"]
        img_tag = lecture.find("img")
        img_url = img_tag[
            "src"
        ]  # https://cdn.inflearn.com/public/courses/334124/cover/5e92d36e-6175-4999-b75f-6f2fa024dcc6/334124.jpg?w=420
        lecture_id = parsing_course_id(img_url)
        if lecture_id:
            data[lecture_id] = {
                "keyword": keyword,
                "sort_type": sort_type,
                "lecture_url": lecture_url,
            }

    return count, data


def parsing_course_id(url):
    match = re.search(r"courses/(\d{6})/", url)
    if match:
        return match.group(1)
    return None


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
