from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from tenacity import retry, stop_after_attempt, wait_exponential
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import concurrent.futures

from datetime import timedelta
import json
import logging
import pendulum
import pymysql
import hashlib

pymysql.install_as_MySQLdb()

kst = pendulum.timezone("Asia/Seoul")


def get_all_json_files_from_s3(bucket_name, prefix=""):
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
    keys = s3_hook.list_keys(bucket_name, prefix=prefix)
    json_files = [key for key in keys if key.endswith(".json")]
    return json_files


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def read_json_file_from_s3(bucket_name, key):
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
    content = s3_hook.read_key(key, bucket_name)
    return json.loads(content)


def _extract_lecture_url_from_s3(bucket_name, pull_prefix, push_prefix, **context):
    execution_date = context["execution_date"]
    korean_time = execution_date
    today = korean_time.strftime("%m-%d")

    temp = []

    # 모든 JSON 파일 목록 가져오기
    json_files = get_all_json_files_from_s3(bucket_name, f"{pull_prefix}/{today}")

    for json_file in json_files:
        json_content = read_json_file_from_s3(bucket_name, json_file)
        url = json_content["url"]
        hashed_url = hashlib.md5(url.encode()).hexdigest()
        s3_key = f"{push_prefix}/{today}/{hashed_url}.json"
        data = {"lecture_url": url, "content": crawling_review_data(url)}
        json_data = json.dumps(data, ensure_ascii=False, indent=4)
        temp.append({"content": json_data, "key": s3_key})
    upload_to_s3(temp)


def crawling_review_data(url):
    url = f"{url}/#reviews"
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
        driver.get(url)
        while True:
            try:
                button = WebDriverWait(driver, 1).until(
                    EC.presence_of_element_located(
                        (
                            By.XPATH,
                            "/html/body/div[1]/main/div/section[2]/div/div/section/div/div[6]/div[2]/div[3]/button",
                        )
                    )
                )
                driver.execute_script("arguments[0].click();", button)
            except:
                break

        return driver.page_source


def _transform_review_data_and_load(bucket_name, pull_prefix, push_prefix, **context):
    execution_date = context["execution_date"]
    korean_time = execution_date
    today = korean_time.strftime("%m-%d")
    reviews = []

    json_files = get_all_json_files_from_s3(bucket_name, f"{pull_prefix}/{today}")

    for json_file in json_files:
        json_content = read_json_file_from_s3(bucket_name, json_file)
        lecture_url, content = json_content["lecture_url"], json_content["content"]
        soup = BeautifulSoup(content, "html.parser")
        hashed_url = hashlib.md5(lecture_url.encode()).hexdigest()
        data = {"lecture_url": lecture_url, "reviews": []}
        s3_key = f"{push_prefix}/{today}/{hashed_url}.json"

        review_elements = soup.find(
            "ul", {"class": "mantine-Stack-root mantine-1178y6y"}
        )
        if review_elements is None:
            logging.info(lecture_url)
            json_data = json.dumps(data, ensure_ascii=False, indent=4)
        else:
            review_list = []
            for ele in review_elements:
                review = ele.find(
                    "p", {"class": "mantine-Text-root css-1l0p3iw mantine-cas3jg"}
                ).text
                if len(review) > 0:
                    review_date = ele.find(
                        "div", {"class": "mantine-Group-root mantine-1esi58d"}
                    ).text
                    review_list.append((review.strip(), review_date))

            review_list = sorted(review_list, key=lambda x: x[1], reverse=True)
            review_list = review_list[:10]
            for review, _ in review_list:
                data["reviews"].append(review)

            json_data = json.dumps(data, ensure_ascii=False, indent=4)
        reviews.append({"content": json_data, "key": s3_key})
    upload_to_s3(reviews)


def upload_to_s3(uploads):
    s3 = S3Hook(aws_conn_id="aws_s3_connection")
    bucket_name = "team-jun-1-bucket"

    def upload_file(data):
        s3.load_string(
            string_data=data["content"],
            key=data["key"],
            bucket_name=bucket_name,
            replace=True,
        )

    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(upload_file, uploads)


# 기본 설정
default_args = {
    "owner": "zjacom",
    "depends_on_past": False,
    "start_date": kst.convert(days_ago(1)),
    "retries": 1,
}

with DAG(
    "load_inflearn_reviews",
    default_args=default_args,
    description="DAG to extract inflearn reviews and load",
    schedule_interval=None,
) as dag:

    extract_lecture_url_from_s3 = PythonOperator(
        task_id="extract_lecture_url_from_s3",
        python_callable=_extract_lecture_url_from_s3,
        provide_context=True,
        op_kwargs={
            "bucket_name": "team-jun-1-bucket",
            "pull_prefix": "inflearn",
            "push_prefix": "raw_data/inflearn/reviews/extract",
        },
    )

    transform_review_data_and_load = PythonOperator(
        task_id="transform_review_data_and_load",
        python_callable=_transform_review_data_and_load,
        provide_context=True,
        op_kwargs={
            "bucket_name": "team-jun-1-bucket",
            "pull_prefix": "raw_data/inflearn/reviews/extract",
            "push_prefix": "raw_data/reviews",
        },
    )

    extract_lecture_url_from_s3 >> transform_review_data_and_load
