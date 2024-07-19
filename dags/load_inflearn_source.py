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
import concurrent.futures
import re
import hashlib


def upload_json_to_s3(uploads):
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


def _get_key_words_from_s3():
    url = "https://online-lecture.s3.ap-northeast-2.amazonaws.com/encoded_keywords.json"
    response = requests.get(url)
    response.raise_for_status()
    json_data = response.json()
    return json_data


# 함수 이름 변경 필요.
# 기능 : 인프런에서 URL 크롤링 | 해당 URL을 사용해서 필요한 정보 파싱 | JSON 객체 생성 | S3에 JSON 저장
def _extract_page_source(**context):
    execution_date = context["execution_date"]
    korean_time = execution_date + timedelta(hours=9)
    today = korean_time.strftime("%m-%d")
    logging.info(today)

    dic = context["ti"].xcom_pull(task_ids="get_key_words")

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
                count, uploads, is_more_lecture = 0, [], True
                for page_number in range(1, 10):
                    if not is_more_lecture:
                        break
                    url = f"https://www.inflearn.com/courses?s={keyword}&sort={sort_type}&page_number={page_number}"
                    driver.get(url)

                    time.sleep(0.5)
                    soup = BeautifulSoup(driver.page_source, "html.parser")

                    is_exist = soup.find("div", {"class": "mantine-ia4qn"})

                    if is_exist and is_exist.text == "찾는 조건의 강의가 없어요.":
                        is_more_lecture = False
                        logging.info("찾는 강의가 없습니다.")
                        break

                    lectures = soup.find_all(
                        "ul", {"class": "css-y21pja mantine-1avyp1d"}
                    )
                    up_lectures = lectures[0].find_all(
                        "li", {"class": "css-8atqhb mantine-1avyp1d"}
                    )
                    down_lectures = lectures[1].find_all(
                        "li", {"class": "mantine-1avyp1d"}
                    )
                    count, uploads = create_json_for_s3(
                        today, keyword, sort_type, up_lectures, count, uploads
                    )
                    count, uploads = create_json_for_s3(
                        today, keyword, sort_type, down_lectures, count, uploads
                    )
                upload_json_to_s3(uploads)


def parsing_data_from_html(url, html_content, sort_type):
    soup = BeautifulSoup(html_content, "html.parser")

    # 강의 제목
    lecture_name = soup.find(
        "h1",
        {"class": "mantine-Text-root mantine-Title-root css-1m4c0vr mantine-17uv248"},
    ).text
    lecture_name = lecture_name.strip()

    # 가격
    price = soup.find(
        "p", {"class": "mantine-Text-root css-6n12g mantine-18jwzem"}
    ).text
    if "월" in price:
        if soup.find("p", {"class": "mantine-Text-root mantine-j1a4p1"}) is None:
            price = soup.find(
                "p", {"class": "mantine-Text-root css-gmb7bs mantine-18jwzem"}
            ).text
        else:
            price = soup.find("p", {"class": "mantine-Text-root mantine-j1a4p1"}).text
    if "무료" in price:
        price = 0
    else:
        price = int(price.translate(str.maketrans("", "", " 원,")).strip())

    # 수준
    level = soup.find(
        "h2",
        {"class": "mantine-Text-root mantine-Title-root css-h8zir mantine-1xk1rww"},
    ).text
    level = level.translate(str.maketrans("", "", " ")).strip()[:2]

    # 강사와 강의 소요시간
    teacher, lecture_time = None, None

    info = soup.find_all("div", {"class": "css-tteyob mantine-ajwi11"})
    for i in range(len(info)):
        if i == 0:
            teacher = re.sub(r"\s+", "", info[i].text)
        if i == 1:
            lecture_time = re.sub(r"\s+", "", info[i].text)

    match = re.search(r"\(([^)]+)\)", lecture_time)
    if match:
        lecture_time = match.group(1)
    else:
        lecture_time = None

    # 태그
    tag_list = []
    tags = soup.find(
        "div", {"class": "mantine-Group-root css-1crf7gu mantine-1vo28iy"}
    ).find_all("span", {"class": "mantine-1356jlc mantine-Badge-inner"})
    for tag in tags:
        tag_list.append(re.sub(r"\s+", "", tag.text))

    # 강의 설명
    descriptions = soup.find("div", {"class": "mantine-Stack-root mantine-caxjnw"})
    description = descriptions.find(
        "p", {"class": "mantine-Text-root mantine-1hql1cd"}
    ).text
    if description is not None:
        description = description.strip()

    # 무엇을 배우나요
    learns = descriptions.find(
        "ul", {"class": "mantine-Stack-root css-82a6rk mantine-1kzvwqj"}
    )
    logging.info(learns)
    what_do_i_learn = []
    if learns is not None:
        for learn in learns:
            what_do_i_learn.append(learn.text.strip())

    # 별점
    scope = soup.find("a", {"class": "mantine-Text-root mantine-ugy335"})
    if scope is not None:
        scope = float(re.sub(r"\s+", "", scope.text)[1:4])
    else:
        scope = None

    # 리뷰수
    review_count = 0
    review_cnt = soup.find(
        "div", {"class": "mantine-Group-root css-xt4uyu mantine-d8k4uu"}
    )
    if review_cnt is not None:
        review_count = int(re.sub(r"\s+", "", review_cnt.text).replace(",", ""))

    # 썸네일 URL
    img_tag = soup.find(
        "div", {"class": "mantine-1iugybl mantine-Image-imageWrapper"}
    ).find("img")
    lecture_thumbnail = img_tag["src"]

    data = {
        "url": url,
        "content": {
            "lecture_id": hashlib.md5(url.encode()).hexdigest(),
            "lecuture_name": lecture_name,
            "price": price,
            "description": description,
            "whatdoilearn": what_do_i_learn,
            "tag": tag_list,
            "level": level,
            "teacher": teacher,
            "scope": scope,
            "review_count": review_count,
            "lecture_time": lecture_time,
            "thumbnail_url": lecture_thumbnail,
            "sort_type": sort_type,
        },
    }
    json_data = json.dumps(data, ensure_ascii=False, indent=4)
    return json_data


def create_json_for_s3(today, keyword, sort_type, lectures, count, uploads):
    for lecture in lectures:
        count += 1
        if sort_type == "RECOMMEND" and count > 100:
            break
        if sort_type == "RECENT" and count > 20:
            break
        a_tag = lecture.find("a")
        lecture_url = a_tag["href"]

        response = requests.get(lecture_url)
        time.sleep(0.5)
        if response.status_code == 200:
            html_content = response.text
            data = parsing_data_from_html(lecture_url, html_content, sort_type)
            s3_key = f"raw_data/inflearn/{keyword}/{sort_type}/{today}/{count}.json"
            uploads.append({"content": data, "key": s3_key})
    return count, uploads


kst = pendulum.timezone("Asia/Seoul")
default_args = {
    "owner": "airflow",
    "start_date": kst.convert(days_ago(1)),
    "retries": 1,
}

with DAG(
    "load_inflearn_source",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    get_key_words_from_s3 = PythonOperator(
        task_id="get_key_words", python_callable=_get_key_words_from_s3
    )

    extract_page_source = PythonOperator(
        task_id="extract_page_source",
        python_callable=_extract_page_source,
        provide_context=True,
    )

    get_key_words_from_s3 >> extract_page_source
