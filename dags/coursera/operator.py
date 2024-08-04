from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from tenacity import retry, stop_after_attempt, wait_exponential
import json
import logging
from datetime import timedelta
import time
import json
import logging

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from plugins.base62 import encoding_url

import concurrent.futures
from bs4 import BeautifulSoup
import re
import requests


class CourseraPreInfoToS3Operator(BaseOperator):
    @apply_defaults
    def __init__(self, bucket_name, pull_prefix, push_prefix, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.pull_prefix = pull_prefix
        self.push_prefix = push_prefix

    def pre_execute(self, context):
        self.s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
        self.today = (context["execution_date"] + timedelta(hours=9)).strftime("%m-%d")
        user_agent = "userMozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36"
        self.chrome_options = Options()
        self.chrome_options.add_argument(f"user-agent={user_agent}")
        self.chrome_options.add_argument("--ignore-ssl-errors=yes")
        self.chrome_options.add_argument("--ignore-certificate-errors")
        self.chrome_options.add_argument("--disable-dev-shm-usage")
        self.chrome_options.add_argument("--no-sandbox")
        self.chrome_options.add_argument("--headless")
        self.chrome_options.add_argument("window-size=1920x1080")
        self.sort_values = ["NEW", "BEST_MATCH"]

    def execute(self, context):
        keywords_json = self.get_keywords_json_file_from_s3()

        data = {}

        for keyword in keywords_json["keywords"]:
            for sort_value in self.sort_values:
                logging.info(f"{keyword}를(을) 검색하고 {sort_value}순으로 탐색합니다.")
                courses_info = self.crawling_course_url(keyword, sort_value)
                for course in courses_info:
                    course_url = course["url"]
                    course_hash = encoding_url(course_url)
                    data[course_hash] = course

        s3_key = self.push_prefix + f"/{self.today}/coursera.json"
        self.upload_json_to_s3(data, self.bucket_name, s3_key)

    def upload_json_to_s3(self, data, bucket_name, key):
        logging.info("S3에 데이터 삽입 시작")
        json_string = json.dumps(data, ensure_ascii=False, indent=4)

        # S3에 JSON 파일 업로드
        self.s3_hook.load_string(
            string_data=json_string, key=key, bucket_name=bucket_name, replace=True
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
    )
    def get_keywords_json_file_from_s3(self):
        file_content = self.s3_hook.read_key(self.pull_prefix, self.bucket_name)
        return json.loads(file_content)

    def crawling_course_url(self, keyword, sort_value):
        with webdriver.Remote(
            "remote_chromedriver:4444/wd/hub", options=self.chrome_options
        ) as driver:
            driver.get(
                f"https://www.coursera.org/search?query={keyword}&language=Korean&productTypeDescription=Courses&sortBy={sort_value}"
            )
            wait = WebDriverWait(driver, 10)
            time.sleep(2)

            sc_cnt = 2 if sort_value == "NEW" else 16
            ele_num = 20 if sort_value == "NEW" else 100

            scroll_location = driver.execute_script("return window.pageYOffset")
            for _ in range(sc_cnt):
                driver.execute_script(
                    "window.scrollTo(0,{})".format(scroll_location + 900)
                )
                time.sleep(2)
                scroll_location = driver.execute_script("return window.pageYOffset")

            course_elements = wait.until(
                EC.presence_of_all_elements_located(
                    (By.XPATH, "//div[contains(@class, 'cds-ProductCard-gridCard')]")
                )
            )
            courses_info = []
            for i, course in enumerate(course_elements[:ele_num]):
                course_link = course.find_element(
                    By.CSS_SELECTOR, "a.cds-CommonCard-titleLink"
                )
                course_url = course_link.get_attribute("href")
                image_element = course.find_element(
                    By.CSS_SELECTOR, ".cds-CommonCard-previewImage img"
                )
                preview_image_src = image_element.get_attribute("src")

                courses_info.append(
                    {
                        "url": course_url,
                        "image": preview_image_src,
                        "sort_by": "RECENT" if sort_value == "NEW" else "RECOMMEND",
                        "keyword": keyword,
                    }
                )
                logging.info(f"{keyword}_{i}_{sort_value} 크롤링 성공 => {course_url}")
            return courses_info


class CourseraInfoToS3Operator(BaseOperator):
    @apply_defaults
    def __init__(self, bucket_name, pull_prefix, push_prefix, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.pull_prefix = pull_prefix
        self.push_prefix = push_prefix

    def pre_execute(self, context):
        self.s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
        self.headers = {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36",
            "Accept-Language": "ko-KR,ko",
        }
        self.today = (context["execution_date"] + timedelta(hours=9)).strftime("%m-%d")

    def execute(self, context):
        uploads = []

        json_file = self.read_json_file_from_s3(self.today)

        count = 0
        for value in json_file.values():
            count += 1
            logging.info(f"{count}번째 데이터를 처리중입니다.")

            lecture_url, thumbnail_url, sort_type, keyword = (
                value["url"],
                value["image"],
                value["sort_by"],
                value["keyword"],
            )
            hashed_url = encoding_url(lecture_url)
            if self.push_prefix == "product":
                parsed_data = self.parsing_course_info(lecture_url)
                parsed_data["thumbnail_url"] = thumbnail_url
                parsed_data = {
                    "lecture_url": lecture_url,
                    "keyword": keyword,
                    "platform_name": "Coursera",
                    "content": parsed_data,
                }
                parsed_data = json.dumps(parsed_data, ensure_ascii=False, indent=4)
                s3_key = f"{self.push_prefix}/{self.today}/{sort_type}/coursera_{hashed_url}.json"
            else:
                parsed_data = self.parsing_course_review(lecture_url)
                s3_key = f"{self.push_prefix}/{self.today}/{hashed_url}.json"
            uploads.append({"content": parsed_data, "key": s3_key})
            time.sleep(1)
        self.upload_to_s3(uploads)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
    )
    def read_json_file_from_s3(self, today):
        file_content = self.s3_hook.read_key(
            self.pull_prefix + f"/{today}" + "/coursera.json", self.bucket_name
        )
        return json.loads(file_content)

    def upload_to_s3(self, uploads):
        logging.info("S3에 데이터 삽입을 시작합니다.")

        def upload_file(data):
            self.s3_hook.load_string(
                string_data=data["content"],
                key=data["key"],
                bucket_name=self.bucket_name,
                replace=True,
            )

        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(upload_file, uploads)

    def parsing_course_info(self, url):
        response = requests.get(url, headers=self.headers)
        soup = BeautifulSoup(response.content, "html.parser")
        course_info = {}

        course_info["lecture_id"] = encoding_url(url)

        title_element = soup.find(
            "h1", class_="cds-119 cds-Typography-base css-1xy8ceb cds-121"
        )
        title = title_element.text.strip() if title_element else None
        course_info["lecture_name"] = title

        course_info["price"] = 0

        description_element = soup.find("div", class_="content-inner")
        description = description_element.text.strip() if description_element else None
        course_info["description"] = description

        content_element = soup.find("ul", class_="cds-9 css-7avemv cds-10")
        content_items = (
            [li.text.strip() for li in content_element.find_all("li")]
            if content_element
            else None
        )
        course_info["what_do_i_learn"] = content_items

        skills_element = soup.find("ul", class_="css-yk0mzy")
        skill_items = (
            [li.text.strip() for li in skills_element.find_all("li")]
            if skills_element
            else None
        )
        course_info["tag"] = skill_items

        teacher_element = soup.find(
            "a", class_="cds-119 cds-113 cds-115 css-wgmz1k cds-142"
        )
        teacher = teacher_element.text.strip() if teacher_element else None
        course_info["teacher"] = teacher

        rating_element = soup.find(
            "div", class_="cds-119 cds-Typography-base css-h1jogs cds-121"
        )
        rating = rating_element.text.strip() if rating_element else None
        course_info["scope"] = float(rating) if rating else 0

        reviews_element = soup.find("p", class_="css-vac8rf")
        review_cnt = (
            re.sub(r"[^\d]", "", reviews_element.text.strip())
            if reviews_element
            else None
        )
        course_info["review_count"] = int(review_cnt) if review_cnt else 0

        ledu_elements = soup.find_all("div", class_="css-fk6qfz")
        if not ledu_elements:
            course_info["lecture_time"] = None
            course_info["level"] = None
            return course_info

        level_element = ledu_elements[0]
        if "수준" in level_element.text:
            level = level_element.text[:2]
            if level == "초급":
                level = "입문"
            elif level == "중급":
                level = "초급"
            elif level == "고등" or level == "믹스":
                level = "중급이상"
            duration_element = ledu_elements[1]
        else:
            level = None
            duration_element = level_element

        duration = (
            re.sub(r"[^\d]", "", duration_element.text.strip())
            if duration_element
            else None
        )
        duration_time = f"{duration}시간 00분" if duration else None
        course_info["lecture_time"] = duration_time
        course_info["level"] = level

        return course_info

    def parsing_course_review(self, url):
        url = f"{url}/reviews?page=1&sort=recent"
        reviews = []
        response = requests.get(url, headers=self.headers)
        soup = BeautifulSoup(response.content, "html.parser")
        review_elements = soup.find_all("div", class_="css-15ee6ym")

        for review in review_elements:
            review_text = review.get_text(strip=True) if review else None
            if (
                review_text
                and len(review_text) >= 10
                and not re.search(r"http[s]?://", review_text)
            ):
                reviews.append(review_text)
            if len(reviews) >= 10:
                break

        data = {
            "lecture_url": url,
            "lecture_id": encoding_url(url),
            "reviews": reviews,
        }
        json_data = json.dumps(data, ensure_ascii=False, indent=4)
        return json_data
