from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import json
from tenacity import retry, stop_after_attempt, wait_exponential
import concurrent.futures
import logging
from datetime import timedelta
import time
from plugins.base62 import encoding_url
import requests
import json
import logging
from urllib.parse import unquote
from pyudemy.udemy import UdemyAffiliate
from airflow.models import Variable


class UdemyInfoToS3Operator(BaseOperator):
    @apply_defaults
    def __init__(self, bucket_name, pull_prefix, sort_type, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.pull_prefix = pull_prefix
        self.sort_type = sort_type

    def pre_execute(self, context):
        self.s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
        execution_date = context["execution_date"]
        korean_time = execution_date + timedelta(hours=9)
        self.today = korean_time.strftime("%m-%d")
        CLIENT_ID = Variable.get("Udemy_CLIENT_ID")
        CLIENT_SECRET = Variable.get("Udemy_CLIENT_SECRET")
        self.base_url = Variable.get("BASE_URL")

        self.udemy = UdemyAffiliate(CLIENT_ID, CLIENT_SECRET)
        self.auth = requests.auth.HTTPBasicAuth(CLIENT_ID, CLIENT_SECRET)
        self.page_size = 100
        self.sort_word = "RECOMMEND"
        if self.sort_type == "newest":
            self.page_size = 20
            self.sort_word = "RECENT"
        self.review_params = {
            "page": 1,
            "page_size": 500,
            "rating@min": 3,
        }

    def execute(self, context):
        keywords_json = self.get_keywords_json_file_from_s3()
        uploads = []
        for keyword in keywords_json["keywords"]:
            try:
                main_params = {
                    "search": unquote(keyword),
                    "language": "ko",
                    "ordering": self.sort_type,
                    "ratings": 3.0,
                    "page": 1,
                    "page_size": self.page_size,
                    "fields[course]": "url,title,price_detail,headline,visible_instructors,image_480x270,instructional_level,description,avg_rating",
                }

                count = 0

                main_results = self.udemy.courses(**main_params)
                main_json, reviews_json, hash_url, count = self.func(
                    main_results, keyword, count
                )
                main_s3_key = (
                    f"product/{self.today}/{self.sort_type}/udemy_{hash_url}.json"
                )
                review_s3_key = f"analytics/reviews/{self.today}/{hash_url}.json"
                uploads.append({"content": main_json, "key": main_s3_key})
                uploads.append({"content": reviews_json, "key": review_s3_key})
            except Exception as e:
                logging.info(f"Error processing keyword {unquote(keyword)}: {str(e)}")
        self.upload_to_s3(uploads)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
    )
    def get_keywords_json_file_from_s3(self):
        file_content = self.s3_hook.read_key(self.pull_prefix, self.bucket_name)
        return json.loads(file_content)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
    )
    def read_json_file_from_s3(self, today):
        file_content = self.s3_hook.read_key(
            self.pull_prefix + f"/{today}" + "/inflearn.json", self.bucket_name
        )
        return json.loads(file_content)

    def upload_to_s3(self, uploads):
        def upload_file(data):
            self.s3_hook.load_string(
                string_data=data["content"],
                key=data["key"],
                bucket_name=self.bucket_name,
                replace=True,
            )

        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(upload_file, uploads)

    def convert_format(self, time):
        time = time.replace("총 ", "").replace("시간", "").strip()

        if "분" not in time:
            # "45시간" 또는 "74.5시간" 형태인 경우
            hours = float(time)
            int_hours = int(hours)
            minutes = int((hours - int_hours) * 60)
            if minutes > 0:
                return f"{int_hours}시간 {minutes}분"
            else:
                return f"{int_hours}시간"
        elif "시간" in time and "분" in time:
            # "2시간 30분" 형태인 경우
            hours, minutes = time.split("시간")
            minutes = minutes.replace("분", "").strip()
            return f"{int(hours)}시간 {int(minutes)}분"
        else:
            return time

    def get_source(url):
        response = requests.get(url)
        return response.text

    def get_udemy(self, course_id):
        url = "https://www.udemy.com/api-2.0/discovery-units/"
        params = {
            "context": "clp-bundle",
            "from": "0",
            "page_size": "3",
            "item_count": "12",
            "course_id": course_id,
            "source_page": "course_landing_page",
            "locale": "ko_KR",
            "currency": "krw",
            "navigation_locale": "en_US",
            "skip_price": "true",
            "funnel_context": "landing-page",
        }
        response = requests.get(url, params=params).json()
        return response

    def func(self, main_results, keyword, count):
        for course in main_results["results"]:
            count += 1
            logging.info(
                f"------------------- Start : {unquote(keyword)} ------------------------------"
            )

            search_url = "https://www.udemy.com" + course["url"]
            title = course["title"]

            price = int(course["price_detail"]["amount"])
            headline = course["headline"]

            # Description 추출
            course_id = course["id"]
            course_element = self.get_udemy(course_id)
            description = course_element["units"][0]["items"][0]["objectives_summary"]

            teacher = course["visible_instructors"][0]["display_name"]
            scope = round(course["avg_rating"], 1)
            img_url = course["image_480x270"]

            lecture_time_element = course_element["units"][0]["items"][0][
                "content_info"
            ]
            lecture_time = self.convert_format(lecture_time_element)

            level = course["instructional_level"]
            if level == "All Levels":
                level = "All"
            elif level == "Beginner Level":
                level = "입문"
            elif level == "Intermediate Level":
                level = "초급"
            else:
                level = "중급이상"

            # Review
            review_endpoint = f"{self.base_url}/courses/{course_id}/reviews/"
            response = requests.get(
                review_endpoint, auth=self.auth, params=self.review_params
            )
            courses = response.json()
            review_cnt = courses["count"]

            hash_url = encoding_url(search_url)

            rating_5 = []
            rating_4 = []
            rating_3 = []

            for course_detail in courses["results"]:
                if course_detail["content"] and "http" not in course_detail["content"]:
                    if int(course_detail["rating"]) == 5 and len(rating_5) < 7:
                        rating_5.append(course_detail["content"])
                    if 4 <= course_detail["rating"] <= 4.5 and len(rating_4) < 2:
                        rating_4.append(course_detail["content"])
                    if 3 <= course_detail["rating"] <= 3.5 and len(rating_3) < 1:
                        rating_3.append(course_detail["content"])
                    if len(rating_5) == 7 and len(rating_4) == 2 and len(rating_3) == 1:
                        break

            all_reviews = rating_5 + rating_4 + rating_3
            reveiws_json = {
                "lecture_id": hash_url,
                "lecture_url": search_url,
                "reviews": all_reviews,
            }

            main_json = {
                "lecture_url": search_url,
                "keyword": keyword,
                "course_id": course_id,
                "platform_name": "Udemy",
                "content": {
                    "lecture_id": hash_url,
                    "lecture_name": title,
                    "price": price,
                    "description": headline,
                    "what_do_i_learn": description,
                    "tag": [],
                    "level": level,
                    "teacher": teacher,
                    "scope": scope,
                    "review_count": review_cnt,
                    "lecture_time": lecture_time,
                    "thumbnail_url": img_url,
                    "sort_type": self.sort_word,
                },
            }
            logging.info(
                f"------------------- END : {unquote(keyword)}_{count} ------------------------------"
            )
            return main_json, reveiws_json, hash_url, count
