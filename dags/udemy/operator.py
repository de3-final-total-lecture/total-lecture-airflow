from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import json
from tenacity import retry, stop_after_attempt, wait_exponential
import concurrent.futures
import logging
from datetime import timedelta
from plugins.base62 import encoding_url
import requests
import json
import logging
from urllib.parse import unquote
from pyudemy.udemy import UdemyAffiliate
from custom.mysqlhook import CustomMySqlHook
import time
from airflow.models import Variable


class UdemyInfoToS3Operator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        bucket_name,
        pull_prefix,
        sort_type,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.pull_prefix = pull_prefix
        self.sort_type = sort_type

    def pre_execute(self, context):
        self.s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
        self.mysql_hook = CustomMySqlHook(mysql_conn_id="mysql_conn")
        self.today = (context["execution_date"] + timedelta(hours=9)).strftime("%m-%d")
        self.client_id = Variable.get("Udemy_CLIENT_ID")
        self.client_secret = Variable.get("Udemy_CLIENT_SECRET")
        self.base_url = Variable.get("BASE_URL")
        self.udemy = UdemyAffiliate(self.client_id, self.client_secret)
        self.auth = requests.auth.HTTPBasicAuth(self.client_id, self.client_secret)
        sort_config = {"newest": ("RECENT", 20), "default": ("RECOMMEND", 100)}
        self.sort_word, self.page_size = sort_config.get(
            self.sort_type, sort_config["default"]
        )
        self.review_params = {
            "page": 1,
            "page_size": 500,
            "rating@min": 3,
        }

    def execute(self, context):
        keywords_json = self.get_keywords_json_file_from_s3()
        uploads, insert_data = [], []
        for keyword in keywords_json["keywords"]:
            main_params = {
                "search": unquote(keyword),
                "language": "ko",
                "ordering": self.sort_type,
                "ratings": 3.0,
                "price": "price-paid",
                "page": 1,
                "page_size": self.page_size,
                "fields[course]": "url,title,price_detail,headline,visible_instructors,image_480x270,instructional_level,description,avg_rating",
            }
            main_results = self.udemy.courses(**main_params)
            for course in main_results["results"]:
                main_json, reviews_json, insert_data = self.get_detail_reviews(
                    course, keyword, insert_data
                )
                if main_json is None and reviews_json is None:
                    continue
                sort_type = "RECENT" if self.sort_type == "newest" else "RECOMMEND"
                hash_url = insert_data[-1][0]
                main_s3_key = f"product/{self.today}/{sort_type}/udemy_{hash_url}.json"
                review_s3_key = f"analytics/reviews/{self.today}/{hash_url}.json"
                uploads.append({"content": main_json, "key": main_s3_key})
                logging.info("강의 내용이 uploads에 저장됩니다.")
                uploads.append({"content": reviews_json, "key": review_s3_key})
                logging.info("강의 리뷰가 uploads에 저장됩니다.")
        logging.info(len(uploads))
        self.upload_to_s3(uploads)
        insert_udemy_id_query = (
            "INSERT IGNORE INTO Udemy (lecture_id, course_id) VALUES (%s, %s)"
        )
        self.mysql_hook.bulk_insert(insert_udemy_id_query, insert_data)

    def common_retry_decorator(func):
        return retry(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=1, min=4, max=10),
        )(func)

    @common_retry_decorator
    def get_keywords_json_file_from_s3(self):
        file_content = self.s3_hook.read_key(self.pull_prefix, self.bucket_name)
        return json.loads(file_content)

    @common_retry_decorator
    def read_json_file_from_s3(self, today):
        file_content = self.s3_hook.read_key(
            self.pull_prefix + f"/{today}" + "/inflearn.json", self.bucket_name
        )
        return json.loads(file_content)

    def upload_to_s3(self, uploads):
        logging.info("S3에 저장합니다.")

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

    def get_detail_reviews(self, course, keyword, insert_data):
        try:
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
            reviews_json = {
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
                },
            }
            if main_json is not None:
                logging.info(f"{keyword}로 검색한 결과, {search_url}강의가 반환됩니다.")
            if reviews_json is not None:
                logging.info(f"{keyword}로 검색한 결과, {search_url}강의가 반환됩니다.")
            main_json_data = json.dumps(main_json, ensure_ascii=False, indent=4)
            review_json_data = json.dumps(reviews_json, ensure_ascii=False, indent=4)
            insert_data.append((hash_url, course_id))
            return main_json_data, review_json_data, insert_data
        except:
            return None, None, insert_data


class UdemyPriceOperator(BaseOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def pre_execute(self, context):
        self.mysql_hook = CustomMySqlHook(mysql_conn_id="mysql_conn")
        client_id = Variable.get("Udemy_CLIENT_ID")
        client_secret = Variable.get("Udemy_CLIENT_SECRET")
        self.udemy = UdemyAffiliate(client_id, client_secret)

    def execute(self, context):
        insert_data = []
        results = self.get_inflearn_id()
        insert_data = self.get_lecture_price(results, insert_data)
        self.load_price_history(insert_data)

    def get_inflearn_id(self):
        get_udemy_id_query = "SELECT course_id, lecture_id FROM Udemy;"
        results = self.mysql_hook.get_records(get_udemy_id_query)
        return results

    def get_lecture_price(self, results, insert_data):
        for row in results:
            course_id, lecture_id = row[0], row[1]
            try:
                detail = self.udemy.course_detail(course_id)
                if self.is_any_change_to_price:
                    update_price_query = f"UPDATE Lecture_info SET price = {price} WHERE lecture_id = '{lecture_id}'"
                    self.mysql_hook.run(update_price_query)
                    logging.info(
                        f"{lecture_id}의 강의 가격이 {price}로 업데이트 되었습니다."
                    )
                insert_data.append((lecture_id, price))
                time.sleep(0.5)
            except Exception as e:
                logging.info(f"{course_id}: {e}")
            price = int(detail["price_detail"]["amount"])
        return insert_data

    def load_price_history(self, insert_data):
        insert_lecture_price_query = (
            "INSERT INTO Lecture_price_history (lecture_id, price) VALUES (%s, %s)"
        )
        self.mysql_hook.bulk_insert(insert_lecture_price_query, parameters=insert_data)

    def is_any_change_to_price(self, lecture_id, price):
        get_existed_price_query = (
            f"SELECT price FROM Lecture_info WHERE lecture_id = {lecture_id}"
        )
        existed_price = int(self.mysql_hook.get_first(get_existed_price_query)[0])
        if existed_price != price:
            return True
        return False
