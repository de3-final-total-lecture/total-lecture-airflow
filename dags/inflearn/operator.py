from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from custom.mysqlhook import CustomMySqlHook
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


class InflearnInfoToS3Operator(BaseOperator):
    @apply_defaults
    def __init__(self, bucket_name, pull_prefix, push_prefix, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.pull_prefix = pull_prefix
        self.push_prefix = push_prefix

    def pre_execute(self, context):
        self.s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
        self.today = (context["execution_date"] + timedelta(hours=9)).strftime("%m-%d")

    def execute(self, context):
        uploads = []

        json_file = self.read_json_file_from_s3(self.today)

        count = 0
        for id, value in json_file.items():
            count += 1
            logging.info(f"{count}번째 데이터를 처리중입니다.")
            keyword, sort_type, lecture_url = (
                value["keyword"],
                value["sort_type"],
                value["lecture_url"],
            )
            hashed_url = encoding_url(lecture_url)
            if self.push_prefix == "product":
                parsed_data = self.parsing_lecture_details(
                    id, lecture_url, keyword, sort_type
                )
                s3_key = f"{self.push_prefix}/{self.today}/{sort_type}/inflearn_{hashed_url}.json"
            else:
                parsed_data = self.parsing_lecture_reviews(id, lecture_url)
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
            self.pull_prefix + f"/{today}" + "/inflearn.json", self.bucket_name
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

    def convert_unix_timestamp_to_hours_minutes(self, timestamp):
        total_seconds = int(timestamp)
        # 시간 계산
        hours = total_seconds // 3600
        # 남은 분 계산
        minutes = (total_seconds % 3600) // 60

        # 결과 포맷팅
        if hours > 0 and minutes > 0:
            return f"{hours}시간 {minutes}분"
        elif hours > 0:
            return f"{hours}시간"
        elif minutes > 0:
            return f"{minutes}분"
        else:
            return "0분"

    def parsing_lecture_details(self, id, lecture_url, keyword, sort_type):
        url_v1 = (
            f"https://www.inflearn.com/course/client/api/v1/course/{id}/online/info"
        )
        url_v2 = f"https://www.inflearn.com/course/client/api/v1/course/{id}/contents"
        response = requests.get(url_v1)
        response.raise_for_status()
        try:
            json_data = response.json()
        except:
            logging.info(f"{url_v1}에서 데이터 가져오기를 실패했습니다.")

        data = json_data["data"]
        lecture_thumbnail = data["thumbnailUrl"]
        # is_new = data["isNew"]
        lecture_name = data["title"]
        lecture_time = data["unitSummary"]["runtime"]

        main_category = data["category"]["main"]["title"]
        mid_category = data["category"]["sub"]["title"]

        price = data["paymentInfo"]["payPrice"]
        review_count = data["review"]["count"]
        scope = data["review"]["averageStar"]
        teacher = data["instructors"][0]["name"]

        for ele in data["levels"]:
            if ele["isActive"]:
                level = ele["title"]
                break

        tags = []
        for ele in data["skillTags"]:
            tags.append(ele["title"])

        response = requests.get(url_v2)
        response.raise_for_status()
        try:
            json_data = response.json()
        except:
            logging.info(f"{url_v2}에서 데이터 가져오기를 실패했습니다.")

        description = json_data["data"]["description"]
        what_do_i_learn = json_data["data"]["abilities"]

        data = {
            "lecture_url": lecture_url,
            "keyword": keyword,
            "platform_name": "Inflearn",
            "content": {
                "lecture_id": encoding_url(lecture_url),
                "lecture_name": lecture_name,
                "price": int(price),
                "description": description,
                "what_do_i_learn": what_do_i_learn,
                "tag": tags,
                "level": level,
                "teacher": teacher,
                "scope": float(scope),
                "review_count": int(review_count),
                "lecture_time": self.convert_unix_timestamp_to_hours_minutes(
                    lecture_time
                ),
                "thumbnail_url": lecture_thumbnail,
            },
            "main_category": main_category,
            "mid_category": mid_category,
        }
        json_data = json.dumps(data, ensure_ascii=False, indent=4)
        return json_data

    def parsing_lecture_reviews(self, id, lecture_url):
        url = f"https://www.inflearn.com/api/v2/review/course/{id}?id={id}&pageNumber=1&pageSize=30&sort=RECENT"

        data = {
            "lecture_id": encoding_url(lecture_url),
            "lecture_url": lecture_url,
            "reviews": [],
        }

        response = requests.get(url)
        response.raise_for_status()
        try:
            json_data = response.json()
        except:
            logging.info(url)

        if json_data["data"]["totalCount"] == 0:
            return data

        for item in json_data["data"]["items"]:
            if item["body"]:
                data["reviews"].append(item["body"])

        json_data = json.dumps(data, ensure_ascii=False, indent=4)
        return json_data


class InflearnPreInfoToS3Operator(BaseOperator):
    @apply_defaults
    def __init__(self, bucket_name, pull_prefix, push_prefix, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.pull_prefix = pull_prefix
        self.push_prefix = push_prefix

    def pre_execute(self, context):
        self.s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
        self.mysql_hook = CustomMySqlHook(mysql_conn_id="mysql_conn")
        self.today = (context["execution_date"] + timedelta(hours=9)).strftime("%m-%d")

    def execute(self, context):
        keywords_json = self.get_keywords_json_file_from_s3()

        upload_data, insert_data = {}, []

        for keyword in keywords_json["keywords"]:
            for sort_type, count in [("RECOMMEND", 100), ("RECENT", 20)]:
                logging.info(f"{keyword}를(을) 검색하고 {sort_type}순으로 탐색합니다.")
                url = f"https://www.inflearn.com/courses/client/api/v1/course/search?isDiscounted=false&isNew=false&keyword={keyword}&pageNumber=1&pageSize={count}&sort={sort_type}&types=ONLINE"
                upload_data, insert_data = self.parsing_lecture_id_url(
                    url, sort_type, keyword, upload_data, insert_data
                )
                time.sleep(0.5)
        s3_key = self.push_prefix + f"/{self.today}/inflearn.json"
        self.upload_json_to_s3(upload_data, self.bucket_name, s3_key)
        insert_inflearn_id_query = (
            "INSERT IGNORE INTO Inflearn (lecture_id, inflearn_id) VALUES (%s, %s)"
        )
        self.mysql_hook.bulk_insert(insert_inflearn_id_query, insert_data)

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

    def parsing_lecture_id_url(self, url, sort_type, keyword, upload_data, insert_data):
        response = requests.get(url)
        response.raise_for_status()
        response_data = response.json()

        response_data = response_data["data"]
        if response_data["totalCount"] == 0:
            return upload_data, insert_data

        lectures = response_data["items"]
        for lecture in lectures:
            lecture = lecture["course"]
            inflearn_id = lecture["id"]
            slug = lecture["slug"]
            lecture_url = f"https://www.inflearn.com/course/{slug}"
            upload_data[inflearn_id] = {
                "keyword": keyword,
                "sort_type": sort_type,
                "lecture_url": lecture_url,
            }
            lecture_id = encoding_url(lecture_url)
            insert_data.append((lecture_id, inflearn_id))
        return upload_data, insert_data


class InflearnPriceOperator(BaseOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def pre_execute(self, context):
        self.mysql_hook = CustomMySqlHook(mysql_conn_id="mysql_conn")

    def execute(self, context):
        insert_data, update_data = [], []
        results = self.get_inflearn_id()
        insert_data = self.get_lecture_price(results, insert_data)
        self.load_price_history(insert_data)

    def get_inflearn_id(self):
        get_inflearn_id_query = "SELECT inflearn_id, lecture_id FROM Inflearn"

        results = self.mysql_hook.get_records(get_inflearn_id_query)
        return results

    def get_lecture_price(self, results, insert_data):
        for row in results:
            inflearn_id, lecture_id = row[0], row[1]
            url = f"https://www.inflearn.com/course/client/api/v1/course/{inflearn_id}/online/info"
            response = requests.get(url)
            response.raise_for_status()
            try:
                response_data = response.json()
            except:
                logging.info(f"{url}에서 가격 데이터 가져오기를 실패했습니다.")
            response_data = response_data["data"]
            price = int(response_data["paymentInfo"]["payPrice"])
            if self.is_any_change_to_price:
                update_price_query = f"UPDATE Lecture_info SET price = {price} WHERE lecture_id = '{lecture_id}'"
                self.mysql_hook.run(update_price_query)
                logging.info(
                    f"{lecture_id}의 강의 가격이 {price}로 업데이트 되었습니다."
                )
            insert_data.append((lecture_id, price))
            time.sleep(0.5)
        return insert_data

    def is_any_change_to_price(self, lecture_id, price):
        get_existed_price_query = (
            f"SELECT price FROM Lecture_info WHERE lecture_id = '{lecture_id}'"
        )
        existed_price = int(self.mysql_hook.get_first(get_existed_price_query)[0])
        if existed_price != price:
            return True
        return False

    def load_price_history(self, insert_data):
        insert_lecture_price_query = (
            "INSERT INTO Lecture_price_history (lecture_id, price) VALUES (%s, %s)"
        )
        self.mysql_hook.bulk_insert(insert_lecture_price_query, parameters=insert_data)


class InflearnCategoryConnectionOperator(BaseOperator):
    @apply_defaults
    def __init__(self, bucket_name, pull_prefix, push_table, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.pull_prefix = pull_prefix
        self.push_table = push_table

    def pre_execute(self, context):
        self.s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
        self.mysql_hook = CustomMySqlHook(mysql_conn_id="mysql_conn")
        self.today = (context["execution_date"] + timedelta(hours=9)).strftime("%m-%d")

    def execute(self, context):
        uploads = []
        # 모든 JSON 파일 목록 가져오기
        json_files = self.get_all_json_files_from_s3(self.bucket_name, self.pull_prefix)
        uploads = self.create_category_conn_data(json_files, uploads)
        insert_category_conn_query = f"INSERT IGNORE INTO {self.push_table} (lecture_id, category_id) VALUES (%s, %s)"
        logging.info("RDS에 카테고리 커넥션 데이터 삽입 시작")
        self.mysql_hook.bulk_insert(insert_category_conn_query, uploads)

    def get_all_json_files_from_s3(self, bucket_name, prefix=""):
        prefix = prefix + f"/{self.today}"
        keys = self.s3_hook.list_keys(bucket_name, prefix=prefix)
        json_files = [key for key in keys if key.endswith(".json")]
        return json_files

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def read_json_file_from_s3(self, bucket_name, key):
        if "inflearn" not in key:
            return None

        content = self.s3_hook.read_key(key, bucket_name)
        return json.loads(content)

    def create_category_conn_data(self, json_files, uploads):
        get_etc_category_id_query = (
            "SELECT category_id FROM Category WHERE main_category_name = %s"
        )
        etc_category_id = self.mysql_hook.get_first(
            get_etc_category_id_query, parameters=("기타",)
        )[0]
        # 각 JSON 파일 읽기 및 처리
        for json_file in json_files:
            json_content = self.read_json_file_from_s3(self.bucket_name, json_file)
            if json_content is None:
                continue

            data = json_content["content"]
            lecture_id = data["lecture_id"]
            main_category, mid_category = (
                json_content["main_category"],
                json_content["mid_category"],
            )

            get_categories_query = "SELECT category_id FROM Category WHERE main_category_name = %s and mid_category_name = %s;"
            # 쿼리 실행 및 결과 가져오기
            category_id = self.mysql_hook.get_first(
                get_categories_query, parameters=(main_category, mid_category)
            )
            if category_id is None:
                uploads.append((lecture_id, etc_category_id))
            else:
                category_id = category_id[0]
                uploads.append((lecture_id, category_id))
        return uploads
