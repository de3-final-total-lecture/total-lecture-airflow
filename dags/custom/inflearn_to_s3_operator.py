from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import hashlib
import json
from tenacity import retry, stop_after_attempt, wait_exponential
import concurrent.futures
import logging
from datetime import timedelta
import time


class InflearnToS3Operator(BaseOperator):

    @apply_defaults
    def __init__(
        self, bucket_name, pull_prefix, push_prefix, process_func, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.pull_prefix = pull_prefix
        self.push_prefix = push_prefix
        self.process_func = process_func

    def pre_execute(self, context):
        self.s3_hook = S3Hook(
            aws_conn_id="aws_s3_connection"  # .config.py 같은 곳에 상수로 지정
        )

    def execute(self, context):
        execution_date = context["execution_date"]
        korean_time = execution_date - timedelta(hours=28)
        today = korean_time.strftime("%m-%d")

        uploads = []

        json_file = self.read_json_file_from_s3(today)

        count = 0
        for id, value in json_file.items():
            count += 1
            logging.info(f"{count}번째 데이터를 처리중입니다.")
            keyword, sort_type, lecture_url = (
                value["keyword"],
                value["sort_type"],
                value["lecture_url"],
            )
            parsed_data = self.process_func(id, lecture_url, keyword, sort_type)
            hashed_url = hashlib.md5(lecture_url.encode()).hexdigest()
            s3_key = f"{self.push_prefix}/{today}/{hashed_url}.json"
            uploads.append({"content": parsed_data, "key": s3_key})
            time.sleep(1)
        logging.info("S3에 데이터 삽입을 시작합니다.")
        self.upload_to_s3(uploads)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
    )
    def read_json_file_from_s3(self, today):
        file_content = self.s3_hook.read_key(
            self.pull_prefix + f"/{today}" + "/lecture_id.json", self.bucket_name
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
