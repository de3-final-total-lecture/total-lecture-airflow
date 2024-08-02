from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from custom.mysqlhook import CustomMySqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import timedelta
import logging
import tempfile
import os


class S3ToRDSOperator(BaseOperator):
    @apply_defaults
    def __init__(self, bucket_name, pull_prefix, push_table, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.pull_prefix = pull_prefix
        self.push_table = push_table

    def pre_execute(self, context):
        self.s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
        self.mysql_hook = CustomMySqlHook(mysql_conn_id="mysql_conn")
        self.today = (context["execution_date"] + timedelta(hours=5)).strftime("%m-%d")

    def execute(self, context):
        # S3에서 CSV 파일 다운로드
        prefix = f"{self.pull_prefix}/{self.today}/lecture_info/"
        logging.info(f"S3에서 {prefix} 경로의 CSV 파일을 가져옵니다.")

        files = self.s3_hook.list_keys(bucket_name=self.bucket_name, prefix=prefix)

        if not files:
            logging.warning("다운로드할 파일이 없습니다.")
            return

        for file in files:
            if file.endswith(".csv"):
                with tempfile.NamedTemporaryFile(mode="wb", suffix=".csv") as temp_file:
                    # 2. 각 파일을 로컬에 다운로드
                    self.s3_hook.get_key(file, self.bucket_name).download_fileobj(
                        temp_file
                    )
                    temp_file.flush()

                    # 3. MySQL bulk load 메서드를 사용하여 데이터 삽입
                    self.mysql_hook.bulk_load(self.push_table, temp_file.name, ";")

                logging.info(f"File {file} loaded into {self.push_table}")
