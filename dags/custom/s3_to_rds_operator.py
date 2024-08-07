from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from custom.mysqlhook import CustomMySqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import timedelta
import logging
import tempfile
import os
import subprocess


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
        self.today = (context["execution_date"] + timedelta(hours=9)).strftime("%m-%d")
        self.connection = self.mysql_hook.get_connection("mysql_conn")

    def execute(self, context):
        # S3에서 CSV 파일 다운로드
        prefix = f"{self.pull_prefix}/{self.today}/lecture_info/"
        logging.info(f"S3에서 {prefix} 경로의 CSV 파일을 가져옵니다.")

        # S3에서 파일 목록 가져오기
        files = self.s3_hook.list_keys(bucket_name=self.bucket_name, prefix=prefix)

        # CSV 파일만 필터링
        csv_files = [f for f in files if f.lower().endswith(".csv")]

        for file in csv_files:
            with tempfile.TemporaryDirectory() as tmpdirname:
                file_path = os.path.join(tmpdirname, f"{self.push_table}.csv")

                self.s3_hook.get_key(file, bucket_name=self.bucket_name).download_file(
                    file_path
                )

                self.mysql_hook.bulk_load(self.push_table, file_path)
                logging.info("성공적으로 저장했습니다.")
