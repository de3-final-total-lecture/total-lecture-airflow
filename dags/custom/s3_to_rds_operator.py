from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from custom.mysqlhook import CustomMySqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import timedelta
import logging
import tempfile
import os
import pandas as pd


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

    def execute(self, context):
        # S3에서 CSV 파일 다운로드
        prefix = f"{self.pull_prefix}/{self.today}/lecture_info/"
        logging.info(f"S3에서 {prefix} 경로의 CSV 파일을 가져옵니다.")

        try:
            files = self.s3_hook.list_keys(bucket_name=self.bucket_name, prefix=prefix)

            if not files:
                logging.warning("다운로드할 파일이 없습니다.")
                return

            # MySQL에 CSV 로드
            logging.info("CSV 파일을 RDS에 벌크 로드합니다.")

            for file_key in files:
                if file_key.endswith(".csv"):
                    # 임시 파일을 생성합니다.
                    with tempfile.NamedTemporaryFile(
                        delete=False, dir="/tmp"
                    ) as tmp_file:
                        try:
                            # S3에서 임시 파일로 다운로드합니다.
                            logging.info(f"S3에서 {file_key} 파일을 다운로드합니다.")
                            self.s3_hook.download_file(
                                bucket_name=self.bucket_name,
                                key=file_key,
                                local_path=tmp_file.name,
                            )

                            # MySQL에 데이터를 벌크 로드합니다.
                            logging.info(
                                f"파일 {tmp_file.name}을 MySQL에 벌크 로드합니다."
                            )
                            self.mysql_hook.bulk_load(self.push_table, tmp_file.name)
                        finally:
                            # 임시 파일 삭제
                            if os.path.exists(tmp_file.name):
                                logging.info(f"임시 파일 {tmp_file.name}을 삭제합니다.")
                                os.remove(tmp_file.name)

        except Exception as e:
            self.log.error(f"오류가 발생했습니다: {e}")
            raise
