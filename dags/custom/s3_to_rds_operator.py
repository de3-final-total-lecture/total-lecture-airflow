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
        self.today = (context["execution_date"] + timedelta(hours=9)).strftime("%m-%d")

    def execute(self, context):
        # S3에서 CSV 파일 다운로드
        prefix = f"{self.pull_prefix}/{self.today}/lecture_info/"
        logging.info(f"S3에서 {prefix} 경로의 CSV 파일을 가져옵니다.")

        files = self.s3_hook.list_keys(bucket_name=self.bucket_name, prefix=prefix)

        if not files:
            logging.warning("다운로드할 파일이 없습니다.")
            return

        # MySQL에 CSV 로드
        logging.info("CSV 파일을 RDS에 벌크 로드합니다.")

        for file_key in files:
            if file_key.endswith(".csv"):
                # /tmp 디렉토리가 존재하는지 확인하고, 없으면 생성합니다.
                tmp_dir = "/tmp"
                # 임시 파일을 생성합니다.
                try:
                    with tempfile.NamedTemporaryFile(
                        delete=False, dir=tmp_dir, suffix=".csv"
                    ) as tmp_file:
                        tmp_file_path = tmp_file.name  # 파일 경로를 저장합니다.
                        # S3에서 임시 파일로 다운로드합니다.
                        logging.info(f"S3에서 {file_key} 파일을 다운로드합니다.")
                        self.s3_hook.download_file(
                            bucket_name=self.bucket_name,
                            key=file_key,
                            local_path=tmp_file_path,
                        )
                        # MySQL에 데이터를 벌크 로드합니다.
                        logging.info(f"파일 {tmp_file_path}을 MySQL에 벌크 로드합니다.")
                        self.mysql_hook.bulk_load(self.push_table, tmp_file_path)
                except Exception as e:
                    logging.error(f"파일 처리 중 오류 발생: {e}")
