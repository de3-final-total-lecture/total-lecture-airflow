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


class S3ToRDSOperator(BaseOperator):
    @apply_defaults
    def __init__(self, bucket_name, pull_prefix, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.pull_prefix = pull_prefix

    def pre_execute(self, context):
        self.s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
        self.mysql_hook = CustomMySqlHook(mysql_conn_id="mysql_conn")
        self.today = (context["execution_date"] + timedelta(hours=9)).strftime("%m-%d")

    def execute(self, context):
        lecture_info_json_files_key = self.get_all_json_files_key_from_s3(
            self.bucket_name, self.pull_prefix, self.today
        )
        self.transform_lecture_data(self, lecture_info_json_files_key, self.bucket_name)

    def get_all_json_files_key_from_s3(self, bucket_name, prefix, today):
        new_prefix = prefix + "/" + today
        keys = self.s3_hook.list_keys(bucket_name, prefix=new_prefix)
        json_files = [key for key in keys if key.endswith(".json")]
        return json_files

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def read_json_file_from_s3(self, bucket_name, key):
        content = self.s3_hook.read_key(key, bucket_name)
        return json.loads(content)

    def execute_select_query(self, lecture_id):
        select_query = (
            "SELECT is_new, is_recommend FROM Lecture_info WHERE lecture_id = %s"
        )
        # 쿼리 실행 및 결과 가져오기
        result = self.mysql_hook.get_first(select_query, parameters=(lecture_id,))
        if result:
            is_new, is_recommend = result
            return is_new, is_recommend
        else:
            return None, None

    def transform_lecture_data(self, keys, bucket_name):
        for key in keys:
            json_content = self.read_json_file_from_s3(bucket_name, key)

            # transform
            data = json_content["content"]
            lecture_name = data.get("lecture_name")
            teacher = data["teacher"]
            price = data.get("price", 0)
            scope = data.get("scope", 0.0)
            review_count = data["review_count"]
            description = data["description"]
            what_do_i_learn = data.get("what_do_i_learn", [])
            # if isinstance(what_do_i_learn, str):
            #     what_do_i_learn = [whatdoilearn_list]
            # elif not isinstance(whatdoilearn_list, list):
            #     whatdoilearn_list = []

            # whatdoilearn_list = [
            #     str(item) if item is not None else "" for item in whatdoilearn_list
            # ]

            tag_list = data.get("tag", [])
            # if isinstance(tag_list, str):
            #     tag_list = [tag_list]
            # elif not isinstance(tag_list, list):
            #     tag_list = []

            # tag_list = [str(item) if item is not None else "" for item in tag_list]

            lecture_time = data["lecture_time"]
            level = data["level"]
            lecture_id = data["lecture_id"]
            is_new, is_recommend = self.execute_select_query(lecture_id)
            thumbnail_url = data["thumbnail_url"]
            platform_name = json_content["platform_name"]
            if is_new is None and is_recommend is None:
                insert_query = """
                    INSERT INTO Lecture_info (lecture_name, teacher, price, scope, review_count, description, what_do_i_learn, tag, lecture_time, level, lecture_id, thumbnail_url, is_new, is_recommend, platform_name)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                if "RECOMMEND" == data["sort_type"]:
                    insert_data = (
                        lecture_name,
                        teacher,
                        price,
                        scope,
                        review_count,
                        description,
                        "|".join(what_do_i_learn),
                        "|".join(tag_list),
                        lecture_time,
                        level,
                        lecture_id,
                        thumbnail_url,
                        False,
                        True,
                        platform_name,
                    )
                elif "RECENT" == data["sort_type"]:
                    insert_data = (
                        lecture_name,
                        teacher,
                        price,
                        scope,
                        review_count,
                        description,
                        "|".join(what_do_i_learn),
                        "|".join(tag_list),
                        lecture_time,
                        level,
                        lecture_id,
                        thumbnail_url,
                        True,
                        False,
                        platform_name,
                    )
                self.mysql_hook.run(insert_query, parameters=insert_data)

            elif is_recommend == False and "RECOMMEND" == data["sort_type"]:
                # UPDATE 쿼리 준비
                update_query = """
                    UPDATE Lecture_info
                    SET is_recommend = %s
                    WHERE lecture_id = %s
                """
                self.mysql_hook.run(update_query, parameters=(True, lecture_id))
                logging.info(
                    f"{platform_name}에서 {lecture_name}는(은) 최신순, 추천순 모두에 존재합니다."
                )
            elif is_new == False and "RECENT" == data["sort_type"]:
                # UPDATE 쿼리 준비
                update_query = """
                    UPDATE Lecture_info
                    SET is_new = %s
                    WHERE lecture_id = %s
                """
                logging.info(
                    f"{platform_name}에서 {lecture_name}는(은) 최신순, 추천순 모두에 존재합니다."
                )
                self.mysql_hook.run(update_query, parameters=(True, lecture_id))
