from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago
from tenacity import retry, stop_after_attempt, wait_exponential
from datetime import timedelta

import json
import logging
import pendulum

kst = pendulum.timezone("Asia/Seoul")
# 기본 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": kst.convert(days_ago(1)),
}


def get_all_json_files_from_s3(bucket_name, prefix=""):
    s3_hook = S3Hook(aws_conn_id='aws_s3_connection')
    keys = s3_hook.list_keys(bucket_name, prefix=prefix)
    json_files = [key for key in keys if key.endswith(".json")]
    return json_files


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def read_json_file_from_s3(bucket_name, key):
    s3_hook = S3Hook(aws_conn_id='aws_s3_connection')
    content = s3_hook.read_key(key, bucket_name)
    return json.loads(content)


def execute_select_query(lecture_id):
    # MySQL 연결 설정
    mysql_hook = MySqlHook(mysql_conn_id="mysql_conn")

    # SQL 쿼리 (파라미터화된 버전)
    select_query = "SELECT is_new, is_recommend FROM Lecture_info WHERE lecture_id = %s"

    # 쿼리 실행 및 결과 가져오기
    result = mysql_hook.get_first(select_query, parameters=(lecture_id,))

    if result:
        is_new, is_recommend = result
        return is_new, is_recommend
    else:
        return None, None


def process_s3_json_files(**context):
    execution_date = context["execution_date"]
    korean_time = execution_date - timedelta(hours=12)
    today = korean_time.strftime("%m-%d")

    mysql_hook = MySqlHook(mysql_conn_id="mysql_conn")
    bucket_name = "team-jun-1-bucket"
    prefixes = [f"product/{today}/recommend", f"product/{today}/recent"]

    # 모든 JSON 파일 목록 가져오기
    json_files = []
    for prefix in prefixes:
        json_files.extend(get_all_json_files_from_s3(bucket_name, prefix))
    
    # 각 JSON 파일 읽기 및 처리
    for json_file in json_files:
        json_content = read_json_file_from_s3(bucket_name, json_file)
        logging.info(json_content["lecture_url"])
        # 여기에서 json_content를 처리하는 로직 추가
        data = json_content["content"]
        lecture_id = data["lecture_id"]
        is_new, is_recommend = execute_select_query(lecture_id)        
            
        whatdoilearn_list = data.get("whatdoilearn", [])
        if isinstance(whatdoilearn_list, str):
            whatdoilearn_list = [whatdoilearn_list]
        elif not isinstance(whatdoilearn_list, list):
            whatdoilearn_list = []
        
        tag_list = data.get("tag", [])
        if isinstance(tag_list, str):
            tag_list = [tag_list]
        elif not isinstance(tag_list, list):
            tag_list = []

        whatdoilearn_list = [str(item) if item is not None else '' for item in whatdoilearn_list]
        tag_list = [str(item) if item is not None else '' for item in tag_list]

        if is_new is None and is_recommend is None:
            insert_query = """
                INSERT INTO Lecture_info (lecture_id, lecture_name, price, description, whatdoilearn, tag, teacher, scope, review_count, lecture_time, level, platform_name, thumbnail_url, is_new, is_recommend)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            if "recommend" in json_file:
                insert_data = (
                    data.get("lecture_id"),
                    data.get("lecture_name", ""),
                    data.get("price"),
                    data.get("description",""),
                    "|".join(whatdoilearn_list),
                    "|".join(tag_list),
                    data.get("teacher", ""),
                    data.get("scope"),
                    data.get("review_count"),
                    data.get("lecture_time", ""),
                    data.get("level", ""),
                    json_content.get("platform"),
                    data.get("thumbnail_url", ""),
                    False,
                    True,
                )
                
            elif "recent" in json_file:
                insert_data = (
                    data.get("lecture_id"),
                    data.get("lecture_name", ""),
                    data.get("price"),
                    data.get("description",""),
                    "|".join(whatdoilearn_list),
                    "|".join(tag_list),
                    data.get("teacher", ""),
                    data.get("scope"),
                    data.get("review_count"),
                    data.get("lecture_time", ""),
                    data.get("level", ""),
                    json_content.get("platform"),
                    data.get("thumbnail_url", ""),
                    True,
                    False,
                )
                
            mysql_hook.run(insert_query, parameters=insert_data)
        elif is_recommend == False and "recommend" in json_file:
            # UPDATE 쿼리 준비
            update_query = """
                UPDATE Lecture_info
                SET is_recommend = %s
                WHERE lecture_id = %s
            """
            mysql_hook.run(update_query, parameters=(True, lecture_id))
        elif is_new == False and "recent" in json_file:
            # UPDATE 쿼리 준비
            update_query = """
                UPDATE Lecture_info
                SET is_new = %s
                WHERE lecture_id = %s
            """
            mysql_hook.run(update_query, parameters=(True, lecture_id))
            
        logging.info(f"success : {json_content['lecture_url']} ")



with DAG(
    "s3_to_coursera_info_table",
    default_args=default_args,
    description="DAG to process all JSON files from an S3 bucket",
    schedule_interval=None,
) as dag:

    process_files = PythonOperator(
        task_id="process_s3_json_files",
        python_callable=process_s3_json_files,
        provide_context=True,
    )

    process_files