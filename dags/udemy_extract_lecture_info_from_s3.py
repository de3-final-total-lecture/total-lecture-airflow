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

'''
product/{timestamp}/flatform_해시url.json
'''

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
    logging.info("함수 시작")
    # execution_date = context["execution_date"]
    # korean_time = execution_date + timedelta(hours=9)
    # today = korean_time.strftime("%m-%d")
    logging.info('mysql 접속 시도')
    mysql_hook = MySqlHook(mysql_conn_id="mysql_conn")
    bucket_name = "team-jun-1-bucket"
    # prefixes = [f"product/{today}/RECOMMEND", f"product/{today}/RECENT"]
    prefixes = [f"product/07-29/RECOMMEND", f"product/07-29/RECENT"]
    logging.info(prefixes)
    # 모든 JSON 파일 목록 가져오기
    json_files = []
    logging.info('json가져오기 시작')
    for prefix in prefixes:
        json_files.extend(get_all_json_files_from_s3(bucket_name, prefix))
    # 각 JSON 파일 읽기 및 처리
    logging.info(json_files)
    logging.info('json파일 읽기 시작')
    for json_file in json_files:
        json_content = read_json_file_from_s3(bucket_name, json_file)
        logging.info('data 가져옴')
        # 여기에서 json_content를 처리하는 로직 추가
        data = json_content["content"]
        lecture_name = data.get('lecture_name')
        platform_name = json_content['platform_name']
        teacher = data['teacher']
        price = data.get('price', 0)
        scope = data.get('scope', 0.0)
        review_count = data["review_count"]
        description = data["description"]
        course_id = json_content['course_id']
        whatdoilearn_list = data.get("what_do_i_learn", [])
        if isinstance(whatdoilearn_list, str):
            whatdoilearn_list = [whatdoilearn_list]
        elif not isinstance(whatdoilearn_list, list):
            whatdoilearn_list = []
        
        whatdoilearn_list = [str(item) if item is not None else '' for item in whatdoilearn_list]
        
        tag_list = data.get("tag", [])
        if isinstance(tag_list, str):
            tag_list = [tag_list]
        elif not isinstance(tag_list, list):
            tag_list = []
        
        tag_list = [str(item) if item is not None else '' for item in tag_list]        
        
        lecture_time = data['lecture_time']
        level = data['level']
        
        lecture_id = data["lecture_id"]
        is_new, is_recommend = execute_select_query(lecture_id)
        
        thumbnail_url = data['thumbnail_url']
        # review_count = data.get("review_count", 0)
        # try:
        #     price = int(price)
        # except (ValueError, TypeError):
        #     price = 0
        # try:
        #     scope = float(scope)
        # except (ValueError, TypeError):
        #     scope = 0.0
        # try:
        #     review_count = int(review_count)
        # except (ValueError, TypeError):
        #     review_count = 0
        logging.info(f'lecture_id: {lecture_id}')
        logging.info(f"is_new: {is_new}")
        logging.info(f"is_recommend: {is_recommend}")
        if is_new is None and is_recommend is None:
            insert_query = """
                INSERT INTO Lecture_info (lecture_name, platform_name, teacher, price, scope, review_count, description, what_do_i_learn, tag, lecture_time, level, lecture_id, thumbnail_url, is_new, is_recommend)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            udemy_query = """
                INSERT INTO Udemy (lecture_id, course_id) VALUES(%s, %s)
            """
            if "RECOMMEND" == data['sort_type']:
                insert_data = (
                    lecture_name,
                    platform_name,
                    teacher,
                    price,
                    scope,
                    review_count,
                    description,
                    "|".join(whatdoilearn_list),
                    "|".join(tag_list),
                    lecture_time,
                    level,
                    lecture_id,
                    thumbnail_url,
                    False,
                    True
                )
                udemy_data = (
                    lecture_id,
                    course_id
                )
            elif "RECENT" == data["sort_type"]:
                insert_data = (
                    lecture_name,
                    platform_name,
                    teacher,
                    price,
                    scope,
                    review_count,
                    description,
                    "|".join(whatdoilearn_list),
                    "|".join(tag_list),
                    lecture_time,
                    level,
                    lecture_id,
                    thumbnail_url,
                    True,
                    False
                ) 
                udemy_data = (
                    lecture_id,
                    course_id
                )
            mysql_hook.run(insert_query, parameters=insert_data)
            mysql_hook.run(udemy_query, parameters=udemy_data)
            logging.info(f"{insert_query}, {insert_data}")
            
        elif is_recommend == False and "RECOMMEND" == data["sort_type"]:
            # UPDATE 쿼리 준비
            update_query = """
                UPDATE Lecture_info
                SET is_recommend = %s
                WHERE lecture_id = %s
            """
            mysql_hook.run(update_query, parameters=(True, lecture_id))
        elif is_new == False and "RECENT" == data["sort_type"]:
            # UPDATE 쿼리 준비
            update_query = """
                UPDATE Lecture_info
                SET is_new = %s
                WHERE lecture_id = %s
            """
            mysql_hook.run(update_query, parameters=(True, lecture_id))
        
with DAG(
    "udemy_s3_json_file_processing",
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