from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from custom.mysqlhook import CustomMySqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from openai import OpenAI
import json
from tenacity import retry, stop_after_attempt, wait_exponential
import logging
from datetime import timedelta, datetime
import time
import json
import logging
import pendulum
from copy import deepcopy


class OpenAICategoryConnectionOperator(BaseOperator):
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
        self.open_ai_key = Variable.get("openai_api_key")

    def execute(self, context):
        """
        작업 플로우:
        1. RDS에서 Categorized 된 강의 목록 가져오기
        2. 오늘 처리할 데이터 중 Categorized 된 데이터 빼고 가져오기 (Udemy, Coursera)
        3. ChatGPT를 사용해서 처리하기
        4. S3에 모든 내용 저장하기
        5. RDS Category_conn에 적재
        LLM 시스템 프롬프트는 직접 넣거나 이미 존재하는 것 사용...
        카테고리는 여기서 변경 가능. 시스템을 변경시엔 새로 LLM 모델 만들기
        """
        prefix = self.pull_prefix + f"/{self.today}"

        uncategorized_datas = self.get_processed_uncategorized_datas(
            self.bucket_name, prefix
        )

        # ChatGPT를 통한 카테고리 매핑
        total_answer_history, question_and_answer, failed_lectures = (
            self.categorize_by_openai(uncategorized_datas, self.open_ai_key)
        )

        # LLM의 응답 파싱 json으로 파싱 및 db에 저장할 형식으로 처리 후 DB에 저장
        category_processed, insert_datas = self.processing_question_and_answer(
            question_and_answer
        )
        self.insert_to_mysql(insert_datas)

        # LLM 결과를 기록을 위해 s3에 저장
        self.save_llm_operation_result_to_s3(
            category_processed,
            failed_lectures,
            total_answer_history,
            question_and_answer,
        )

    def get_all_json_files_from_s3(self, bucket_name, prefix=""):
        keys = self.s3_hook.list_keys(bucket_name, prefix=prefix)
        logging.info(f"keys: {keys}")
        json_files = [
            key
            for key in keys
            if key.endswith(".json") and not key.split("/")[-1].startswith("inflearn")
        ]
        return json_files

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def read_json_file_from_s3(self, bucket_name, key):
        content = self.s3_hook.read_key(key, bucket_name)
        return json.loads(content)

    def get_result_from_mysql(self, query):
        conn = self.mysql_hook.get_conn()
        result = None
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
        finally:
            cursor.close()
            conn.close()

        return result

    def read_categorized_datas_from_mysql(self):
        query = """
            SELECT * FROM Category_conn
        """
        result = self.get_result_from_mysql(query)

        categorized_lecture_id_set = set()
        for row in result:
            categorized_lecture_id_set.add(row[0])

        return categorized_lecture_id_set

    def read_categories_from_mysql(self, mysql_hook):
        query = """
            SELECT main_category_name, mid_category_name FROM Category
        """
        result = self.get_result_from_mysql(query)

        category_set = set()
        category_str = ""
        for idx, elem in enumerate(result):
            if f"{elem[0]}-{elem[1]}" not in category_set:
                category_str += f"{idx+1}: [{elem[0]}-{elem[1]}]\n"
                category_set.add(f"{elem[0]}-{elem[1]}")

        for row in result:
            category_set.add(row[1:3])

        return category_set, category_str

    def processing_lecture_data_for_llm(self, json_content):
        content = json_content["content"]
        llm_input = {}
        need_columns = ["lecture_id", "lecture_name", "what_do_i_learn", "tag"]
        for col in need_columns:
            if content[col]:
                llm_input[col] = content[col]
        return llm_input

    def get_processed_uncategorized_datas(self, bucket_name, prefix):
        """
        s3에서 오늘 처리할 데이터를 가져오고,
        mysql의 데이터와 비교해 처리 안된 강의만 가져오는 함수
        """
        # DB에서 Categorized된 강의 목록 가져오기
        categorized_lecture_id_set = self.read_categorized_datas_from_mysql()

        # 모든 JSON 파일 목록 가져오기
        logging.info("get all json files from s3 except inflearn")
        json_files = self.get_all_json_files_from_s3(bucket_name, prefix)
        logging.info(f"json files: {json_files[:3]}")
        uncategorized_datas = []

        for json_file in json_files:
            lecture_id = json_file.split("_")[-1].split(".")[0]

            # 이미 카테고리화 된 경우는 continue
            if lecture_id not in categorized_lecture_id_set:
                # s3에서 읽어온 후 llm에 넣을 데이터만 추출
                json_content = self.read_json_file_from_s3(bucket_name, json_file)
                processed_content = self.processing_lecture_data_for_llm(json_content)
                uncategorized_datas.append(processed_content)
                logging.info(f"{lecture_id} is not categorized!")

                # 중복으로 들어오는 데이터 체크를 위해 일단 처리된 데이터에 추가
                categorized_lecture_id_set.add(lecture_id)

        return uncategorized_datas

    def get_openai_client(self, api_key):
        return OpenAI(api_key=api_key)

    def get_open_ai_assistant(self, openai_client):
        if len(openai_client.beta.assistants.list().data) == 0:
            logging.info("create new assistant")
            category_assist = openai_client.beta.assistants.create(
                # JSON 형식으로 답변을 위한 개선된 프롬프트
                instructions="You are an assistant tasked with mapping lecture data to one of 25 categories. A list of these categories will be provided to you. Subsequently, you will receive lecture data in the format 'Lecture 1: {information}, Lecture 2: {information},' separated by commas. Your task is to assign a category to each lecture. Respond in a JSON format that is easy to parse. Each answer should have a problem number as the key, and the value should be another JSON object containing 'describe' for the reason and 'ans' for the correct category number. If the data provided is not suitable for categorization, set 'ans' to -1. Additionally, due to cost concerns, ensure that each 'describe' entry is no longer than three sentences.",
                name="Category Assistant",
                model="gpt-3.5-turbo",
            )

        else:
            logging.info("load assistant")
            category_assist = openai_client.beta.assistants.list().data[0]

        return category_assist

    def get_message_from_openai(self, message_content, openai_client, category_assist):
        # 대화를 위한 스레드와 질문 메시지 생성
        thread = openai_client.beta.threads.create()
        message = openai_client.beta.threads.messages.create(
            thread_id=thread.id, role="user", content=message_content, timeout=30
        )

        # 답변 생성 요청
        run = openai_client.beta.threads.runs.create_and_poll(
            thread_id=thread.id, assistant_id=category_assist.id, timeout=30
        )

        # 생성된 답변 받기
        if run.status == "completed":
            messages = openai_client.beta.threads.messages.list(
                thread_id=thread.id, timeout=30
            )
            return [run.status, messages]
        else:
            return [run.status, None]

    def categorize_by_openai(self, uncategorized_datas, open_ai_key):
        # API 요청을 위한 클라이언트 및 LLM 모델 (Assistant)
        open_ai_client = self.get_openai_client(open_ai_key)
        category_assist = self.get_open_ai_assistant(open_ai_client)

        total_answer_history = []
        question_and_answer = []
        failed_lectures = []

        # 한번에 최대 4개의 강의씩 처리
        batch_num = 4
        for start_batch in range(0, len(uncategorized_datas), batch_num):
            cnt = 0
            lecture_info_str = ""
            for idx in range(batch_num):
                if start_batch + idx >= len(uncategorized_datas):
                    break
                lecture_copy = deepcopy(uncategorized_datas[start_batch + idx])
                lecture_copy.pop("lecture_id")
                lecture_info_str += (
                    f"Lecture {idx + 1}: "
                    + str(lecture_copy).replace("'", "").replace('"', "")
                    + ","
                )
                cnt += 1
            lecture_info_str = lecture_info_str[:-1]  # 마지막 comma 제거

            message_content = "Here is the list of categories: 1: [Development & Programming - Web Development] 2: [Development & Programming - Frontend] 3: [Development & Programming - Backend] 4: [Development & Programming - Full Stack] 5: [Development & Programming - Mobile App Development] 6: [Development & Programming - Programming Languages] 7: [Development & Programming - Algorithms & Data Structures] 8: [Development & Programming - Databases] 9: [Development & Programming - DevOps & Infrastructure] 10: [Development & Programming - Software Testing] 11: [Development & Programming - Development Tools] 12: [Development & Programming - Web Publishing] 13: [Game Development - Game Programming] 14: [Data Science - Data Analysis] 15: [Data Science - Data Engineering] 16: [Artificial Intelligence - AI & ChatGPT Utilization] 17: [Artificial Intelligence - Deep Learning & Machine Learning] 18: [Artificial Intelligence - Computer Vision] 19: [Artificial Intelligence - Natural Language Processing] 20: [Security & Networking - Security] 21: [Security & Networking - Networking] 22: [Security & Networking - Systems] 23: [Security & Networking - Cloud] 24: [Security & Networking - Blockchain] 25: [Hardware - Embedded Systems & IoT] \n"
            message_content += (
                f"Here are the details of {cnt} lectures: " + lecture_info_str
            )
            logging.info(message_content)

            # 재시도 횟수 2회 - JSON 포맷으로 리턴 및,
            retry = 0
            process_flag = False
            while retry < 2:
                retry += 1
                result = self.get_message_from_openai(
                    message_content, open_ai_client, category_assist
                )

                if result[0] != "completed":
                    logging.error("open ai response error!:", result[0])
                    time.sleep(5)
                    continue

                ans_str = ""
                ans = result[1].data[0]
                for content in ans.content:
                    ans_str += content.text.value
                total_answer_history.append([message_content, ans_str])

                try:
                    json_ans = json.loads(ans_str)
                    if len(json_ans.keys()) == cnt:
                        for ans_key in json_ans.keys():
                            if (
                                1 <= int(json_ans[ans_key]["ans"]) <= 25
                                or json_ans[ans_key]["ans"] == -1
                            ):
                                continue
                            else:
                                raise Exception(
                                    f"{json_ans[ans_key]['ans']} is out of range(1, 25)!!"
                                )
                        question_and_answer.append(
                            [
                                message_content,
                                json_ans,
                                [
                                    uncategorized_datas[start_batch + idx]["lecture_id"]
                                    for idx in range(batch_num)
                                ],
                            ]
                        )
                        process_flag = True
                        logging.info(f"q: {message_content}\na: {json_ans}")
                        break
                except Exception as e:
                    logging.error(e)

            # 2회 재시도에서 실패한 강의 목록
            if not process_flag:
                for idx in range(batch_num):
                    failed_lectures.append(uncategorized_datas[start_batch + idx])
            time.sleep(5)

        return total_answer_history, question_and_answer, failed_lectures

    def processing_question_and_answer(self, question_and_answer):
        category_processed = {}
        for elem in question_and_answer:
            keys = list(elem[1].keys())
            for idx, key in enumerate(keys):
                lecture_id = elem[2][idx]
                json_ob = elem[1][key]
                category_processed[lecture_id] = [lecture_id, json_ob, json_ob["ans"]]

        insert_datas = []
        for lecture_id, data in category_processed.items():
            if data[-1] != -1:
                insert_datas.append((lecture_id, data[-1]))
            else:
                insert_datas.append((lecture_id, 26))

        return category_processed, insert_datas

    def insert_to_mysql(self, mysql_hook, processed_datas):
        insert_query = (
            "INSERT IGNORE INTO Category_conn (lecture_id, category_id) VALUES (%s, %s)"
        )
        logging.info(f"processed_datas: {processed_datas[:3]}")
        for elem in processed_datas:
            mysql_hook.run(insert_query, parameters=elem)

    def save_llm_operation_result_to_s3(
        self,
        category_processed,
        failed_lectures,
        total_answer_history,
        question_and_answer,
    ):
        llm_operation_log = {}
        llm_operation_log["category_processed"] = category_processed
        llm_operation_log["failed_lectures"] = failed_lectures
        llm_operation_log["total_answer_history"] = total_answer_history
        llm_operation_log["successed_answer"] = question_and_answer
        upload = {
            "string_data": json.dumps(llm_operation_log),
            "key": f"llm_operation_history/{self.today}_llm_operation_history.json",
            "bucket_name": "team-jun-1-bucket",
        }

        self.upload_file(upload)

    def upload_file(self, upload):
        self.s3_hook.load_string(
            string_data=upload["string_data"],
            key=upload["key"],
            bucket_name=upload["bucket_name"],
            replace=True,
        )
        logging.info(f'Uploaded data to s3://{upload["bucket_name"]}/{upload["key"]}')
