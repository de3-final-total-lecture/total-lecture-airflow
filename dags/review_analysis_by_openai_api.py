from airflow import DAG
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from tenacity import retry, stop_after_attempt, wait_exponential
from openai import OpenAI
from datetime import timedelta, datetime
import logging
import json
import time


def get_all_json_files_from_s3(bucket_name, prefix=""):
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
    keys = s3_hook.list_keys(bucket_name, prefix=prefix)
    logging.info(f"keys: {keys}")
    json_files = [key for key in keys if key.endswith(".json")]
    return json_files


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def read_json_file_from_s3(bucket_name, key):
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
    content = s3_hook.read_key(key, bucket_name)
    return json.loads(content)


def get_result_from_mysql(mysql_hook, query):
    """
    쿼리 결과를 내주는 함수
    """
    conn = mysql_hook.get_conn()
    result = None
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
    finally:
        cursor.close()
        conn.close()
    return result


def read_review_analysis_from_mysql(mysql_hook):
    query = """
        SELECT lecture_id FROM Review_analysis
    """
    result = get_result_from_mysql(mysql_hook, query)

    analized_lecture_id_set = set()
    for row in result:
        analized_lecture_id_set.add(row[0])

    return analized_lecture_id_set


def get_processed_unanalysised_datas(mysql_hook, bucket_name, prefix):
    """
    s3에서 오늘 처리할 데이터를 가져오고,
    mysql의 데이터와 비교해 처리 안된 강의만 가져오는 함수
    """
    analized_lecture_id_set = read_review_analysis_from_mysql(mysql_hook)

    # 오늘 처리할 모든 JSON 파일 목록 가져오기
    logging.info(f"get all review json files from s3 {prefix}")
    json_files = get_all_json_files_from_s3(bucket_name, prefix)
    logging.info(f"json files: {json_files[:3]}")
    unanalized_datas = []

    for json_file in json_files:
        lecture_id = json_file.split("/")[-1].split(".")[0]

        # 리뷰 분석 안된 강의만 추출
        if lecture_id not in analized_lecture_id_set:
            json_data = read_json_file_from_s3(bucket_name, json_file)
            json_data.pop("lecture_url")
            # logging.info(f"{lecture_id} is not analysised!")
            unanalized_datas.append(json_data)

            # 중복으로 들어오는 데이터 체크를 위해 일단 처리된 데이터에 추가
            analized_lecture_id_set.add(lecture_id)
        # else:
        #     logging.info(f"{lecture_id} is analysised!")
    return unanalized_datas


def get_openai_client(api_key):
    return OpenAI(api_key=api_key)


def get_open_ai_assistant(openai_client):
    review_assist = None
    for llm_model in openai_client.beta.assistants.list().data:
        if llm_model.name == "Review Assistant":
            logging.info("load review assistant")
            review_assist = llm_model
            break

    if review_assist is None:
        logging.info("create review assistant")
        review_assist = openai_client.beta.assistants.create(
            instructions="""You are a lecture review analysis assistant, and your task is to analyze given lists of reviews for specific lectures. When provided with a list of reviews for each lecture, you must perform sentiment analysis on each review, assigning a score of 1 for positive reviews and 0 for negative reviews, and present the results in a list format. Finally, you must provide a summary of all the reviews in a JSON format in Korean. Your responses should be strictly in JSON format, and you should not provide any responses outside of JSON. Here is an example: Input: ["영어권에서는 블로그, 콘텐츠 작성 전략이 어떨지 궁금했는데 마침 강의가 딱 있어서 좋았습니다 ㅎㅎ 한국 시장에서 적용할 때에도 도움될만한 인사이트가 많이 들어있네요~ 썸네일, 제목, 상세페이지, 뉴스레터 등 카피라이팅이 중요한 업무 담당하시는 분들은 참고하시면 좋을 강의입니다!", "글쓰기 연습에 도움이 되는 강의에요!", "목적성 , 홍보성 글 쓰기에 좋은 가이드 입니다. 업무하면서 틈틈히 살펴보면 좋을 것 같아요!", "유익한 강의입니다~!", "이론적인 얘기만 수두룩 하네 ㅋ 돈 아깝게 이딴걸 왜 듣는지 ㅋㅋ"] Response: { "sentiment": [1.0, 0.9, 0.9, 0.9, 0.1], "summary": "이 강의는 영어권 블로그 및 콘텐츠 작성 전략에 대해 다루며, 한국 시장에서도 유용한 인사이트를 제공합니다. 특히, 썸네일, 제목, 상세페이지, 뉴스레터 등 카피라이팅에 중요합니다. 일부 리뷰어는 글쓰기 연습과 목적성, 홍보성 글 작성에 도움이 된다고 했으며, 업무 중 틈틈이 참고하면 좋다고 합니다. 대체로 유익하다는 평가가 많지만, 한 리뷰어는 이론적인 내용만 많아 돈이 아깝다고 평가했습니다." } In the example above, only one lecture was provided, but in practice, multiple lectures may be input. In such cases, wrap the results for each lecture in a JSON object using lec_n as the key.""",
            name="Review Assistant",
            model="gpt-4o-mini",
        )

    return review_assist


def get_message_from_openai(message_content, openai_client, assistant):
    # 대화를 위한 스레드와 질문 메시지 생성
    thread = openai_client.beta.threads.create()
    message = openai_client.beta.threads.messages.create(
        thread_id=thread.id, role="user", content=message_content, timeout=30
    )

    # 답변 생성
    run = openai_client.beta.threads.runs.create_and_poll(
        thread_id=thread.id, assistant_id=assistant.id, timeout=30
    )

    # 답변 받기~~
    if run.status == "completed":
        messages = openai_client.beta.threads.messages.list(
            thread_id=thread.id, timeout=30
        )
        return [run.status, messages]
    else:
        return [run.status, None]


def check_llm_output(json_ans, lecture_cnt):
    if len(json_ans.keys()) == lecture_cnt:
        for idx in range(lecture_cnt):
            val = json_ans.get(f"lec_{idx+1}", None)
            if val is None:
                return False
            if "sentiment" not in val.keys() or "summary" not in val.keys():
                return False
        return True
    else:
        return False


def llm_ans_preprocess(ans_str):
    """
    LLM이 잘못 내뱉는 경우에 대한 일반적인 처리 -> 그 외 처리는 어려움
    """
    if "`" in ans_str:
        ans_str = ans_str.replace("`", "")
    if not ans_str.startswith("{"):
        start_idx = max(ans_str.find("{"), 0)
        ans_str = ans_str[start_idx:]
    return ans_str.strip()


def review_analysis_by_openai(unanalysised_datas, open_ai_key, mysql_hook):
    # API 요청을 위한 클라이언트 및 LLM Assistant 가져오기
    openai_client = get_openai_client(open_ai_key)
    review_assist = get_open_ai_assistant(openai_client)

    total_answer_history = []
    llm_result = {}
    failed_lectures = []
    no_review_lectures = []
    batch_num = 4
    for batch_start in range(0, len(unanalysised_datas), batch_num):
        now_lectures = []
        message_content = ""
        stop_flag = False
        cnt = 1
        for i in range(batch_num):
            if batch_start + i >= len(unanalysised_datas):
                stop_flag = True
                break
            elem = unanalysised_datas[batch_start + i]
            lecture_id = elem["lecture_id"]
            review_list = elem["reviews"]
            if len(review_list) == 0:
                no_review_lectures.append(lecture_id)
                continue
            now_lectures.append(lecture_id)
            message_content += f"lec_{cnt}: {str(review_list)}\n"
            cnt += 1
        message_content = message_content.strip()
        if len(message_content) == 0 and not stop_flag:
            continue
        logging.info(f"q: {message_content} \n length: {len(message_content)}")

        retry = 0
        process_flag = False
        while retry < 2:
            retry += 1
            try:
                result = get_message_from_openai(
                    message_content, openai_client, review_assist
                )
                time.sleep(5)

                if result[0] != "completed":
                    logging.info("open ai response error: ", result[0])
                    continue

                ans_str = ""
                ans = result[1].data[0]
                for content in ans.content:
                    ans_str += content.text.value
                ans_str = llm_ans_preprocess(ans_str)
                total_answer_history.append([now_lectures, message_content, ans_str])
                json_ans = json.loads(ans_str)
                if check_llm_output(json_ans, len(now_lectures)):
                    logging.info(f"a: {json_ans}")
                    result_json_dict = {}
                    for idx, lecture_id in enumerate(now_lectures):
                        val = json_ans[f"lec_{idx+1}"]
                        result_json = {
                            "positive_count": 0,
                            "negative_count": 0,
                            "neutral_count": 0,
                        }

                        avg_sentiment = 0.0
                        for elem in val["sentiment"]:
                            sentiment_score = float(elem)
                            if sentiment_score > 0.7999:
                                result_json["positive_count"] += 1
                            elif sentiment_score > 0.3999:
                                result_json["neutral_count"] += 1
                            else:
                                result_json["negative_count"] += 1
                            avg_sentiment += sentiment_score
                        avg_sentiment /= max(len(val["sentiment"]), 1)
                        result_json["avg_sentiment"] = avg_sentiment

                        result_json["summary"] = val["summary"]
                        llm_result[lecture_id] = result_json
                        result_json_dict[lecture_id] = result_json
                    
                    processed_datas = processing_review_analysis_result(result_json_dict)
                    insert_to_mysql(mysql_hook, processed_datas)

                    process_flag = True
                    logging.info(f"success!!{batch_start}/{len(unanalysised_datas)}")
                    break
                else:
                    logging.info("process failed by unknown result")

            except Exception as e:
                logging.info(f"error: {e}")
        if not process_flag:
            failed_lectures += now_lectures
            logging.info(f"{now_lectures} are failed to process")
        if stop_flag:
            break
        time.sleep(5)
    return llm_result, failed_lectures, no_review_lectures, total_answer_history


def processing_review_analysis_result(review_analysis_result):
    insert_datas = []
    for k in review_analysis_result.keys():
        it = review_analysis_result[k]
        insert_datas.append(
            (
                k,
                it["summary"],
                it["negative_count"],
                it["neutral_count"],
                it["positive_count"],
                it["avg_sentiment"],
            )
        )
    return insert_datas


def insert_to_mysql(mysql_hook, processed_datas):
    insert_query = "INSERT INTO Review_analysis (lecture_id, summary, negative_count, neutral_count, positive_count, avg_sentiment) VALUES (%s, %s, %s, %s, %s, %s)"
    logging.info(f"processed_datas: {processed_datas[:3]}")

    for elem in processed_datas:
        mysql_hook.run(insert_query, parameters=elem)


def upload_file(upload):
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
    s3_hook.load_string(
        string_data=upload["string_data"],
        key=upload["key"],
        bucket_name=upload["bucket_name"],
        replace=True,
    )
    logging.info(f'Uploaded data to s3://{upload["bucket_name"]}/{upload["key"]}')


def save_llm_operation_result_to_s3(
    review_analysis_result,
    failed_lectures,
    no_review_lectures,
    total_answer_history,
    today,
):
    llm_operation_log = {}
    llm_operation_log["review_analysis_result"] = review_analysis_result
    llm_operation_log["failed_lectures"] = failed_lectures
    llm_operation_log["no_review_lectures"] = no_review_lectures
    llm_operation_log["total_answer_history"] = total_answer_history
    upload = {
        "string_data": json.dumps(llm_operation_log),
        "key": f"llm_operation_history/review/{today}_review_llm_operation_history.json",
        "bucket_name": "team-jun-1-bucket",
    }

    upload_file(upload)


def _review_analysis_by_chatgpt(*args, **kwargs):
    """
    Work Flow
    1. RDS에서 Review Analysis가 완료된 데이터 fetch
    2. 처리할 데이터에서 처리 완료된 데이터 빼고 가져오기
    3. ChatGPT API를 사용해 처리
    4. RDS Review_analysis에 적재
    5. S3에 처리 로그 적재
    시스템 프롬프트 변경시엔 기존 Review Assistant 삭제 후 새로 LLM 모델 만들기
    """
    execution_date = kwargs["execution_date"]
    korean_time = execution_date + timedelta(hours=9)
    # 테스트를 위한 today 변경
    today = korean_time.strftime("%m-%d")
    logging.info(f"logical kst: {korean_time}")

    mysql_hook = MySqlHook(mysql_conn_id="mysql_conn")
    bucket_name = "team-jun-1-bucket"
    prefix = f"analytics/reviews/{today}"

    unanalized_datas = get_processed_unanalysised_datas(mysql_hook, bucket_name, prefix)
    logging.info(f"Number of today analysis data: {len(unanalized_datas)}")

    # ChatGPT를 통한 카테고리 매핑 (내부적으로 JSON으로 처리 후 DB 적재까지 함)
    open_ai_key = kwargs["open_ai_api"]
    (
        review_analysis_result,
        failed_lectures,
        no_review_lectures,
        total_answer_history,
    ) = review_analysis_by_openai(unanalized_datas, open_ai_key, mysql_hook)
    logging.info(f"Number of failed lectures: {len(failed_lectures)}")
    logging.info(f"Top 5 of failed lectures: {failed_lectures[:5]}")

    # 리뷰가 없는 데이터는 None으로 저장
    no_review_datas = []
    for no_review_lec in no_review_lectures:
        no_review_datas.append((no_review_lec, None, None, None, None, None))

    insert_to_mysql(mysql_hook, no_review_datas)

    save_llm_operation_result_to_s3(
        review_analysis_result,
        failed_lectures,
        no_review_lectures,
        total_answer_history,
        korean_time.strftime("%m-%d_%H:%M:%S"),
    )


with DAG(
    dag_id="review_analysis_by_openai_api",
    schedule_interval=None,
    default_args={
        "dependes_on_past": True,
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
    },
) as dag:

    review_analysis_by_chatgpt = PythonOperator(
        task_id="review_analysis_by_chatgpt",
        python_callable=_review_analysis_by_chatgpt,
        op_kwargs={"open_ai_api": "{{ var.value.openai_api_key}}"},
    )
