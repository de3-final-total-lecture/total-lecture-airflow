from pyudemy.udemy import UdemyAffiliate
from airflow import DAG
# from airflow.models import Variable
from airflow import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago

from datetime import datetime, timedelta
from urllib.parse import unquote
import json
import requests
import time
import pendulum
import logging
from base62 import encoding_url

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

kst = pendulum.timezone("Asia/Seoul")

def convert_format(time):
    time = time.replace('총 ', '').replace('시간', '').strip()
    
    if '분' not in time:
        # "45시간" 또는 "74.5시간" 형태인 경우
        hours = float(time)
        int_hours = int(hours)
        minutes = int((hours - int_hours) * 60)
        if minutes > 0:
            return f'{int_hours}시간 {minutes}분'
        else:
            return f'{int_hours}시간'
    elif '시간' in time and '분' in time:
        # "2시간 30분" 형태인 경우
        hours, minutes = time.split('시간')
        minutes = minutes.replace('분', '').strip()
        return f'{int(hours)}시간 {int(minutes)}분'
    else:
        return time

def get_source(url):
    response = requests.get(url)
    return response.text

def get_udemy(course_id):
    url = 'https://www.udemy.com/api-2.0/discovery-units/'
    params = {
        "context": 'clp-bundle',
        "from": "0",
        "page_size": "3",
        "item_count": "12",
        "course_id": course_id,
        "source_page": "course_landing_page",
        "locale": "ko_KR",
        "currency": "krw",
        "navigation_locale": "en_US",
        "skip_price": "true",
        "funnel_context": "landing-page"
    }
    response = requests.get(url, params=params).json()
    return response


def extract_udemy(sort_type, **kwargs):
    logger.info("Starting Udemy extraction...")
    CLIENT_ID = Variable.get('Udemy_CLIENT_ID')
    CLIENT_SECRET = Variable.get('Udemy_CLIENT_SECRET')
    BASE_URL = Variable.get('BASE_URL')

    udemy = UdemyAffiliate(CLIENT_ID, CLIENT_SECRET)
    auth = requests.auth.HTTPBasicAuth(CLIENT_ID, CLIENT_SECRET)

    keyword_url = Variable.get('keywords_url')
    keywords_json = get_source(keyword_url)
    keywords = json.loads(keywords_json)

    timestamp = datetime.now().strftime("%m-%d")

    review_params = {
        "page": 1,
        "page_size": 500,
        "rating@min": 3,
    }
    # 정렬 기준에 따른 변화
    page_size = 100
    sort_word = "RECOMMEND"
    if sort_type == "newest":
        page_size = 20
        sort_word = "RECENT"

    # 저장할 bucket에 접근
    html_bucket = Variable.get('bucket_name')
    s3_hook = S3Hook(aws_conn_id='aws_s3_connection')
    url_json = {}
    
    for keyword in keywords["keywords"]:
        try:
            main_params = {
                "search": unquote(keyword),
                "language": "ko",
                "ordering": sort_type,
                "ratings": 3.0,
                "page": 1,
                "page_size": page_size,
                "fields[course]": "url,title,price_detail,headline,visible_instructors,image_480x270,instructional_level,description,avg_rating"
                }

            count = 0

            main_results = udemy.courses(**main_params)
            for course in main_results['results']:
                count += 1
                logging.info(f'------------------- Start : {unquote(keyword)} ------------------------------')
                
                search_url = 'https://www.udemy.com' + course['url']
                title = course['title']
                
                price = int(course['price_detail']['amount'])
                headline = course['headline']

                # Description 추출
                course_id = course["id"]
                course_element = get_udemy(course_id)
                description = course_element['units'][0]['items'][0]['objectives_summary']
                
                teacher = course['visible_instructors'][0]['display_name']
                scope = round(course['avg_rating'], 1)
                img_url = course['image_480x270']
                
                lecture_time_element = course_element['units'][0]['items'][0]['content_info']
                lecture_time = convert_format(lecture_time_element)
                
                level = course['instructional_level']
                if level == "All Levels":
                    level = "All"
                elif level == "Beginner Level":
                    level = "입문"
                elif level == "Intermediate Level":
                    level = "초급"
                else:
                    level = "중급이상"

                
                # Review
                review_endpoint = f'{BASE_URL}/courses/{course_id}/reviews/'
                response = requests.get(review_endpoint, auth=auth, params=review_params)
                courses = response.json()
                review_cnt = courses["count"]
                
                hash_url = encoding_url(search_url)

                rating_5 = []
                rating_4 = []
                rating_3 = []

                for course_detail in courses["results"]:
                    if course_detail["content"] and "http" not in course_detail["content"]:
                        if int(course_detail["rating"]) == 5 and len(rating_5) < 7:
                            rating_5.append(course_detail["content"])
                        if 4 <= course_detail["rating"] <= 4.5 and len(rating_4) < 2:
                            rating_4.append(course_detail["content"])
                        if 3 <= course_detail["rating"] <= 3.5 and len(rating_3) < 1:
                            rating_3.append(course_detail["content"])
                        if len(rating_5) == 7 and len(rating_4) == 2 and len(rating_3) == 1:
                            break
                        
                all_reviews = rating_5 + rating_4 + rating_3
                reveiws_json = {
                    "lecture_id": hash_url,
                    "lecture_url": search_url,
                    "reviews": all_reviews
                }

                main_json = {
                    "lecture_url": search_url,
                    "keyword": keyword,
                    "course_id": course_id,
                    "platform_name": "Udemy",
                    "content": {
                    "lecture_id": hash_url,
                    "lecture_name": title,
                    "price": price,
                    "description": headline,
                    "what_do_i_learn": description,
                    "tag": [],
                    "level": level,
                    "teacher": teacher,
                    "scope": scope,
                    "review_count": review_cnt,
                    "lecture_time": lecture_time,
                    "thumbnail_url": img_url,
                    "sort_type": sort_word
                    }
                }
                url_json[hash_url] = {
                    "keyword": keyword,
                    "sort_type": sort_word,
                    "lecture_url": search_url
                }

                
                main_s3_key = f'product/{timestamp}/{sort_word}/udemy_{hash_url}.json'
                review_s3_key = f'analytics/reviews/{timestamp}/{hash_url}.json'
                s3_hook.load_string(json.dumps(main_json, ensure_ascii=False), key=main_s3_key, bucket_name=html_bucket, replace=True)
                s3_hook.load_string(json.dumps(reveiws_json, ensure_ascii=False), key=review_s3_key, bucket_name=html_bucket, replace=True)
                logger.info(f"Successfully processed course: {title}")
                logging.info(f'------------------- END : {unquote(keyword)}_{count} ------------------------------')
                time.sleep(0.5)
        except Exception as e:
            logger.error(f"Error processing keyword {unquote(keyword)}: {str(e)}")
    url_s3_key = f'raw_data/URL/{timestamp}/udemy.json'
    s3_hook.load_string(json.dumps(url_json, ensure_ascii=False), key=url_s3_key, bucket_name=html_bucket,replace=True)
    logger.info("End Dags")


# DAG 정의
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
        'udemy_API',
        default_args=default_args,
        description='Extract course information using Udemy API and save it to S3',
        schedule_interval='0 0 1 * *',
        start_date=kst.convert(days_ago(1)),
        catchup=False,
) as dag:
    extract_most_reviewed = PythonOperator(
        task_id='extract_most_reviewed',
        python_callable=extract_udemy,
        op_kwargs={'sort_type': 'most-num_reviews'},
        provide_context=True,
    )
    extract_newest = PythonOperator(
        task_id='extract_newest',
        python_callable=extract_udemy,
        op_kwargs={'sort_type': 'newest'},
        provide_context=True,
    )

    extract_most_reviewed >> extract_newest