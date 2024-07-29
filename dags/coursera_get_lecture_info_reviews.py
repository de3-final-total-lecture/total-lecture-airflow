from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json, logging, re, requests, os
from bs4 import BeautifulSoup
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
import pendulum
import concurrent.futures
from plugins.base62 import encoding_url

kst = pendulum.timezone("Asia/Seoul")
headers = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36",
    "Accept-Language": "ko-KR,ko"
}

def extract_course_url_from_s3(**kwargs):
    timestamp = datetime.now().strftime('%m-%d')
    s3_hook = S3Hook(aws_conn_id='aws_conn_id')
    all_courses_url = []

    base_s3_path = f'raw_data/URL/{timestamp}/coursera.json'
    file_keys = s3_hook.list_keys(bucket_name='team-jun-1-bucket', prefix=base_s3_path)

    for file_key in file_keys:
        urls = s3_hook.read_key(key=file_key, bucket_name='team-jun-1-bucket')
        urls_data = json.loads(urls)
                
        for url_info in urls_data.items():
            all_courses_url.append(url_info[1])

    with open('/tmp/all_courses_url.json', 'w') as f:
        json.dump(all_courses_url, f, ensure_ascii=False, indent=4)
        
    logging.info(all_courses_url)
    logging.info(f"Extracted and saved course URLs to /tmp/all_courses_url.json")

def crawling_course_info(url):
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')
    course_info = {}
    
    course_info['lecture_id'] = encoding_url(url)

    title_element = soup.find('h1', class_='cds-119 cds-Typography-base css-1xy8ceb cds-121')
    title = title_element.text.strip() if title_element else None
    course_info['lecture_name'] = title
    
    course_info['price'] = 0
    
    description_element = soup.find('div', class_='content-inner')
    description = description_element.text.strip() if description_element else None
    course_info['description'] = description
    
    content_element = soup.find('ul', class_='cds-9 css-7avemv cds-10')
    content_items = [li.text.strip() for li in content_element.find_all('li')] if content_element else None
    course_info['whatdoilearn'] = content_items

    skills_element = soup.find('ul', class_='css-yk0mzy')
    skill_items = [li.text.strip() for li in skills_element.find_all('li')] if skills_element else None
    course_info['tag'] = skill_items
    
    teacher_element = soup.find('a', class_='cds-119 cds-113 cds-115 css-wgmz1k cds-142')
    teacher = teacher_element.text.strip() if teacher_element else None
    course_info['teacher'] = teacher

    rating_element = soup.find('div', class_='cds-119 cds-Typography-base css-h1jogs cds-121')
    rating = rating_element.text.strip() if rating_element else None
    course_info['scope'] = float(rating) if rating else 0

    reviews_element = soup.find('p', class_='css-vac8rf')
    review_cnt = re.sub(r'[^\d]', '', reviews_element.text.strip()) if reviews_element else None
    course_info['review_count'] = int(review_cnt) if review_cnt else 0
    
    ledu_elements = soup.find_all('div', class_='css-fk6qfz')
    if not ledu_elements:
        course_info['lecture_time'] = None
        course_info['level'] = None
        return course_info
    
    level_element = ledu_elements[0]    
    if "수준" in level_element.text:
        level = level_element.text[:2] 
        if level == '초급': level = '입문'
        elif level == '중급': level = '초급'
        elif level == '고등' or level == '믹스': level = '중급이상'
        duration_element = ledu_elements[1]       
    else:
        level = None
        duration_element = level_element

    duration = re.sub(r'[^\d]', '', duration_element.text.strip()) if duration_element else None
    duration_time = f"{duration}시간 00분" if duration else None
    course_info['lecture_time'] = duration_time
    course_info['level'] = level
    
    return course_info

def crawling_course_review(url):
    reviews = []
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')
    review_elements = soup.find_all('div', class_='css-15ee6ym')

    for review in review_elements:
        review_text = review.get_text(strip=True) if review else None
        if review_text and len(review_text) >= 10 and not re.search(r'http[s]?://', review_text):
            reviews.append(review_text)
        if len(reviews) >= 10:
            break
    return reviews

def get_course_info_and_reviews(**kwargs):
    
    with open('/tmp/all_courses_url.json', 'r') as f:
        all_courses_url = json.load(f)
        
    all_course_infos = []
    all_course_reviews = []
    
    for course_data in all_courses_url:
        course_url = course_data['url']
        course_info = crawling_course_info(course_url)
        course_info["thumbnail_url"] = course_data['image']
        course_info["sort_type"] = course_data['sort_by']
        course_info["keyword"] = course_data['keyword']
                
        final_course_info = {
            "lecture_url": course_url,
            "platform" : 'Coursera',
            "content": course_info
        }
        logging.info(final_course_info)
        all_course_infos.append(final_course_info)

        review_url = f"{course_url}/reviews?page=1&sort=recent"
        reviews = crawling_course_review(review_url)
        course_review = {
            "lecture_url": course_url,
            "lecture_id" : course_info["lecture_id"],
            "reviews": reviews
        }
        logging.info(course_review)
        all_course_reviews.append(course_review)
    
    with open('/tmp/all_course_infos.json', 'w') as f:
        json.dump(all_course_infos, f)

    with open('/tmp/all_course_reviews.json', 'w') as f:
        json.dump(all_course_reviews, f)

def upload_to_s3(**kwargs):
    timestamp = datetime.now().strftime('%m-%d')
    s3_hook = S3Hook(aws_conn_id='aws_conn_id')
    
    with open('/tmp/all_course_infos.json', 'r') as f:
        all_course_infos = json.load(f)

    with open('/tmp/all_course_reviews.json', 'r') as f:
        all_course_reviews = json.load(f)


    uploads = []

    for course_info in all_course_infos:
        lecture_id = course_info['content']['lecture_id']
        sort_type = course_info['content']['sort_type'].lower()
        course_s3_path = f'product/{timestamp}/{sort_type}/coursera_{lecture_id}.json'
        
        uploads.append({
            'string_data': json.dumps(course_info, ensure_ascii=False, indent=4),
            'key': course_s3_path,
            'bucket_name': 'team-jun-1-bucket'
        })
    
    for course_review in all_course_reviews:
        lecture_url = course_review['lecture_url']
        lecture_id = course_review['lecture_id']
        review_s3_path = f'analytics/reviews/{timestamp}/{lecture_id}.json'
        
        uploads.append({
            'string_data': json.dumps(course_review, ensure_ascii=False, indent=4),
            'key': review_s3_path,
            'bucket_name': 'team-jun-1-bucket'
        })

    def upload_file(upload):
        s3_hook.load_string(
            string_data=upload['string_data'],
            key=upload['key'],
            bucket_name=upload['bucket_name'],
            replace=True,
        )
        logging.info(f'Uploaded data to s3://{upload["bucket_name"]}/{upload["key"]}')
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(upload_file, uploads)
        
    os.remove('/tmp/all_courses_url.json'),
    os.remove('/tmp/all_course_infos.json'),
    os.remove('/tmp/all_course_reviews.json')

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'coursera_get_lecture_info_reviews',
    default_args=default_args,
    description='Scrape Coursera course info and reviews and upload to S3',
    schedule_interval=timedelta(weeks=1),
    start_date=kst.convert(days_ago(1)), 
    catchup=False,
) as dag:

    extract_course_url = PythonOperator(
        task_id='extract_course_url_from_s3',
        python_callable=extract_course_url_from_s3,
        provide_context=True,
    )

    transform_and_load_info_and_reviews = PythonOperator(
        task_id='get_course_info_and_reviews',
        python_callable=get_course_info_and_reviews,
        provide_context=True,
    )

    upload_all_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        provide_context=True,
    )

    extract_course_url >> transform_and_load_info_and_reviews >> upload_all_to_s3
