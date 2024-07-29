from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json, time, logging
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from airflow.utils.dates import days_ago
import pendulum
import concurrent.futures
from plugins.base62 import encoding_url


kst = pendulum.timezone("Asia/Seoul")
user_agent = "userMozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36"
headers = {
    "user-agent": user_agent,
    "Accept-Language": "ko-KR,ko"
}
keywords_json = Variable.get("test_keyword")
keywords = json.loads(keywords_json)
sort_values = ['NEW', 'BEST_MATCH']

chrome_options = Options()
chrome_options.add_argument(f"user-agent={user_agent}")
chrome_options.add_argument("--ignore-ssl-errors=yes")
chrome_options.add_argument("--ignore-certificate-errors")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--headless")
chrome_options.add_argument('window-size=1920x1080')

def crawling_course_url(keyword, sort_value):
    with webdriver.Remote("remote_chromedriver:4444/wd/hub", options=chrome_options) as driver:
        driver.get(f"https://www.coursera.org/search?query={keyword}&language=Korean&productTypeDescription=Courses&sortBy={sort_value}")
        wait = WebDriverWait(driver, 10)
        time.sleep(2)

        sc_cnt = 2 if sort_value == "NEW" else 16
        ele_num = 20 if sort_value == "NEW" else 100

        scroll_location = driver.execute_script("return window.pageYOffset")
        for _ in range(sc_cnt):
            driver.execute_script("window.scrollTo(0,{})".format(scroll_location + 900))
            time.sleep(2)
            scroll_location = driver.execute_script("return window.pageYOffset")

        course_elements = wait.until(EC.presence_of_all_elements_located((By.XPATH, "//div[contains(@class, 'cds-ProductCard-gridCard')]")))
        courses_info = []
        for i, course in enumerate(course_elements[:ele_num]):
            course_link = course.find_element(By.CSS_SELECTOR, "a.cds-CommonCard-titleLink")
            course_url = course_link.get_attribute("href")
            image_element = course.find_element(By.CSS_SELECTOR, ".cds-CommonCard-previewImage img")
            preview_image_src = image_element.get_attribute("src")

            courses_info.append({
                "url": course_url,
                "image": preview_image_src,
                "sort_by" : "RECENT" if sort_value == "NEW" else "RECOMMEND",
                "keyword" : keyword
            })
            logging.info(f"{keyword}_{i}_{sort_value} 크롤링 성공 => {course_url}")
        return courses_info

def extract_load_url():
    s3_hook = S3Hook(aws_conn_id='aws_s3_connection')
    timestamp = datetime.now().strftime('%m-%d')
    all_courses_info = {}
    uploads = []
    
    for keyword in keywords:
        for sort_value in sort_values:
            logging.info(f"{keyword}_{sort_value} 크롤링 start")
            courses_info = crawling_course_url(keyword, sort_value)
            for course in courses_info:
                course_url = course["url"]
                course_hash = encoding_url(course_url)
                all_courses_info[course_hash] = course 

    json_data = json.dumps(all_courses_info, ensure_ascii=False, indent=4)
    s3_key = f'raw_data/URL/{timestamp}/coursera.json'
       
    uploads.append({
        'string_data': json_data,
        'key': s3_key,
        'bucket_name': 'team-jun-1-bucket'
    })
    
    def upload_file(upload):
        s3_hook.load_string(
            string_data=upload['string_data'],
            key=upload['key'],
            bucket_name=upload['bucket_name'],
            replace=True,
        )
        logging.info(f'Uploaded course info to s3://{upload["bucket_name"]}/{upload["key"]}')
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(upload_file, uploads)

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'coursera_get_lecutre_id_url',
    default_args=default_args,
    description='Extract Coursera URL and upload to S3',
    schedule_interval=timedelta(weeks=1),
    start_date=kst.convert(days_ago(1)), 
    catchup=False,
) as dag:

    extract_url_load_S3 = PythonOperator(
        task_id='load_url_S3',
        python_callable=extract_load_url,
        provide_context=True,
    )

extract_url_load_S3
