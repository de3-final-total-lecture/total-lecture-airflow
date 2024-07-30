from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago
from airflow.models import Variable
from pyudemy.udemy import UdemyAffiliate
import pendulum
import time

kst = pendulum.timezone("Asia/Seoul")

def _get_lecture_price(sort_word, **context):
    CLIENT_ID = Variable.get('Udemy_CLIENT_ID')
    CLIENT_SECRET = Variable.get('Udemy_CLIENT_SECRET')
    udemy = UdemyAffiliate(CLIENT_ID, CLIENT_SECRET)
    detail = udemy.course_detail(course_id)

    mysql_hook = MySqlHook(mysql_conn_id="mysql_conn")

    get_udemy_id_query = "SELECT course_id, lecture_id FROM Udemy;"
    results = mysql_hook.get_records(get_udemy_id_query)
    for result in results:
        course_id, lecture_id = result[0], result[1]
        
        price = int(detail['price_detail']['amount'])

        insert_lecture_price_history_query = (
            "INSERT INTO Lecture_price_history (lecture_id, price) VALUES (%s, %s)"
        )   
        mysql_hook.run(
            insert_lecture_price_history_query, parameters=(lecture_id, price)
        )
        time.sleep(0.5)


default_args = {
    "owner": "airflow",
    "start_date": kst.convert(days_ago(1)),
}

with DAG(
    "udemy_get_lecture_price",
    default_args=default_args,
    description="DAG to get lecture price.",
    schedule_interval=None,
) as dag:
    udemy_extract_lecture_price = PythonOperator(
        task_id="udemy_extract_lecture_price",
        python_callable=_get_lecture_price,
        provide_context=True,
    )

    udemy_extract_lecture_price