from inflearn.operator import InflearnPriceOperator
from udemy.operator import UdemyPriceOperator
from airflow.models import Variable

from airflow import DAG
from airflow.utils.dates import days_ago

import pendulum

kst = pendulum.timezone("Asia/Seoul")

# 기본 설정
default_args = {
    "owner": "zjacom",
    "depends_on_past": False,
    "start_date": kst.convert(days_ago(1)),
}

with DAG(
    "load_lecture_price",
    default_args=default_args,
    description="DAG for load lecture price in Lecture_price_history table.",
    schedule_interval=None,
) as dag:

    get_inflearn_lecture_price = InflearnPriceOperator(
        task_id="get_inflearn_lecture_price",
    )

    get_udemy_lecture_price = UdemyPriceOperator(
        task_id="get_udemy_lecture_price",
        client_id=Variable.get("Udemy_CLIENT_ID"),
        client_secret=Variable.get("Udemy_CLIENT_SECRET"),
    )

    [get_inflearn_lecture_price, get_udemy_lecture_price]
