from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago
from datetime import timedelta

import pendulum

import time
import requests
import logging
import json
import re


def _extract_lecture_id_url():
    mysql_hook = MySqlHook(mysql_conn_id="mysql_conn")

    get_lecture_id_query = (
        "SELECT lecture_id FROM Lecture_info WHERE thumbnail_url LIKE '%inflearn%';"
    )

    results = mysql_hook.get_records(get_lecture_id_query)

    for row in results:
        get_inflearn_id_query = (
            "SELECT inflearn_id FROM Inflearn_id WHERE lecture_id = %s"
        )
        inflearn_id = mysql_hook.get_first(get_inflearn_id_query, (row[0],))[0]
        url = f"https://www.inflearn.com/course/client/api/v1/course/{inflearn_id}/online/info"
        response = requests.get(url)
        response.raise_for_status()
        try:
            response_data = response.json()
        except:
            logging.info(url)
        response_data = response_data["data"]
        price = response_data["paymentInfo"]["payPrice"]
        lecture_id = row[0]

        insert_lecture_price_history_query = (
            "INSERT INTO Lecture_price_history (lecture_id, price) VALUES (%s, %s)"
        )
        mysql_hook.run(
            insert_lecture_price_history_query, parameters=(lecture_id, price)
        )
        time.sleep(0.5)


kst = pendulum.timezone("Asia/Seoul")
default_args = {
    "owner": "zjacom",
    "start_date": kst.convert(days_ago(1)),
    # "retries": 3,
}

with DAG(
    "inflearn_get_lecture_price",
    default_args=default_args,
    description="DAG to get lecture price.",
    schedule_interval=None,
) as dag:
    extract_lecture_id_url = PythonOperator(
        task_id="extract_lecture_id_url",
        python_callable=_extract_lecture_id_url,
        provide_context=True,
    )

    extract_lecture_id_url