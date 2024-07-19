from airflow import DAG
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from bs4 import BeautifulSoup

import pendulum

# import pymysql
import requests

# pymysql.install_as_MySQLdb()

kst = pendulum.timezone("Asia/Seoul")
# 기본 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": kst.convert(days_ago(1)),
}


def _load_category_data():
    hook = MySqlHook(mysql_conn_id="mysql_conn")
    res = requests.get("https://www.inflearn.com/").text

    soup = BeautifulSoup(res, "html.parser")
    categorys = {}
    for elem in soup.find("ul", attrs={"class": "navbar-dropdown is-boxed"}).find_all(
        "li"
    ):
        is_big_cate = (
            len(
                elem.find_all(
                    "ul", attrs={"class": "navbar-dropdown is-boxed step_menu step_2"}
                )
            )
            > 0
        )
        if is_big_cate:
            category_str = elem.text.strip().split("\n\n\n")
            categorys[category_str[0]] = {}
            key_temp = None
            for elem in category_str[1:]:
                if "\n " in elem:
                    categorys[category_str[0]][key_temp] = list(
                        map(lambda x: x.strip(), elem.split("\n "))
                    )
                else:
                    key_temp = elem.strip()
                    categorys[category_str[0]][key_temp] = []

    exclude_main = [
        "디자인 · 아트",
        "비즈니스 · 마케팅",
        "공학 · 수학 · 외국어",
        "커리어",
    ]
    include_hardware = ["컴퓨터 구조", "임베디드 · IoT"]
    for main in categorys:
        if main in exclude_main:
            continue
        for mid in categorys[main]:
            if main == "게임 개발" and mid != "게임 프로그래밍":
                continue
            if main == "하드웨어":
                if mid not in include_hardware:
                    continue
            if "기타" in mid or "자격증" in mid:
                continue
            for sub in categorys[main][mid]:
                if mid == "AI · ChatGPT 활용" and sub != "프롬프트엔지니어링":
                    continue
                sql = "INSERT INTO Category (main_category_name, mid_category_name, sub_category_name) VALUES (%s, %s, %s)"
                hook.run(sql, parameters=(main, mid, sub))


# 하드웨어 - 컴퓨터 구조, 임베디드
with DAG(
    "load_category_data",
    default_args=default_args,
    description="DAG to load inflearn category in Category table.",
    schedule_interval=None,
) as dag:

    load_category_data = PythonOperator(
        task_id="load_category_data",
        python_callable=_load_category_data,
        provide_context=True,
    )

    load_category_data
