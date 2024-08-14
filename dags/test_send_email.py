from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from custom.mysqlhook import CustomMySqlHook
import logging
import time
from plugins.my_email import send_email
import logging


class TestEmailOperator(BaseOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def pre_execute(self, context):
        self.mysql_hook = CustomMySqlHook(mysql_conn_id="mysql_conn")

    def execute(self, context):
        lecture_ids = [
            "wYbUTe6qHx46mYVVr0dRM",
            "WYnA0CqmiaKA0eQZ9n93P",
            "7ToFdBzi0Cit8UF0EvVzy4",
        ]
        self.get_lecture_price(lecture_ids)

    def get_lecture_price(self, lecture_ids):
        for lecture_id in lecture_ids:
            price = 999999999
            if self.is_any_change_to_price(lecture_id, price):
                logging.info(
                    f"{lecture_id}의 강의 가격이 {price}로 업데이트 되었습니다."
                )
                self.send_email_to_user(lecture_id)
            time.sleep(0.5)

    def is_any_change_to_price(self, lecture_id, price):
        get_existed_price_query = (
            f"SELECT price FROM Lecture_info WHERE lecture_id = '{lecture_id}'"
        )
        result = self.mysql_hook.get_first(get_existed_price_query)
        if result and int(result[0]) != price:
            return True
        else:
            return False

    def send_email_to_user(self, lecture_id):
        get_lecture_name_user_id_query = f"SELECT lecture_name, user_id, is_alarm FROM wish_list WHERE lecture_id = '{lecture_id}'"
        results = self.mysql_hook.run(get_lecture_name_user_id_query)
        if results:
            for result in results:
                lecture_name, user_id, is_alarm = result
                if is_alarm:
                    get_user_email_query = f"SELECT user_email FROM lecture_users WHERE user_id = {user_id}"
                    user_email = self.mysql_hook.run(get_user_email_query)
                    send_email(
                        "linden97xx@gmail.com",
                        user_email,
                        "OLLY에서 알려드립니다.",
                        f"{lecture_name}의 가격이 변동되었습니다. 사이트에서 확인 부탁드립니다.",
                    )
