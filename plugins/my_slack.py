from airflow.models import Variable

import requests


def on_failure_callback(context):
    text = str(context["task_instance"])
    text += "```" + str(context.get("exception")) + "```"
    send_message_to_a_slack_channel(text, ":scream:")


def on_success_callback(context):
    text = str(context["task_instance"])
    text += "이(가) 성공적으로 실행되었습니다."
    send_message_to_a_slack_channel(text, ":scream:")


def send_message_to_a_slack_channel(message, emoji):
    url = "https://hooks.slack.com/services/" + Variable.get(
        "slack_access_token_zjacom"
    )
    headers = {
        "content-type": "application/json",
    }
    data = {"username": "zjacom", "text": message, "icon_emoji": emoji}
    r = requests.post(url, json=data, headers=headers)
    return r
