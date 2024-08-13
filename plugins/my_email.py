from airflow.models import Variable

import smtplib
from email.mime.text import MIMEText


def send_email(sender, receiver, subject, content):
    # 이메일 생성
    msg = MIMEText(content)
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = receiver

    # 이메일 서버에 연결
    server = smtplib.SMTP("smtp.gmail.com", 587)
    server.starttls()  # TLS 보안 시작
    email_address, email_password = Variable.get("smtp_email_addr"), Variable.get(
        "smtp_email_pw"
    )
    server.login(email_address, email_password)  # 로그인

    # 이메일 보내기
    server.sendmail(sender, receiver, msg.as_string())
    server.quit()
