# 온라인 강의 통합 서비스 Workflow

## 워크플로우 개요
<img width="848" alt="스크린샷 2024-08-20 오후 3 39 59" src="https://github.com/user-attachments/assets/d81fbadc-4cc8-42bb-88a2-ebb49b2d88ba">

온라인 강의 플랫폼 유데미, 인프런, 코세라에서 강의 정보를 추출하여 S3, Glue, OpenAI API를 거쳐서 최종적으로 RDS에 저장되는 흐름을 가지고 있습니다.

## 데이터 출처

- [인프런](https://www.inflearn.com/)
- [코세라](https://www.coursera.org/courseraplus?utm_source=gg&utm_medium=sem&utm_campaign=b2c_apac_coursera-plus_coursera_ftcof_subscription_arte_may-24_dr_geo-set-1-multi-phrase-search-term_sem_rsa_gads_lg-all&utm_content=b2c&campaignid=21276384423&adgroupid=162958956195&device=c&keyword=%EC%BD%94%20%EC%84%B8%EB%9D%BC&matchtype=p&network=g&devicemodel=&adpostion=&creativeid=699165981561&hide_mobile_promo&gad_source=1&gclid=Cj0KCQjw2ou2BhCCARIsANAwM2GlFV83d2YD7UJBT1HNkCOrTld4Ev5c5liza03Znw3Zx7u2BXm4Y5caAmyfEALw_wcB)
- [유데미](https://www.udemy.com)

## 데이터 정의
```json
{
    "lecture_url": "https://www.inflearn.com/course/그림으로-배우는-쿠버네티스",
    "keyword": "GCP",
    "platform_name": "Inflearn",
    "content": {
        "lecture_id": "13DjwIg4CXSVeSNx456rie",
        "lecture_name": "그림으로 배우는 쿠버네티스(v1.30) - {{ x86-64, arm64 }}",
        "price": 198000,
        "origin_price": 198000,
        "description": "쿠버네티스(☸)의 많은 부분을 그림으로 배울 수 있도록 구성되어 있습니다.",
        "what_do_i_learn": [
            "쿠버네티스를 이루는 코드(YAML)을 이해할 수 있어요",
            "쿠버네티스 인프라의 조건들을 코드를 통해서 확인할 수 있어요",
            "kubeadm을 통해서 실제로 쿠버네티스 클러스터를 구현할 수 있어요"
        ],
        "tag": [
            "Kubernetes",
            "Docker"
        ],
        "level": "중급이상",
        "teacher": "조훈(Hoon Jo)",
        "scope": 4.9,
        "review_count": 107,
        "lecture_time": "29시간 27분",
        "thumbnail_url": "https://cdn.inflearn.com/public/courses/327444/cover/e6464bbe-144f-4115-b0cd-5b466bf086f1/327444.png"
    }
}
```

|컬럼 이름|컬럼 타입|
|:-:|:-:|
|lecture_url|String|
|keyword|String|
|platform_name|String|
|lecture_id|String|
|lecture_name|String|
|price|Int|
|origin_price|Int|
|description|String|
|what_do_i_learn|String|
|tag|String|
|level|String|
|teacher|String|
|scope|Float|
|review_count|Int|
|lecture_time|String|
|thumbnail_url|String|

## Airflow Architecture
<img width="350" alt="스크린샷 2024-08-20 오후 3 14 30" src="https://github.com/user-attachments/assets/4a0c69ce-6f0f-488e-8b49-1ce178d7b3b9">

## DAG 구성
<img width="265" alt="스크린샷 2024-08-20 오후 3 17 16" src="https://github.com/user-attachments/assets/ba7097a8-43f4-4858-97dc-dc0000cd90a1">

### load_data_from_three_platform -> 실행 주기 2주
<img width="507" alt="스크린샷 2024-08-20 오후 3 18 48" src="https://github.com/user-attachments/assets/3ec1db33-5276-4d09-9e06-76ec02792de2">

### load_lecture_price -> 실행 주기 1일
<img width="179" alt="스크린샷 2024-08-20 오후 3 19 34" src="https://github.com/user-attachments/assets/585c2362-9830-455d-be3a-85aedefc3c10">

### Custom Operator
<img width="117" alt="스크린샷 2024-08-20 오후 3 17 34" src="https://github.com/user-attachments/assets/572b643e-10a7-4ad9-b0ab-1241c20d600f">

## 활용 기술
### 언어
<img src="https://img.shields.io/badge/Python-3776AB?style=flat&logo=Python&logoColor=white"> <img src="https://img.shields.io/badge/MySQL-4479A1?style=flat&logo=MySQL&logoColor=white">

### 데이터 레이크
<img src="https://img.shields.io/badge/Amazon S3-569A31?style=flat&logo=Amazon S3&logoColor=white">

### 프로덕션 데이터베이스
<img src="https://img.shields.io/badge/Amazon RDS-527FFF?style=flat&logo=Amazon RDS&logoColor=white">

### 커뮤니케이션 & 협업
<img src="https://img.shields.io/badge/GitHub-181717?style=flat&logo=GitHub&logoColor=white"> <img src="https://img.shields.io/badge/Gather-2535A0?style=flat&logo=Gather&logoColor=white"> <img src="https://img.shields.io/badge/Slack-4A154B?style=flat&logo=Slack&logoColor=white"> <img src="https://img.shields.io/badge/Notion-000000?style=flat&logo=Notion&logoColor=white"> <img src="https://img.shields.io/badge/Jira-0052CC?style=flat&logo=Jira&logoColor=white">

# 참여자 정보
<table>
  <tbody>
    <tr>
      <td align="center"><a href="https://github.com/kdk0411"><img src="https://avatars.githubusercontent.com/u/99461483?v=4" width="100px;" alt=""/><br /><sub><b>김동기</b></sub></a><br /></td>
      <td align="center"><a href="https://github.com/zjacom"><img src="https://avatars.githubusercontent.com/u/112957047?v=4" width="100px;" alt=""/><br /><sub><b>김승훈</b></sub></a><br /></td>
      <td align="center"><a href="https://github.com/Kim-2301"><img src="https://avatars.githubusercontent.com/u/84478606?v=4" width="100px;" alt=""/><br /><sub><b>김승현</b></sub></a><br /></td>
      <td align="center"><a href="https://github.com/7xxogre"><img src="https://avatars.githubusercontent.com/u/61622859?v=4" width="100px;" alt=""/><br /><sub><b>김유민</b></sub></a><br /></td>
    </tr>
  </tbody>
</table>

### 역할 분담
- 김동기 : Udemy 데이터
- 김승훈 : Inflearn 데이터 및 DAG 통합
- 김승현 : Coursera 데이터
- 김유민 : OpenAI API를 사용한 LLM 활용 DAG 작성
