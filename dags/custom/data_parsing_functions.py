import requests
import hashlib
import json
import logging


def convert_unix_timestamp_to_hours_minutes(timestamp):
    total_seconds = int(timestamp)
    # 시간 계산
    hours = total_seconds // 3600
    # 남은 분 계산
    minutes = (total_seconds % 3600) // 60

    # 결과 포맷팅
    if hours > 0 and minutes > 0:
        return f"{hours}시간 {minutes}분"
    elif hours > 0:
        return f"{hours}시간"
    elif minutes > 0:
        return f"{minutes}분"
    else:
        return "0분"


def parsing_lecture_details(id, lecture_url, keyword, sort_type):
    url_v1 = f"https://www.inflearn.com/course/client/api/v1/course/{id}/online/info"
    url_v2 = f"https://www.inflearn.com/course/client/api/v1/course/{id}/contents"
    response = requests.get(url_v1)
    response.raise_for_status()
    try:
        json_data = response.json()
    except:
        logging.info(url_v1)

    data = json_data["data"]
    lecture_thumbnail = data["thumbnailUrl"]
    # is_new = data["isNew"]
    lecture_name = data["title"]
    lecture_time = data["unitSummary"]["runtime"]

    main_category = data["category"]["main"]["title"]
    mid_category = data["category"]["sub"]["title"]

    price = data["paymentInfo"]["payPrice"]
    review_count = data["review"]["count"]
    scope = data["review"]["averageStar"]
    teacher = data["instructors"][0]["name"]

    for ele in data["levels"]:
        if ele["isActive"]:
            level = ele["title"]
            break

    tags = []
    for ele in data["skillTags"]:
        tags.append(ele["title"])

    response = requests.get(url_v2)
    response.raise_for_status()
    try:
        json_data = response.json()
    except:
        logging.info(url_v2)

    description = json_data["data"]["description"]
    what_do_i_learn = json_data["data"]["abilities"]

    data = {
        "url": lecture_url,
        "keyword": keyword,
        "content": {
            "lecture_id": hashlib.md5(lecture_url.encode()).hexdigest(),
            "lecture_name": lecture_name,
            "price": int(price),
            "description": description,
            "whatdoilearn": what_do_i_learn,
            "tag": tags,
            "level": level,
            "teacher": teacher,
            "scope": float(scope),
            "review_count": int(review_count),
            "lecture_time": convert_unix_timestamp_to_hours_minutes(lecture_time),
            "thumbnail_url": lecture_thumbnail,
            "sort_type": sort_type,
        },
        "main_category": main_category,
        "mid_category": mid_category,
    }
    json_data = json.dumps(data, ensure_ascii=False, indent=4)
    return json_data


def parsing_lecture_reviews(id, lecture_url, *args):
    url = f"https://www.inflearn.com/api/v2/review/course/{id}?id={id}&pageNumber=1&pageSize=30&sort=RECENT"

    data = {
        "lecture_id": hashlib.md5(lecture_url.encode()).hexdigest(),
        "lecture_url": lecture_url,
        "reviews": [],
    }

    response = requests.get(url)
    response.raise_for_status()
    try:
        json_data = response.json()
    except:
        logging.info(url)

    if json_data["data"]["totalCount"] == 0:
        return data

    for item in json_data["data"]["items"]:
        if item["body"]:
            data["reviews"].append(item["body"])

    json_data = json.dumps(data, ensure_ascii=False, indent=4)
    return json_data
