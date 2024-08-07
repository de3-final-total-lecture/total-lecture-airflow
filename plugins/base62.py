import hashlib

# Base62 인코딩을 위한 문자 집합
BASE62 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"


def base62_encode(num):
    """숫자를 Base62로 인코딩"""
    if num == 0:
        return BASE62[0]

    encoded = []
    while num:
        num, rem = divmod(num, 62)
        encoded.append(BASE62[rem])
    return "".join(reversed(encoded))


def encoding_url(url):
    """URL을 Base62로 인코딩"""
    # URL의 해시값을 계산
    url_hash = hashlib.md5(url.encode()).hexdigest()
    # 해시값을 16진수에서 10진수로 변환
    num = int(url_hash, 16)
    # 10진수를 Base62로 인코딩
    return base62_encode(num)
