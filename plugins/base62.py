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


def base62_decode(encoded):
    """Base62 문자열을 숫자로 디코딩"""
    num = 0
    for char in encoded:
        num = num * 62 + BASE62.index(char)
    return num


def encoding_url(url):
    """URL을 Base62로 인코딩"""
    # URL의 해시값을 계산
    url_hash = hashlib.md5(url.encode()).hexdigest()
    # 해시값을 16진수에서 10진수로 변환
    num = int(url_hash, 16)
    # 10진수를 Base62로 인코딩
    return base62_encode(num)


def decoding_url(encoded, original_url):
    """Base62 문자열을 원래 URL로 디코딩 (원래 URL을 비교하여 검증)"""
    num = base62_decode(encoded)
    url_hash = hashlib.md5(original_url.encode()).hexdigest()
    return num == int(url_hash, 16)
