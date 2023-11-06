

from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.sql.functions import split, posexplode
from pyspark.sql.functions import collect_list

from dataverse.etl.registry import register_etl

import re
import unicodedata
from typing import Union

import re
from enum import IntEnum

class KoreanType(IntEnum):
    JAUM = 0
    MOUM = 1
    COMPLETE = 2
    ELSE = -1

KOR_BEGIN = 44032
KOR_END = 55203
CHOSUNG_BASE = 588
JUNGSUNG_BASE = 28
JAUM_BEGIN = 12593
JAUM_END = 12622
MOUM_BEGIN = 12623
MOUM_END = 12643

CHOSUNG = ["ㄱ", "ㄲ", "ㄴ", "ㄷ", "ㄸ", "ㄹ", "ㅁ", "ㅂ", "ㅃ", "ㅅ", "ㅆ", "ㅇ", "ㅈ", "ㅉ", "ㅊ", "ㅋ", "ㅌ", "ㅍ", "ㅎ"]
JUNGSUNG = ["ㅏ", "ㅐ", "ㅑ", "ㅒ", "ㅓ", "ㅔ", "ㅕ", "ㅖ", "ㅗ", "ㅘ", "ㅙ", "ㅚ", "ㅛ", "ㅜ", "ㅝ", "ㅞ", "ㅟ", "ㅠ", "ㅡ", "ㅢ", "ㅣ"]
JONGSUNG = [" ", "ㄱ", "ㄲ", "ㄳ", "ㄴ", "ㄵ", "ㄶ", "ㄷ", "ㄹ", "ㄺ", "ㄻ", "ㄼ", "ㄽ", "ㄾ", "ㄿ", "ㅀ", "ㅁ", "ㅂ", "ㅄ", "ㅅ", "ㅆ", "ㅇ", "ㅈ", "ㅊ", "ㅋ", "ㅌ", "ㅍ", "ㅎ"]

JAUM = ["ㄱ", "ㄲ", "ㄳ", "ㄴ", "ㄵ", "ㄶ", "ㄷ", "ㄸ", "ㄹ", "ㄺ", "ㄻ", "ㄼ", "ㄽ", "ㄾ", "ㄿ", "ㅀ", "ㅁ", "ㅂ", "ㅃ", "ㅄ", "ㅅ", "ㅆ", "ㅇ", "ㅈ", "ㅉ", "ㅊ", "ㅋ", "ㅌ", "ㅍ", "ㅎ"]
MOUM = ["ㅏ", "ㅐ", "ㅑ", "ㅒ", "ㅓ", "ㅔ", "ㅕ", "ㅖ", "ㅗ", "ㅘ", "ㅙ", "ㅚ", "ㅛ", "ㅜ", "ㅝ", "ㅞ", "ㅟ", "ㅠ", "ㅡ", "ㅢ", "ㅣ"]


def character_is_korean(c):
    i = ord(c)
    return (
        (KOR_BEGIN <= i <= KOR_END)
        or (JAUM_BEGIN <= i <= JAUM_END)
        or (MOUM_BEGIN <= i <= MOUM_END)
    )

def decompose(c):
    if not character_is_korean(c):
        return None

    i = ord(c)
    if JAUM_BEGIN <= i <= JAUM_END:
        return c, " ", " "
    if MOUM_BEGIN <= i <= MOUM_END:
        return " ", c, " "

    i -= KOR_BEGIN
    cho = i // CHOSUNG_BASE
    jung = (i - cho * CHOSUNG_BASE) // JUNGSUNG_BASE
    jong = i - cho * CHOSUNG_BASE - jung * JUNGSUNG_BASE

    return CHOSUNG[cho], JUNGSUNG[jung], JONGSUNG[jong]

def compose(chosung, jungsung, jongsung):
    unicode = KOR_BEGIN
    unicode += CHOSUNG_BASE * CHOSUNG.index(chosung)
    unicode += JUNGSUNG_BASE * JUNGSUNG.index(jungsung)
    unicode += JONGSUNG.index(jongsung)
    return chr(unicode)




@register_etl
def cleaning___korean___filter_by_ratio(
    spark,
    data: Union[RDD, DataFrame],
    subset='text',
    filter_type='word',
    korean_ratio=0.5,
    *args,
    **kwargs
):
    """
    filter out the text that has less than `korean_ratio` excluding space
    - korean_ratio=0.5
        - means that the text should have at least 50% of korean char or words

    [ caveat ]
    - regex to count korean doesn't work properly on characters that are not words
        - e.g. 안녕"하세요 is counted is 2 korean words
            - ["안녕", "하세요"]

    [ example ]
    - 한국어가 포함 비율이 50% 이상인 경우만 남김 
        - `char`
            - korean char - 17 / non-korean char - 3 / total char - 20
            - 17 / 20 > 0.5 -> True (survive!)
        - `word`
            - korean char - 6 / non-korean char - 1 / total char - 7
            - 6 / 7 > 0.5 -> True (survive!)
    - korean including 비율이 50% 미만인 경우 제거
        - `char`
            - korean char - 10 / non-korean char - 28 / total char - 38
            - 10 / 38 > 0.5 -> False (remove!)
        - `word`
            - korean char - 4 / non-korean char - 3 / total char - 7
            - 4 / 7 > 0.5 -> True (survive!)


    Code is from eleutherAI/dps and was modified
    https://github.com/EleutherAI/dps/blob/master/dps/spark/prep/korean_prep.py#L52

    args:
        subset (str): subset or columns to consider to filter
        filter_type (str): `char` or `word`
        korean_ratio (float): ratio of korean char or words
    """
    assert filter_type in ['char', 'word'], f"filter_type should be either `char` or `word` but got {filter_type}"
    assert 0. <= korean_ratio <= 1., f"korean_ratio should be between 0. ~ 1. but got {korean_ratio}"

    if isinstance(data, DataFrame):
        data = data.rdd
        data = data.map(lambda row: row.asDict())

    def _korean_ratio_filter(row):
        if row[subset] is None or len(row[subset]) == 0:
            return False

        if filter_type == 'char':
            korean_counts = len(re.findall("[ㄱ-힣]", row[subset]))
            all_counts = len(re.sub("[ \r\n\t\f\v]", "", row[subset]))
        if filter_type == 'word':
            korean_counts = len(re.findall(r'\b[\w]*[ㄱ-힣][\w]*\b', row[subset]))
            all_counts = len(re.findall(r'\b\w+\b', row[subset]))

        if all_counts == 0:
            return False

        return (korean_counts / all_counts) >= korean_ratio

    data = data.filter(_korean_ratio_filter)

    return data


def classify_korean_type(unicode):
    if JAUM_BEGIN <= unicode <= JAUM_END:
        return KoreanType.JAUM
    elif MOUM_BEGIN <= unicode <= MOUM_END:
        return KoreanType.MOUM
    elif KOR_BEGIN <= unicode <= KOR_END:
        return KoreanType.COMPLETE
    else:
        return KoreanType.ELSE

def reduce_repeated_emotions(text, num_repeats=2):
    if num_repeats > 0:
        repeat_chars_pattern = re.compile("(\w)\\1{2,}")
        text = repeat_chars_pattern.sub("\\1" * num_repeats, text)

    return text

@register_etl
def cleaning___korean___reduce_emoticon(
    spark,
    data: Union[RDD, DataFrame],
    subset='text',
    num_repeats=2,
    *args,
    **kwargs
):
    """
    reduce emoticon korean characters

    this will proceed reducing emotions ex) ㅋㅋㅋㅋ, ㅎㅎㅎㅎ 
    1. split complete korean character into individual characters, prev jaum, next moum
        - e.g. (remain) ㅋㅋ킄ㅋㅋㅋ -> ㅋㅋ킄ㅋㅋㅋ
            - only prev jaum, next moum will be splitted
        - e.g. (splited) ㅋㅋ쿠ㅜㅜㅜ -> ㅋㅋㅋㅜㅜㅜㅜ
    2. reduce repeating korean characters
        - e.g. ㅋㅋㅋㅋㅋ -> ㅋㅋ

    [ potential risk of splitting complete korean character ]
    splitting emoticon characters into individual characters has high risk inside
    so only left one case that is `complete korean character between jaum and moum`
    other cases were added also but due to the risk, wiped out

    - you can check the risk of other cases
        - when complete korean character followed by jaum
            - 케익ㄱㄱㄱ -> 케이ㄱㄱㄱ
            - 안녕하세요ㅋㅋ -> 안녕하세ㅋㅋ
        - when complete korean character is after jaum
            - ㅋㅋ큰일났어요 -> ㅋㅋㅡ일났어요

    Function that reduces repeating Korean characters
    - e.g. ㅋㅋㅋㅋㅋ => ㅋㅋ

    Args:
        subset (str): subset or columns to consider for reducing emotion
        num_repeats (int): number of repeating characters to reduce

    Reference
    - https://github.com/lovit/soynlp/blob/master/soynlp/normalizer/_normalizer.py
    - https://github.com/EleutherAI/dps/blob/master/dps/spark/prep/korean_prep.py
    """

    def _reduce_korean_emotion(row):
        text = row[subset]
        if not text:
            return row

        korean_types = [classify_korean_type(ord(c)) for c in text]
        last_idx = len(korean_types) - 1

        normalized_text = []
        for i, (korean_type, c) in enumerate(zip(korean_types, text)):

            # when complete korean character is between jaum and moum
            if (0 < i < last_idx) and (
                korean_types[i - 1] == KoreanType.JAUM
                and korean_type == KoreanType.COMPLETE
                and korean_types[i + 1] == KoreanType.MOUM
            ):
                cho, jung, jong = decompose(c)

                # case 1. when complete kor char is combination of prev jaum and next moum
                # e.g. ㅋ(쿠)ㅜ -> ㅋ(ㅋㅜ)ㅜ
                if (
                    cho == text[i - 1]
                    and jung == text[i + 1]
                    and jong == " "
                ):
                    normalized_text.append(cho)
                    normalized_text.append(jung)

                # case 2. otherwise, just leave it
                # e.g. ㅋ(쿵)ㅜ -> ㅋ(쿵)ㅜ
                else:
                    normalized_text.append(c)

            else:
                normalized_text.append(c)

        row[subset] = reduce_repeated_emotions("".join(normalized_text), num_repeats)

        return row

    if isinstance(data, DataFrame):
        data = data.rdd
        data = data.map(lambda row: row.asDict())

    data = data.map(_reduce_korean_emotion)

    return data
