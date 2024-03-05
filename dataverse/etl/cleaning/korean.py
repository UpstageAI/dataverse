"""
This is only for Korean text datas.

Copyright (c) 2024-present Upstage Co., Ltd.
Apache-2.0 license
"""

import re
from enum import IntEnum
from typing import List, Union

from pyspark.rdd import RDD
from pyspark.sql import DataFrame

from dataverse.etl.registry import register_etl


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

# fmt: off
CHOSUNG = ["ㄱ", "ㄲ", "ㄴ", "ㄷ", "ㄸ", "ㄹ", "ㅁ", "ㅂ", "ㅃ", "ㅅ", "ㅆ", "ㅇ", "ㅈ", "ㅉ", "ㅊ", "ㅋ", "ㅌ", "ㅍ", "ㅎ"]
JUNGSUNG = ["ㅏ", "ㅐ", "ㅑ", "ㅒ", "ㅓ", "ㅔ", "ㅕ", "ㅖ", "ㅗ", "ㅘ", "ㅙ", "ㅚ", "ㅛ", "ㅜ", "ㅝ", "ㅞ", "ㅟ", "ㅠ", "ㅡ", "ㅢ", "ㅣ"]
JONGSUNG = [" ", "ㄱ", "ㄲ", "ㄳ", "ㄴ", "ㄵ", "ㄶ", "ㄷ", "ㄹ", "ㄺ", "ㄻ", "ㄼ", "ㄽ", "ㄾ", "ㄿ", "ㅀ", "ㅁ", "ㅂ", "ㅄ", "ㅅ", "ㅆ", "ㅇ", "ㅈ", "ㅊ", "ㅋ", "ㅌ", "ㅍ", "ㅎ"]

JAUM = ["ㄱ", "ㄲ", "ㄳ", "ㄴ", "ㄵ", "ㄶ", "ㄷ", "ㄸ", "ㄹ", "ㄺ", "ㄻ", "ㄼ", "ㄽ", "ㄾ", "ㄿ", "ㅀ", "ㅁ", "ㅂ", "ㅃ", "ㅄ", "ㅅ", "ㅆ", "ㅇ", "ㅈ", "ㅉ", "ㅊ", "ㅋ", "ㅌ", "ㅍ", "ㅎ"]
MOUM = ["ㅏ", "ㅐ", "ㅑ", "ㅒ", "ㅓ", "ㅔ", "ㅕ", "ㅖ", "ㅗ", "ㅘ", "ㅙ", "ㅚ", "ㅛ", "ㅜ", "ㅝ", "ㅞ", "ㅟ", "ㅠ", "ㅡ", "ㅢ", "ㅣ"]


# fmt: on
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


def cleaning___korean___filter_by_ratio(
    spark,
    data: Union[RDD, DataFrame],
    subset: str = "text",
    filter_type: str = "word",
    korean_ratio: float = 0.5,
    *args,
    **kwargs,
) -> RDD:
    """
    Filters out the text that has less than `korean_ratio` excluding space.

    Code is from eleutherAI/dps and was modified
    https://github.com/EleutherAI/dps/blob/master/dps/spark/prep/korean_prep.py#L52

    Args:
        spark (SparkSession): The Spark session object.
        data(Union[RDD, DataFrame]): The input data to be processed. It can be either an RDD or a DataFrame.
        subset(str, optional): A subset or column to consider. Defaults to 'text'.
        filter_type(str, optional): The type of filtering to be applied. Can be 'char' or 'word'. Defaults to 'word'.
        korean_ratio(float, optional) : The minimum ratio of Korean characters or words required for a text to survive the filtering. Defaults to 0.5.

    Returns:
        The filtered data with it's Korean ratio.

    Raises:
        ValueError: If the filter_type is not 'char' or 'word', or if the korean_ratio is not between 0 and 1.

    Examples:
        With korean_ratio = 0.5

            +------------------------------------------------+
            |                       text                     |
            +================================================+
            |  "한국어가 포함 비율이 50% 이상인 경우만 남김" |
            +------------------------------------------------+


            - filter_type = 'char' -> [survive!]
                - Korean characters: 17
                - Non-Korean characters: 3
                - Total characters: 20
                - Korean character ratio: 17 / 20 > 0.5 -> True
            - filter_type = 'word' -> [survive!]
                - Korean characters: 6
                - Non-Korean characters: 1
                - Total characters: 7
                - Korean character ratio: 6 / 7 > 0.5 -> True

            +------------------------------------------------+
            |                       text                     |
            +================================================+
            | "korean including 비율이 50% 미만인 경우 제거" |
            +------------------------------------------------+

            - filter_type = 'char' -> [remove!]
                - Korean characters: 10
                - Non-Korean characters: 28
                - Total characters: 38
                - Korean word ratio: 10 / 38 > 0.5 -> False
            - filter_type = 'word' -> [survive!]
                - Korean characters: 4
                - Non-Korean characters: 3
                - Total characters: 7
                - Korean word ratio: 4 / 7 > 0.5 -> True

    Note:
        - The regex to count Korean characters doesn't work properly on characters that are not words.
            - e.g 안녕"하세요 is counted is 2 korean words - ["안녕", "하세요"]
    """
    assert filter_type in [
        "char",
        "word",
    ], f"filter_type should be either `char` or `word` but got {filter_type}"
    assert (
        0.0 <= korean_ratio <= 1.0
    ), f"korean_ratio should be between 0. ~ 1. but got {korean_ratio}"

    if isinstance(data, DataFrame):
        data = data.rdd
        data = data.map(lambda row: row.asDict())

    def _korean_ratio_filter(row):
        if row[subset] is None or len(row[subset]) == 0:
            return False

        if filter_type == "char":
            korean_counts = len(re.findall("[ㄱ-힣]", row[subset]))
            all_counts = len(re.sub("[ \r\n\t\f\v]", "", row[subset]))
        if filter_type == "word":
            korean_counts = len(re.findall(r"\b[\w]*[ㄱ-힣][\w]*\b", row[subset]))
            all_counts = len(re.findall(r"\b\w+\b", row[subset]))

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
        repeat_chars_pattern = re.compile(r"(\w)\\1{2,}")
        text = repeat_chars_pattern.sub("\\1" * num_repeats, text)

    return text


@register_etl
def cleaning___korean___reduce_emoticon(
    spark,
    data: Union[RDD, DataFrame],
    subset: Union[str, List[str]] = "text",
    num_repeats: int = 2,
    *args,
    **kwargs,
) -> RDD:
    """
    Reduces emoticon Korean characters.

    It performs the following steps:

    1. Splits complete Korean characters into individual characters, preserving only the previous jaum and next moum.

        - e.g. (remain) ㅋㅋ킄ㅋㅋㅋ -> ㅋㅋ킄ㅋㅋㅋ
        - e.g. (splited) ㅋㅋ쿠ㅜㅜㅜ -> ㅋㅋㅋㅜㅜㅜㅜ

    2. Reduces repeating Korean characters.
        - e.g. ㅋㅋㅋㅋㅋ -> ㅋㅋ

    Args:
        spark(SparkSession): The Spark session object.
        data(Union[RDD, DataFrame]): The input data to be processed. It can be either an RDD or a DataFrame.
        subset(str, optional): A subset or columns to consider. Defaults to 'text'.
        num_repeats(int, optional): The number of repeating characters to reduce. Defaults to 2.

    Returns:
        RDD: The processed data with reduced emoticon Korean characters.

    Note:
        **[ potential risk of splitting complete korean character ]**

        splitting emoticon characters into individual characters has high risk inside
        so only left one case that is `complete korean character between jaum and moum`
        other cases were added also but due to the risk, wiped out

    References:
        - `soynlp normalizer.py <https://github.com/lovit/soynlp/blob/master/soynlp/normalizer/_normalizer.py>`_
        - `dps korean_prep.py <https://github.com/EleutherAI/dps/blob/master/dps/spark/prep/korean_prep.py>`_
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
                if cho == text[i - 1] and jung == text[i + 1] and jong == " ":
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
