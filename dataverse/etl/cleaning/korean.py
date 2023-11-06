

from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.sql.functions import split, posexplode
from pyspark.sql.functions import collect_list

from dataverse.etl.registry import register_etl

import re
import unicodedata
from typing import Union



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
        subset (str): subset or columns to consider if duplicated
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
