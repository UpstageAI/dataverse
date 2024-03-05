"""
Copyright (c) 2024-present Upstage Co., Ltd.
Apache-2.0 license
"""

import re
import unicodedata
from typing import Union

from pyspark.rdd import RDD
from pyspark.sql import DataFrame

from dataverse.etl.registry import register_etl

UNICODE_PUNCT = {
    "，": ",",
    "。": ".",
    "、": ",",
    "„": '"',
    "”": '"',
    "“": '"',
    "«": '"',
    "»": '"',
    "１": '"',
    "」": '"',
    "「": '"',
    "《": '"',
    "》": '"',
    "´": "'",
    "∶": ":",
    "：": ":",
    "？": "?",
    "！": "!",
    "（": "(",
    "）": ")",
    "；": ";",
    "–": "-",
    "—": " - ",
    "．": ". ",
    "～": "~",
    "’": "'",
    "…": "...",
    "━": "-",
    "〈": "<",
    "〉": ">",
    "【": "[",
    "】": "]",
    "％": "%",
    "►": "-",
}


@register_etl
def cleaning___unicode___remove_punct(
    spark, data: Union[RDD, DataFrame], subset: str = "text", *args, **kwargs
) -> RDD:
    """
    Removes all the Unicode punctuations.

    Code is from facebookresearch/cc_net
    https://github.com/facebookresearch/cc_net/blob/main/cc_net/text_normalizer.py

    Args:
        spark (SparkSession): The Spark session object.
        data (Union[RDD, DataFrame]): The input data to be processed. It can be either an RDD or a DataFrame.
        subset (str, optional): A subset or column to consider. Defaults to 'text'.

    Returns:
        The cleaned data.
    """
    if isinstance(data, DataFrame):
        data = data.rdd
        data = data.map(lambda row: row.asDict())

    def _remove_unicode_punct(row):
        row[subset] = re.sub(f"[{''.join(UNICODE_PUNCT.keys())}]", "", row[subset])
        return row

    data = data.map(_remove_unicode_punct)

    return data


@register_etl
def cleaning___unicode___replace_punct(
    spark, data: Union[RDD, DataFrame], subset: str = "text", *args, **kwargs
) -> RDD:
    """
    Replace all the unicode punctuations

    Code is from facebookresearch/cc_net
    https://github.com/facebookresearch/cc_net/blob/main/cc_net/text_normalizer.py

    Args:
        spark (SparkSession): The Spark session object.
        data (Union[RDD, DataFrame]): The input data to be processed. It can be either an RDD or a DataFrame.
        subset (str, optional): A subset or column to consider. Defaults to 'text'.

    Returns:
        The cleaned data.
    """
    if isinstance(data, DataFrame):
        data = data.rdd
        data = data.map(lambda row: row.asDict())

    def _replace_unicode_punct(row):
        row[subset] = "".join((UNICODE_PUNCT.get(c, c) for c in row[subset]))
        return row

    data = data.map(_replace_unicode_punct)

    return data


@register_etl
def cleaning___unicode___normalize(
    spark, data: Union[RDD, DataFrame], subset="text", *args, **kwargs
):
    """
    Normalize the unicode

    Args:
        spark (SparkSession): The Spark session object.
        data (Union[RDD, DataFrame]): The input data to be processed. It can be either an RDD or a DataFrame.
        subset (str, optional): A subset or column to consider. Defaults to 'text'.

    Returns:
        The cleaned data.
    """
    if isinstance(data, DataFrame):
        data = data.rdd
        data = data.map(lambda row: row.asDict())

    def _normalize(row):
        row[subset] = unicodedata.normalize("NFC", row[subset])
        return row

    data = data.map(_normalize)
    return data
