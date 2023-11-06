

from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.sql.functions import split, posexplode
from pyspark.sql.functions import collect_list

from dataverse.etl.registry import register_etl

import re
import unicodedata
from typing import Union



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
    spark,
    data: Union[RDD, DataFrame],
    subset='text',
    *args,
    **kwargs
):
    """
    remove all the unicode punctuations

    Code is from facebookresearch/cc_net
    https://github.com/facebookresearch/cc_net/blob/main/cc_net/text_normalizer.py

    args:
        subset (str): subset or columns to consider
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
    spark,
    data: Union[RDD, DataFrame],
    subset='text',
    *args,
    **kwargs
):
    """
    replace all the unicode punctuations

    Code is from facebookresearch/cc_net
    https://github.com/facebookresearch/cc_net/blob/main/cc_net/text_normalizer.py

    args:
        subset (str): subset or columns to consider if duplicated
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
    spark,
    data: Union[RDD, DataFrame],
    subset='text',
    *args,
    **kwargs
):
    """
    normalize the unicode

    Args:
        subset (str): subset or columns to consider for normalization
    """
    if isinstance(data, DataFrame):
        data = data.rdd
        data = data.map(lambda row: row.asDict())

    def _normalize(row):
        row[subset] = unicodedata.normalize("NFC", row[subset])
        return row

    data = data.map(_normalize)
    return data
