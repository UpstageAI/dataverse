
"""
normalization for text

Code is from facebookresearch/cc_net
https://github.com/facebookresearch/cc_net/blob/main/cc_net/text_normalizer.py
"""

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
def cleaning___normalization___remove_unicode_punct(
    spark,
    data: Union[RDD, DataFrame],
    subset='text',
    *args,
    **kwargs
):
    """
    remove all the unicode punctuations

    args:
        subset (str): subset or columns to consider if duplicated
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
def cleaning___normalization___replace_unicode_punct(
    spark,
    data: Union[RDD, DataFrame],
    subset='text',
    *args,
    **kwargs
):
    """
    replace all the unicode punctuations

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
def cleaning___normalization___remove_unprintable_char(
    spark,
    data: Union[RDD, DataFrame],
    subset='text',
    *args,
    **kwargs
):
    """
    remove all the non-printable characters

    args:
        subset (str): subset or columns to consider if duplicated
    """
    if isinstance(data, DataFrame):
        data = data.rdd
        data = data.map(lambda row: row.asDict())

    def _remove_non_printable_char(row):
        new_lines = []
        for line in row[subset].split("\n"):
            new_lines.append(
                re.sub(
                    f"[{''.join(map(chr, list(range(0,32)) + list(range(127,160))))}]",
                    "",
                    line
                )
            )
        row[subset] = "\n".join(new_lines)
        return row

    data = data.map(_remove_non_printable_char)

    return data


def strip_accents(text: str) -> str:
    """Strips accents from a piece of text."""
    nfd = unicodedata.normalize("NFD", text)
    output = [c for c in nfd if unicodedata.category(c) != "Mn"]
    if len(output) == text:
        return text
    return "".join(output)

@register_etl
def cleaning___normalization___strip_accents(
    spark,
    data: Union[RDD, DataFrame],
    subset='text',
    *args,
    **kwargs
):
    """
    strip accents from a piece of text
    - example
        - "café" -> "cafe"
        - "résumé" -> "resume"

    args:
        subset (str): subset or columns to consider if duplicated
    """
    if isinstance(data, DataFrame):
        data = data.rdd
        data = data.map(lambda row: row.asDict())

    def _strip_accents(row):
        row[subset] = strip_accents(row[subset])
        return row

    data = data.map(_strip_accents)

    return data


@register_etl
def cleaning___normalization___number(
    spark,
    data: Union[RDD, DataFrame],
    subset='text',
    assign_number=0,
    *args,
    **kwargs
):
    """
    convert all the number to assigned number (e.g. 0)
    example
    - 1234 -> 0000
    - 1234.5678 -> 0000.0000

    args:
        subset (str): subset or columns to consider if duplicated
        assign_number (int): number to assign
    """
    if isinstance(data, DataFrame):
        data = data.rdd
        data = data.map(lambda row: row.asDict())

    def _normalize_number(row):
        row[subset] = re.sub(r"\d", str(assign_number), row[subset])
        return row

    # assign_number is between 0 ~ 9
    assert assign_number in range(10), f"assign_number should be between 0 ~ 9 but got {assign_number}"
    data = data.map(_normalize_number)

    return data
