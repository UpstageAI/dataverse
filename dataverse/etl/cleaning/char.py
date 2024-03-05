"""
A collection of modules for cleaning data at the character level.
For example: whitespace, accent characters, and unprintable characters.

Copyright (c) 2024-present Upstage Co., Ltd.
Apache-2.0 license
"""

import re
from typing import Union

from pyspark.rdd import RDD
from pyspark.sql import DataFrame

from dataverse.etl.registry import register_etl


@register_etl
def cleaning___char___normalize_whitespace(
    spark, data: Union[RDD, DataFrame], subset: str = "text", *args, **kwargs
) -> RDD:
    r"""
    Normalize whitespace.
    - Strips the leading and trailing whitespaces.
    - Replaces all consecutive whitespaces with a single space,
    excluding ``\n`` and ``\r`` characters.

    Args:
        spark (SparkSession): The Spark session object.
        data (Union[RDD, DataFrame]): The input data to be processed.
        subset (str): A subset or column to consider. Defaults to 'text'.

    Returns:
        RDD: The processed data with normalized whitespace.
    """
    if isinstance(data, DataFrame):
        data = data.rdd
        data = data.map(lambda row: row.asDict())

    pattern = re.compile(r"[^\S\r\n]+")

    def _normalize_whitespace(row):
        row[subset] = re.sub(pattern, " ", row[subset].strip())
        return row

    data = data.map(_normalize_whitespace)

    return data


@register_etl
def cleaning___char___remove_unprintable(
    spark, data: Union[RDD, DataFrame], subset="text", *args, **kwargs
) -> RDD:
    """
    Remove all the non-printable characters.

    Code is from facebookresearch/cc_net
    https://github.com/facebookresearch/cc_net/blob/main/cc_net/text_normalizer.py

    Args:
        spark (SparkSession): The Spark session object.
        data (Union[RDD, DataFrame]): The input data to be processed.
        subset (str): A subset or column to consider. Defaults to 'text'.

    Returns:
        RDD: The processed data with unprintable characters are removed.
    """
    if isinstance(data, DataFrame):
        data = data.rdd
        data = data.map(lambda row: row.asDict())

    def _remove_non_printable_char(row):
        new_lines = []
        for line in row[subset].split("\n"):
            new_lines.append(
                re.sub(f"[{''.join(map(chr, list(range(0,32)) + list(range(127,160))))}]", "", line)
            )
        row[subset] = "\n".join(new_lines)
        return row

    data = data.map(_remove_non_printable_char)

    return data
