

from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.sql.functions import split, posexplode
from pyspark.sql.functions import collect_list

from dataverse.etl.registry import register_etl

import re
import unicodedata
from typing import Union


@register_etl
def cleaning___char___normalize_whitespace(
    spark,
    data: Union[RDD, DataFrame],
    subset='text',
    *args,
    **kwargs
):
    """
    Normalize the whitespaces
    - strip the leading and trailing whitespaces
    - replace all the consecutive whitespaces with a single space
        - this excludes the \n and \r

    args:
        subset (str): subset or columns to consider
    """
    if isinstance(data, DataFrame):
        data = data.rdd
        data = data.map(lambda row: row.asDict())

    pattern = re.compile("[^\S\r\n]+") 
    def _normalize_whitespace(row):
        row[subset] = re.sub(pattern, ' ', row[subset].strip())
        return row

    data = data.map(_normalize_whitespace)

    return data



@register_etl
def cleaning___char___remove_unprintable(
    spark,
    data: Union[RDD, DataFrame],
    subset='text',
    *args,
    **kwargs
):
    """
    remove all the non-printable characters

    Code is from facebookresearch/cc_net
    https://github.com/facebookresearch/cc_net/blob/main/cc_net/text_normalizer.py

    args:
        subset (str): subset or columns to consider
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
