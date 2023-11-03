

from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from typing import Union, Optional

from dataverse.etl.registry import register_etl



@register_etl
def cleaning___length___char_len_filter(
    spark,
    data: Union[RDD, DataFrame],
    subset='text',
    min_len: int = None,
    max_len: int = None,
    *args,
    **kwargs
):
    """
    filter by char length

    min_len <= len <= max_len
    - if min_len is None, then len <= max_len
    - if max_len is None, then len >= min_len

    args:
        subset: column to filter
        min_len: minimum length to filter
        max_len: maximum length to filter

    """
    if isinstance(data, DataFrame):
        data = data.rdd
        data = data.map(lambda row: row.asDict())

    assert min_len is not None or max_len is not None, "min_len and max_len cannot be None at the same time"

    if min_len is not None and max_len is not None:
        data = data.filter(lambda row: min_len <= len(row[subset]) <= max_len)
    elif min_len is None:
        data = data.filter(lambda row: len(row[subset]) <= max_len)
    elif max_len is None:
        data = data.filter(lambda row: min_len <= len(row[subset]))

    return data


@register_etl
def cleaning___length___word_len_filter(
    spark,
    data: Union[RDD, DataFrame],
    subset='text',
    min_len: int = None,
    max_len: int = None,
    *args,
    **kwargs
):
    """
    filter by word length

    min_len <= len <= max_len
    - if min_len is None, then len <= max_len
    - if max_len is None, then len >= min_len

    args:
        subset: column to filter
        min_len: minimum length to filter
        max_len: maximum length to filter

    """
    if isinstance(data, DataFrame):
        data = data.rdd
        data = data.map(lambda row: row.asDict())

    assert min_len is not None or max_len is not None, "min_len and max_len cannot be None at the same time"

    if min_len is not None and max_len is not None:
        data = data.filter(lambda row: min_len <= len(row[subset].split()) <= max_len)
    elif min_len is None:
        data = data.filter(lambda row: len(row[subset].split()) <= max_len)
    elif max_len is None:
        data = data.filter(lambda row: min_len <= len(row[subset].split()))

    return data