"""
Copyright (c) 2024-present Upstage Co., Ltd.
Apache-2.0 license
"""

import re
from typing import Union

from pyspark.rdd import RDD
from pyspark.sql import DataFrame

from dataverse.etl.registry import register_etl


@register_etl
def cleaning___number___normalize(
    spark,
    data: Union[RDD, DataFrame],
    subset: str = "text",
    assign_number: int = 0,
    *args,
    **kwargs,
) -> RDD:
    """
    Convert all the number to assigned number (e.g. 0)

    Code is from facebookresearch/cc_net
    https://github.com/facebookresearch/cc_net/blob/main/cc_net/text_normalizer.py

    Examples:

        - input

        +----------+
        |   text   |
        +==========+
        |      1234|
        | 1234.5678|
        +----------+

        - output

        +----------+
        |   text   |
        +==========+
        |      0000|
        | 0000.0000|
        +----------+

    Args:
        spark (SparkSession): The Spark session object.
        data (Union[RDD, DataFrame]): The input data to be processed. It can be either an RDD or a DataFrame.
        subset (str, optional): A subset or column to consider. Defaults to 'text'.
        assign_number (int, optional): The number to assign. Default is 0.

    Returns:
        The normalized data.

    Raises:
        AssertionError: If assign_number is not between 0 and 9 (inclusive).
    """
    if isinstance(data, DataFrame):
        data = data.rdd
        data = data.map(lambda row: row.asDict())

    def _normalize_number(row):
        row[subset] = re.sub(r"\d", str(assign_number), row[subset])
        return row

    # assign_number is between 0 ~ 9
    assert assign_number in range(
        10
    ), f"assign_number should be between 0 ~ 9 but got {assign_number}"
    data = data.map(_normalize_number)

    return data
