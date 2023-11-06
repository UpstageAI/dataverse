

from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.sql.functions import split, posexplode
from pyspark.sql.functions import collect_list

from dataverse.etl.registry import register_etl

import re
import unicodedata
from typing import Union



@register_etl
def cleaning___number___normalize(
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

    Code is from facebookresearch/cc_net
    https://github.com/facebookresearch/cc_net/blob/main/cc_net/text_normalizer.py

    args:
        subset (str): subset or columns to consider
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
