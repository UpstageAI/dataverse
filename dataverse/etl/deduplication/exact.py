"""
Copyright (c) 2024-present Upstage Co., Ltd.
Apache-2.0 license
"""


from typing import List, Union

from pyspark.rdd import RDD
from pyspark.sql import DataFrame

from dataverse.etl.registry import register_etl


@register_etl
def deduplication___exact___column(
    spark, data: Union[RDD, DataFrame], subset: List[str] = ["text"], *args, **kwargs
):
    """
    Exact column deduplication

    Args:
        spark (SparkSession): The Spark session object.
        data (Union[RDD, DataFrame]): The input data to be deduplicated..
        subset(List[str]): Subset of columns to consider for duplication check. Default to ['text'].

    Returns:
        Deduplicated DataFrame object
    """
    if isinstance(data, RDD):
        data = data.toDF()

    assert isinstance(data, DataFrame), f"data must be DataFrame, got {type(data)}"
    data = data.dropDuplicates(subset=subset)
    return data
