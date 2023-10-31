

from pyspark.rdd import RDD
from pyspark.sql import DataFrame

from dataverse.etl.registry import register_etl

from typing import Union


@register_etl
def deduplication___exact___column(spark, data: Union[RDD, DataFrame], subset=['text'], *args, **kwargs):
    """
    exact column deduplication

    args:
        subset: subset or columns to consider if duplicated
    """
    if isinstance(data, RDD):
        data = data.toDF()

    assert isinstance(data, DataFrame), f"data must be DataFrame, got {type(data)}"
    data = data.dropDuplicates(subset=subset)
    return data