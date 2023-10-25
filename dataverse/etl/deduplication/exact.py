

from pyspark.rdd import RDD
from pyspark.sql import DataFrame

from dataverse.etl.registry import register_etl

from typing import Union


@register_etl
def deduplication___exact___spark_df_column(spark, data: Union[RDD, DataFrame], subset=['text'], *args, **kwargs):
    """
    exact deduplication

    args:
        subset: subset or columns to consider if duplicated
    """
    if isinstance(data, RDD):
        df = data.toDF()

    assert isinstance(df, DataFrame), f"data must be DataFrame, got {type(data)}"
    df = df.dropDuplicates(subset=subset)
    return df