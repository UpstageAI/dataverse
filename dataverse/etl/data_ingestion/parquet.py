"""
Copyright (c) 2024-present Upstage Co., Ltd.
Apache-2.0 license
"""
from typing import List, Union

from pyspark.rdd import RDD

from dataverse.etl import register_etl


@register_etl
def data_ingestion___parquet___pq2raw(
    spark, path: Union[str, List[str]], repartition=20, *args, **kwargs
) -> RDD:
    """
    Reads parquet files into an RDD and repartitions it.

    Args:
        spark (SparkSession): The Spark session.
        path (str or list): The path of the parquet files.
        repartition (int): The number of partitions.

    Returns:
        rdd: The repartitioned RDD containing the data from the parquet files.
    """
    if isinstance(path, str):
        path = [path]

    df = spark.read.parquet(*path)
    rdd = df.rdd.repartition(repartition)
    rdd = rdd.map(lambda row: row.asDict())
    return rdd
