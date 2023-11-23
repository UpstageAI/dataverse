
"""
"""

from dataverse.etl import register_etl
from typing import Union, List


@register_etl
def data_ingestion___parquet___pq2ufl(
    spark,
    path: Union[str, List[str]],
    repartition=20,
    *args,
    **kwargs
):
    """
    Args:
        spark (SparkSession): spark session
        path (str or list): the path of the parquet files
        repartition (int): the number of partitions
    """
    if isinstance(path, str):
        path = [path]

    df = spark.read.parquet(*path)
    rdd = df.rdd.repartition(repartition)
    rdd = rdd.map(lambda row: row.asDict())
    return rdd