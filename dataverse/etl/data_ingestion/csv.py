"""
Load CSV data

Copyright (c) 2024-present Upstage Co., Ltd.
Apache-2.0 license
"""
from typing import List, Union

from pyspark.rdd import RDD

from dataverse.etl import register_etl

# from dataverse.utils.format import huggingface2parquet, load_huggingface_dataset


@register_etl
def data_ingestion___csv___csv2raw(
    spark, path: Union[str, List[str]], repartition: int = 20, verbose: bool = True, *args, **kwargs
) -> RDD:
    """
    Converts CSV data to raw RDD.

    Args:
        spark (SparkSession): The Spark session.
        path (Union[str, List[str]]): The path(s) to the CSV file(s).
        repartition (int, optional): The number of partitions for the RDD. Defaults to 20.
        verbose (bool, optional): Whether to print the information of the dataset.

    Returns:
        RDD: The raw RDD containing the CSV data.
    """
    if isinstance(path, str):
        path = [path]

    df = spark.read.csv(*path, header=True)
    rdd = df.rdd.repartition(repartition)
    rdd = rdd.map(lambda row: row.asDict())

    return rdd
