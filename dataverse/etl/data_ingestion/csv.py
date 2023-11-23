
"""
Load CSV data
"""

from dataverse.etl import register_etl
from dataverse.utils.format import huggingface2parquet
from dataverse.utils.format import load_huggingface_dataset
from typing import Union, List


@register_etl
def data_ingestion___csv___csv2raw(
    spark,
    path: Union[str, List[str]],
    repartition=20,
    verbose=True,
    *args,
    **kwargs
):
    """
    Args:
        spark (SparkSession): spark session
        path (str or list): csv path
        repartition (int): the number of partitions
        verbose (bool): whether to print the information of the dataset
    """
    if isinstance(path, str):
        path = [path]

    df = spark.read.csv(*path, header=True)
    rdd = df.rdd.repartition(repartition)
    rdd = rdd.map(lambda row: row.asDict())

    return rdd
