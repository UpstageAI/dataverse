
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
    csv_path,
    repartition=20,
    verbose=True,
    *args,
    **kwargs
):
    """
    convert huggingface dataset to raw format as dict

    Args:
        spark (SparkSession): spark session
        csv_path (str): the path of the csv file
        repartition (int): the number of partitions
        verbose (bool): whether to print the information of the dataset
    """
    df = spark.read.csv(csv_path, header=True)
    rdd = df.rdd.repartition(repartition)
    rdd = rdd.map(lambda row: row.asDict())

    return rdd
