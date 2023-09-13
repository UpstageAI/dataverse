
from pyspark.rdd import RDD
from pyspark.sql import DataFrame

from pyspark import SparkContext
from dataverse.etl.registry import BaseETL
from dataverse.etl.registry import register_etl
from dataverse.etl.registry import ETLRegistry
from dataverse.utils.format import huggingface2parquet

from typing import Union

import datasets


"""
Supported datasets:
https://huggingface.co/datasets/togethercomputer/RedPajama-Data-1T
https://huggingface.co/datasets/togethercomputer/RedPajama-Data-1T-Sample
"""


import uuid

def get_uuid():
    return uuid.uuid1().hex

def convert2ufl(row):
    row['id'] = get_uuid()
    row['name'] = 'red_pajama'
    return row


@register_etl
def data_ingestion___red_pajama___parquet2ufl(spark, input_paths, repartition=20, *args, **kwargs):
    """
    convert parquet file to ufl
    """
    df = spark.read.parquet(",".join(input_paths))
    rdd = df.rdd.repartition(repartition)
    rdd = rdd.map(lambda row: row.asDict())
    rdd = rdd.map(lambda x: convert2ufl(x))

    return rdd

@register_etl
def data_ingestion___red_pajama___hf2ufl(
    spark,
    name_or_path,
    split,
    repartition=20,
    verbose=True,
    *args,
    **kwargs
):
    """
    convert huggingface dataset to ufl

    Args:
        spark (SparkSession): spark session
        name_or_path (str): the name or path of the huggingface dataset
        split (str): the split of the dataset
        repartition (int): the number of partitions
        verbose (bool): whether to print the information of the dataset
    """
    # load huggingface dataset
    dataset = datasets.load_dataset(name_or_path, split=split)
    parquet_path = huggingface2parquet(dataset, verbose=verbose)

    df = spark.read.parquet(parquet_path)
    rdd = df.rdd.repartition(repartition)
    rdd = rdd.map(lambda row: row.asDict())
    rdd = rdd.map(lambda x: convert2ufl(x))

    return rdd
