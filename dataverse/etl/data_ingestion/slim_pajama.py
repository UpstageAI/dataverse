
from pyspark.rdd import RDD
from pyspark.sql import DataFrame

from pyspark import SparkContext
from dataverse.etl.registry import BaseETL
from dataverse.etl.registry import register_etl
from dataverse.etl.registry import ETLRegistry
from dataverse.utils.format import huggingface2parquet
from dataverse.utils.format import load_huggingface_dataset

from typing import Union, List
from omegaconf import ListConfig

import datasets


"""
Supported datasets:
https://huggingface.co/datasets/cerebras/SlimPajama-627B
"""


@register_etl
def data_ingestion___slim_pajama___parquet2ufl(spark, input_paths, repartition=20, *args, **kwargs):
    """
    convert parquet file to ufl
    """
    df = spark.read.parquet(*input_paths)
    rdd = df.rdd.repartition(repartition)
    rdd = rdd.map(lambda row: row.asDict())
    return rdd

@register_etl
def data_ingestion___slim_pajama___hf2ufl(
    spark,
    name_or_path : Union[str, List[str]],
    split=None,
    repartition=20,
    verbose=True,
    *args,
    **kwargs
):
    """
    convert huggingface dataset to ufl

    Args:
        spark (SparkSession): spark session
        name_or_path (str or list): the name or path of the huggingface dataset
        split (str): the split of the dataset
        repartition (int): the number of partitions
        verbose (bool): whether to print the information of the dataset
    """
    dataset = load_huggingface_dataset(name_or_path, split=split)
    parquet_path = huggingface2parquet(dataset, verbose=verbose)

    df = spark.read.parquet(parquet_path)
    rdd = df.rdd.repartition(repartition)
    rdd = rdd.map(lambda row: row.asDict())
    return rdd
