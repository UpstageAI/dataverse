
from pyspark.rdd import RDD
from pyspark.sql import DataFrame

from dataverse.etl import register_etl
from dataverse.utils.format import huggingface2parquet
from dataverse.utils.format import load_huggingface_dataset
from dataverse.utils.format import get_uuidv1

from typing import Union, List
from omegaconf import ListConfig

import datasets


"""
Supported datasets:
https://huggingface.co/datasets/togethercomputer/RedPajama-Data-1T
https://huggingface.co/datasets/togethercomputer/RedPajama-Data-1T-Sample
"""



"""
1 stage data ingestion - default
====================================
direct loading ufl with one ETL process
"""
def convert2ufl(row):
    row['id'] = get_uuidv1()
    row['name'] = 'red_pajama'
    return row

@register_etl
def data_ingestion___red_pajama___parquet2ufl(spark, input_paths, repartition=20, *args, **kwargs):
    """
    convert parquet file to ufl
    """
    df = spark.read.parquet(*input_paths)
    rdd = df.rdd.repartition(repartition)
    rdd = rdd.map(lambda row: row.asDict())
    rdd = rdd.map(lambda x: convert2ufl(x))

    return rdd

@register_etl
def data_ingestion___red_pajama___hf2ufl(
    spark,
    name_or_path : Union[str, List[str]] = 'togethercomputer/RedPajama-Data-1T-Sample',
    split=None,
    from_disk=False,
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
        from_disk (bool): whether to load from disk
            - no split is allowed when from_disk is True
        repartition (int): the number of partitions
        verbose (bool): whether to print the information of the dataset
    """
    dataset = load_huggingface_dataset(name_or_path, split=split, from_disk=from_disk)
    parquet_path = huggingface2parquet(dataset, verbose=verbose)

    df = spark.read.parquet(parquet_path)
    rdd = df.rdd.repartition(repartition)
    rdd = rdd.map(lambda row: row.asDict())
    rdd = rdd.map(lambda x: convert2ufl(x))

    return rdd


"""
2 stage data ingestion - default
====================================
loading ufl with custom template with two ETL process
"""
@register_etl
def data_ingestion___red_pajama___hf2raw(
    spark,
    name_or_path : Union[str, List[str]] = 'togethercomputer/RedPajama-Data-1T-Sample',
    split=None,
    repartition=20,
    verbose=True,
    *args,
    **kwargs
):
    """
    convert huggingface dataset to raw format as dict

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


@register_etl
def data_ingestion___red_pajama___raw2ufl_templatev1(spark, ufl, *args, **kwargs):
    """
    convert raw format to ufl with custom template
    """
    def templatev1(row):
        row['id'] = get_uuidv1()
        row['name'] = 'red_pajama'
        return row

    ufl = ufl.map(lambda x: templatev1(x))

    return ufl

@register_etl
def data_ingestion___red_pajama___raw2ufl_templatev2(spark, ufl, *args, **kwargs):
    ...
    return ufl
