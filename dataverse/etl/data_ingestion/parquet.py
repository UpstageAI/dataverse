
from pyspark.rdd import RDD
from pyspark.sql import DataFrame

from pyspark import SparkContext
from dataverse.etl.registry import BaseETL
from dataverse.etl.registry import register_etl
from dataverse.etl.registry import ETLRegistry

from typing import Union



@register_etl
def data_ingestion___parquet___df(spark, input_paths, repartition=20, *args, **kwargs):
    """
    Load parquet file as DataFrame
    """
    df = spark.read.parquet(",".join(input_paths))
    df = df.repartition(repartition)
    return df

@register_etl
def data_ingestion___parquet___rdd(spark, input_paths, repartition=20, *args, **kwargs):
    """
    Load parquet file as RDD
    """
    df = spark.read.parquet(",".join(input_paths))
    rdd = df.rdd.repartition(repartition)
    return rdd
