
from pyspark.rdd import RDD
from pyspark.sql import DataFrame

from pyspark import SparkContext
from dataverse.etl.registry import BaseETL
from dataverse.etl.registry import register_etl
from dataverse.etl.registry import ETLRegistry

from typing import Union



@register_etl
def data_ingestion___parquet___df(spark, input_paths, *args, **kwargs):
    """
    """
    print("loading parquet text")
    df = spark.read.parquet(",".join(input_paths))
    return df
