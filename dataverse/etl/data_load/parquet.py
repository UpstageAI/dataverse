

"""
Data loading to Parquets
"""

import os
from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from dataverse.etl import register_etl


@register_etl
def data_load___parquet___ufl2parquet(spark, ufl, save_path, repartition=1, *args, **kwargs):
    """
    save data to parquet and return the path
    """
    if os.path.exists(save_path):
        raise ValueError(f"save_path {save_path} already exists")

    if isinstance(ufl, RDD):
        ufl = ufl.toDF()

    assert isinstance(ufl, DataFrame), f"ufl must be RDD or DataFrame, got {type(ufl)}"

    ufl = ufl.repartition(repartition)
    ufl.write.parquet(save_path, mode="overwrite")

    return save_path
