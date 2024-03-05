"""
Data loading to Parquets

Copyright (c) 2024-present Upstage Co., Ltd.
Apache-2.0 license
"""

import os
from typing import Union

from pyspark.rdd import RDD
from pyspark.sql import DataFrame

from dataverse.etl import register_etl


@register_etl
def data_load___parquet___ufl2parquet(
    spark,
    ufl: Union[RDD, DataFrame],
    save_path: str,
    repartition: int = 1,
    *args,
    **kwargs,
) -> str:
    """
    Save data to parquet and return the path.

    Args:
        spark(sparkSession): The Spark session.
        ufl(Union[RDD, DataFrame]):The input data to be saved.
        save_path(str): The path to save the HF dataset.
        repartition(int, optional): The number of partitions to repartition the data. Defaults to 1.

    Raises:
        ValueError: If the save_path already exists.

    Returns:
        str: The path where the parquet file is saved.
    """
    if os.path.exists(save_path):
        raise ValueError(f"save_path {save_path} already exists")

    if isinstance(ufl, RDD):
        ufl = ufl.toDF()

    assert isinstance(ufl, DataFrame), f"ufl must be RDD or DataFrame, got {type(ufl)}"

    ufl = ufl.repartition(repartition)
    ufl.write.parquet(save_path, mode="overwrite")

    return save_path
