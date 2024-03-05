"""
Sampling module for data ingestion

Copyright (c) 2024-present Upstage Co., Ltd.
Apache-2.0 license
"""

from typing import Union

from pyspark.rdd import RDD
from pyspark.sql import DataFrame

from dataverse.etl import register_etl


@register_etl
def utils___sampling___random(
    spark,
    data: Union[RDD, DataFrame],
    replace: bool = False,
    sample_n_or_frac: float = 0.1,
    seed: int = 42,
    *args,
    **kwargs
) -> RDD:
    """
    Randomly sample the input RDD.

    Args:
        spark (SparkSession): The Spark session object.
        data (Union[RDD, DataFrame]): The input data to be sampled.
        replace (bool, optional): Whether to sample with replacement. Defaults to False.
        sample_n_or_frac (float, optional): Number of samples to take or fraction of the RDD to sample. Defaults to 0.1
        seed (int, optional): Seed for the random number generator. Defaults to 42.

    Returns:
        RDD: Sampled RDD
    """
    if isinstance(data, DataFrame):
        data = data.rdd
        data = data.map(lambda row: row.asDict())

    if isinstance(sample_n_or_frac, float):
        data = data.sample(replace, sample_n_or_frac, seed)

    # XXX: Take too long, 1M sample takes over 10 mins and didn't finish
    elif isinstance(sample_n_or_frac, int):
        data = data.takeSample(replace, sample_n_or_frac, seed)
    return data
