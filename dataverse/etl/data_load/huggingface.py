"""
Data loading to Huggingface Datasets

Huggingface support spark natively!
https://huggingface.co/docs/datasets/use_with_spark

Copyright (c) 2024-present Upstage Co., Ltd.
Apache-2.0 license
"""

import os
from typing import Union

from datasets import Dataset
from pyspark.rdd import RDD
from pyspark.sql import DataFrame

from dataverse.etl import register_etl


@register_etl
def data_load___huggingface___ufl2hf_hub(spark, ufl, hub_path, repartition=1, *args, **kwargs):
    """
    TODO: Save data to Hugging Face dataset and upload to hub.
    """
    NotImplementedError()
    return None


@register_etl
def data_load___huggingface___ufl2hf(
    spark, ufl: Union[RDD, DataFrame], save_path: str, repartition: int = 1, *args, **kwargs
) -> str:
    """
    Save data to HuggingFace dataset and return the path.

    Args:
        spark(sparkSession): The Spark session.
        ufl(Union[RDD, DataFrame]):The input data to be saved.
        save_path(str): The path to save the HF dataset.
        repartition(int, optional): The number of partitions to repartition the data. Defaults to 1.

    Raises:
        ValueError: If the save_path already exists.
        AssertionError: If ufl is not an RDD or DataFrame.

    Returns:
        str: The path where the HuggingFace dataset is saved.
    """

    if os.path.exists(save_path):
        raise ValueError(f"save_path {save_path} already exists")

    if isinstance(ufl, RDD):
        ufl = ufl.toDF()

    assert isinstance(ufl, DataFrame), f"ufl must be RDD or DataFrame, got {type(ufl)}"

    ufl = ufl.repartition(repartition)
    hf_dataset = Dataset.from_spark(ufl)
    hf_dataset.save_to_disk(save_path)

    return save_path


@register_etl
def data_load___huggingface___ufl2hf_obj(
    spark, ufl: Union[RDD, DataFrame], repartition: int = 1, *args, **kwargs
) -> Dataset:
    """
    Convert data to HuggingFace dataset object.

    Args:
        spark(sparkSession): The Spark session.
        ufl(Union[RDD, DataFrame]):The input data to be saved.
        repartition(int, optional): The number of partitions to repartition the data. Defaults to 1.

    Returns:
        Dataset: The HuggingFace dataset object.

    Raises:
        AssertionError: If the input data is not RDD or DataFrame.
    """
    if isinstance(ufl, RDD):
        ufl = ufl.toDF()

    assert isinstance(ufl, DataFrame), f"ufl must be RDD or DataFrame, got {type(ufl)}"

    ufl = ufl.repartition(repartition)
    hf_dataset = Dataset.from_spark(ufl)

    return hf_dataset
