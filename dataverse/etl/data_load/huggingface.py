
"""
Data loading to Huggingface Datasets

Huggingface support spark natively!
https://huggingface.co/docs/datasets/use_with_spark
"""

import os
from datasets import Dataset
from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from dataverse.etl import register_etl


@register_etl
def data_load___huggingface___ufl2hf_hub(spark, ufl, hub_path, repartition=1, *args, **kwargs):
    """
    save data to hf dataset and upload to hub
    """
    NotImplementedError()
    return None

@register_etl
def data_load___huggingface___ufl2hf(spark, ufl, save_path, repartition=1, *args, **kwargs):
    """
    save data to hf dataset and return the path
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
def data_load___huggingface___ufl2hf_obj(spark, ufl, repartition=1, *args, **kwargs):
    """
    convert data to huggingface dataset object
    """
    if isinstance(ufl, RDD):
        ufl = ufl.toDF()

    assert isinstance(ufl, DataFrame), f"ufl must be RDD or DataFrame, got {type(ufl)}"

    ufl = ufl.repartition(repartition)
    hf_dataset = Dataset.from_spark(ufl)

    return hf_dataset