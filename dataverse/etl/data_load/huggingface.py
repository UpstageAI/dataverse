
"""
Data loading to Huggingface Datasets

Huggingface support spark natively!
https://huggingface.co/docs/datasets/use_with_spark
"""

import os
from dataverse.etl.registry import register_etl
from datasets import Dataset


@register_etl
def data_load___huggingface___ufl2hf_hub(ufl, hub_path, repartition=1, *args, **kwargs):
    """
    save data to hf dataset and upload to hub
    """
    NotImplementedError()
    return None

@register_etl
def data_load___huggingface___ufl2hf(ufl, save_path, repartition=1, *args, **kwargs):
    """
    save data to hf dataset and return the path
    """

    if os.path.exists(save_path):
        raise ValueError(f"save_path {save_path} already exists")

    # check if ufl is RDD or DataFrame
    if hasattr(ufl, "toDF"):
        ufl = ufl.toDF()

    ufl = ufl.repartition(repartition)
    hf_dataset = Dataset.from_spark(ufl)
    hf_dataset.save_to_disk(save_path)

    return save_path

@register_etl
def data_load___huggingface___ufl2hf_obj(ufl, repartition=1, *args, **kwargs):
    """
    convert data to huggingface dataset object
    """
    # check if ufl is RDD or DataFrame
    if hasattr(ufl, "toDF"):
        ufl = ufl.toDF()

    ufl = ufl.repartition(repartition)
    hf_dataset = Dataset.from_spark(ufl)

    return hf_dataset