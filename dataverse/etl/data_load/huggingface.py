
"""
Data loading to Huggingface Datasets
"""

import os
from dataverse.etl.registry import register_etl


@register_etl
def data_load___huggingface___ufl2hf(ufl, save_path, repartition=1, *args, **kwargs):
    """
    save data to hf dataset and return the path
    """
    ...
    return save_path

@register_etl
def data_load___huggingface___ufl2hf_obj(ufl, repartition=1, *args, **kwargs):
    """
    convert data to huggingface dataset object
    """
    hf_obj = ...
    return hf_obj