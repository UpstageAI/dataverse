

"""
Data loading to Parquets
"""

import os
from dataverse.etl.registry import register_etl


@register_etl
def data_load___parquet___ufl(ufl, save_path, repartition=1, *args, **kwargs):
    """
    save data to parquet and return the path
    """
    if os.path.exists(save_path):
        raise ValueError(f"save_path {save_path} already exists")

    # check if ufl is RDD or DataFrame
    if hasattr(ufl, "toDF"):
        ufl = ufl.toDF()

    ufl = ufl.repartition(repartition)
    ufl.write.parquet(save_path, mode="overwrite")

    return save_path
