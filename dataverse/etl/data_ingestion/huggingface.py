
"""
Load Huggingface data 

This is used just to load huggingface dataset without any refomatting
"""

from dataverse.etl import register_etl
from dataverse.utils.format import huggingface2parquet
from dataverse.utils.format import load_huggingface_dataset
from typing import Union, List


@register_etl
def data_ingestion___huggingface___hf2raw(
    spark,
    name_or_path : Union[str, List[str]],
    split=None,
    from_disk=False,
    repartition=20,
    verbose=True,
    *args,
    **kwargs
):
    """
    convert huggingface dataset to raw format as dict

    Args:
        spark (SparkSession): spark session
        name_or_path (str or list): the name or path of the huggingface dataset
        split (str): the split of the dataset
        from_disk (bool): whether to load from disk
            - no split is allowed when from_disk is True
        repartition (int): the number of partitions
        verbose (bool): whether to print the information of the dataset
    """
    dataset = load_huggingface_dataset(name_or_path, split=split, from_disk=from_disk)
    parquet_path = huggingface2parquet(dataset, verbose=verbose)
    df = spark.read.parquet(parquet_path)
    rdd = df.rdd.repartition(repartition)
    rdd = rdd.map(lambda row: row.asDict())

    return rdd
