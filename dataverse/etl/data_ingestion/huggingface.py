"""
Load Huggingface data

This is used just to load huggingface dataset without any refomatting

Copyright (c) 2024-present Upstage Co., Ltd.
Apache-2.0 license
"""
from typing import List, Union

from pyspark.rdd import RDD

from dataverse.etl import register_etl
from dataverse.utils.format import huggingface2parquet, load_huggingface_dataset


@register_etl
def data_ingestion___huggingface___hf2raw(
    spark,
    name_or_path: Union[str, List[str]],
    split: int = None,
    from_disk: bool = False,
    repartition: int = 20,
    verbose: bool = True,
    *args,
    **kwargs
) -> RDD:
    """
    Convert a HuggingFace dataset to raw format as a dictionary.

    Args:
        spark (SparkSession): The Spark session.
        name_or_path (Union[str, List[str]]): The name or path of the HuggingFace dataset.
        split(int, optional): The split of the dataset. Defaults to None.
        from_disk(bool, optional): Whether to load from disk. Defaults to False.
            No split is allowed when from_disk is True.
        repartition(int, optional): The number of partitions. Defaults to 20.
        verbose(bool, optional): Whether to print the information of the dataset. Defaults to True.

    Returns:
        rdd: The converted dataset as an RDD of dictionaries.
    """
    dataset = load_huggingface_dataset(name_or_path, split=split, from_disk=from_disk)
    parquet_path = huggingface2parquet(dataset, verbose=verbose)
    df = spark.read.parquet(parquet_path)
    rdd = df.rdd.repartition(repartition)
    rdd = rdd.map(lambda row: row.asDict())

    return rdd
