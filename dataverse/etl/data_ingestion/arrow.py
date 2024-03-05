"""
Load Arrow.
Support direct loading of arrow saved huggingface dataset to spark dataframe.

Copyright (c) 2024-present Upstage Co., Ltd.
Apache-2.0 license
"""

import glob
import os
from typing import List, Union

import numpy as np
import pyarrow as pa
from omegaconf import ListConfig
from pyspark.rdd import RDD

from dataverse.etl import register_etl


def find_arrow_paths(directory):
    """find *.arrow files recursively"""
    if isinstance(directory, str):
        return glob.glob(os.path.join(directory, "**/*.arrow"), recursive=True)
    elif isinstance(directory, list) or isinstance(directory, ListConfig):
        arrow_paths = []
        for d in directory:
            arrow_paths.extend(find_arrow_paths(d))
        return arrow_paths

    raise ValueError(f"directory must be str or list, got {type(directory)}")


def get_dir_size(arrow_paths):
    total_size = 0
    for fp in arrow_paths:
        # skip if it is not `.arrow` file
        if not fp.endswith(".arrow"):
            continue

        # skip if it is symbolic link
        if not os.path.islink(fp):
            total_size += os.path.getsize(fp)

    return total_size


def arrow_table_to_dict(arrow_path):
    """
    speed 10000 take - 70ms

    faster than
    - pyarrow -> pydict direct loading
    - pyarrow -> pandas -> pydict loading

    TODO: speed and memory improvement
    """
    in_memory_stream = pa.input_stream(arrow_path)
    opened_stream = pa.ipc.open_stream(in_memory_stream)
    table = opened_stream.read_all()

    # get schema for field names
    schema = table.schema

    rows = []
    # iterate over each row
    for row in range(table.num_rows):
        row_data = {
            schema.field(col).name: table.column(col)[row].as_py()
            for col in range(table.num_columns)
        }
        rows.append(row_data)

    return rows


@register_etl
def data_ingestion___arrow___hf2raw(
    spark,
    path: Union[str, List[str]],
    sample_n: int = -1,
    arrow_partition_mb_size: int = -1,
    raw_partition_mb_size: int = 256,
    repartition: int = -1,
    seed: int = 42,
    verbose: bool = True,
    *args,
    **kwargs,
) -> RDD:
    """
    Directly loads the arrow saved HuggingFace dataset to raw format as a dictionary.

    Args:
        spark (SparkSession): The Spark session object.
        path (Union[str, List[str]]): The path of the arrow folders.
        sample_n (int, optional): The number of arrow files to be sampled. Defaults to -1.
            If sample_n is -1, all arrow files will be loaded.
        arrow_partition_mb_size (int, optional): The size of each arrow partition in MB. Defaults to -1.
            If arrow_partition_size is -1, it will repartition arrow files by the number of arrow files.
            This assumes that arrow file size is evenly distributed. When there is data skew in arrow file size, it is recommended to use the default (-1).
        raw_partition_mb_size (int, optional): The size of each raw partition in MB. Defaults to 256.
            This is activated only when repartition is -1.
        repartition (int, optional): Manually choose the number of partitions. Defaults to -1.
        seed (int, optional): The seed for sampling. Defaults to 42.
        verbose (bool, optional): Whether to print the information of the dataset. Defaults to True.

    Returns:
        RDD: The RDD containing the raw data in dictionary format.

    Examples:
        >>> import datasets
        >>> dataset = datasets.load_dataset('ducky')
        >>> dataset.save_to_disk('your/path/to/ducky')
        >>> data_ingestion___arrow___hf2raw()(spark, 'your/path/to/ducky')

    Caveats:
        Arrow paths are repartitioned by the number of arrow files.
    """
    arrow_paths = find_arrow_paths(path)
    assert len(arrow_paths) > 0, f"no arrow files found in {path}"

    # sample from the arrow files
    if sample_n > 0 and sample_n < len(arrow_paths):
        np.random.seed(seed)
        arrow_paths = np.random.choice(arrow_paths, size=sample_n, replace=False)

    if arrow_partition_mb_size == -1:
        # if data is skewed, recommend to use default (-1)
        arrow_repartition = len(arrow_paths)
    else:
        # this assume that arrow file size is evenly distributed
        assert (
            arrow_partition_mb_size > 0
        ), f"arrow_partition_mb_size must be positive, got {arrow_partition_mb_size}"
        arrow_total_mb_size = get_dir_size(arrow_paths) / 1024 / 1024
        arrow_repartition = arrow_total_mb_size // arrow_partition_mb_size
        arrow_repartition += 1 if arrow_total_mb_size % arrow_partition_mb_size else 0
        arrow_repartition = min(int(arrow_repartition), len(arrow_paths))

    rdd = spark.sparkContext.parallelize(arrow_paths)
    rdd = rdd.repartition(arrow_repartition)
    rdd = rdd.flatMap(arrow_table_to_dict)

    if repartition != -1:
        raw_repartition = repartition
    else:
        assert (
            raw_partition_mb_size > 0
        ), f"raw_partition_mb_size must be positive, got {raw_partition_mb_size}"

        arrow_total_mb_size = get_dir_size(arrow_paths) / 1024 / 1024
        raw_repartition = arrow_total_mb_size // raw_partition_mb_size
        raw_repartition += 1 if arrow_total_mb_size % raw_partition_mb_size else 0

        # count the number of data points (this is expensive)
        # this is to prevent the case where the number of data points is less than raw_repartition
        total_data_n = rdd.count()
        raw_repartition = min(int(raw_repartition), total_data_n)

    rdd = rdd.repartition(raw_repartition)

    return rdd
