
"""
Load Arrow

support direct loading of arrow saved huggingface dataset to spark dataframe
"""

from dataverse.etl import register_etl

import os
import glob
import pyarrow as pa
import numpy as np
from typing import Union, List
from omegaconf import ListConfig



def find_arrow_paths(directory):
    """find *.arrow files recursively"""
    if isinstance(directory, str):
        return glob.glob(os.path.join(directory, '**/*.arrow'), recursive=True)
    elif isinstance(directory, list) or isinstance(directory, ListConfig):
        arrow_paths = []
        for d in directory:
            arrow_paths.extend(find_arrow_paths(d))
        return arrow_paths

    raise ValueError(f"directory must be str or list, got {type(directory)}")


def arrow_table_to_dict(arrow_path):
    """
    pandas version

    speed 10000 take - 100ms
    """
    in_memory_stream = pa.input_stream(arrow_path)
    opened_stream = pa.ipc.open_stream(in_memory_stream)
    pa_table = opened_stream.read_all()
    df = pa_table.to_pandas()

    return df.to_dict('records') 

def arrow_table_to_dict(arrow_path):
    """
    speed 10000 take - 70ms

    this is used because it's faster than the pyarrow -> pydict direct loading version

    TODO: there is plenty of room for improvement
    ...
    """
    in_memory_stream = pa.input_stream(arrow_path)
    opened_stream = pa.ipc.open_stream(in_memory_stream)
    table = opened_stream.read_all()

    # get schema for field names
    schema = table.schema

    rows = []
    # iterate over each row
    for row in range(table.num_rows):
        row_data = {schema.field(col).name: table.column(col)[row].as_py() for col in range(table.num_columns)}
        rows.append(row_data)

    return rows

def arrow_table_to_dict(arrow_path):
    """
    pyarrow generator version

    this is used because of the memory issue

    speed 10000 take - 90ms
    """
    in_memory_stream = pa.input_stream(arrow_path)
    opened_stream = pa.ipc.open_stream(in_memory_stream)
    table = opened_stream.read_all()

    # get schema for field names
    schema = table.schema

    # iterate over each row
    for row in range(table.num_rows):
        row_data = {schema.field(col).name: table.column(col)[row].as_py() for col in range(table.num_columns)}
        yield row_data


@register_etl
def data_ingestion___arrow___hf2raw(
    spark,
    path : Union[str, List[str]],
    sample_n=-1,
    repartition=20,
    seed=42,
    verbose=True,
    *args,
    **kwargs
):
    """
    direct loading of arrow saved huggingface dataset to raw format as dict

    [ usage ]
    >>> import datasets
    >>> dataset = datasets.load_dataset('ducky')
    >>> dataset.save_to_disk('your/path/to/ducky')
    >>> data_ingestion___arrow___hf2raw()(spark, 'your/path/to/ducky')

    Args:
        spark (SparkSession): spark session
        path (str or list): the path of the arrow folders
        sample_n (int): the number of arrow files to be sampled
            - if sample_n is -1, all arrow files will be loaded
        seed (int): the seed for sampling
        repartition (int): the number of partitions
        verbose (bool): whether to print the information of the dataset
    """
    arrow_paths = find_arrow_paths(path)

    # sample from the arrow files
    if sample_n > 0 and sample_n < len(arrow_paths):
        np.random.seed(seed)
        arrow_paths = np.random.choice(arrow_paths, size=sample_n, replace=False)

    rdd = spark.sparkContext.parallelize(arrow_paths)
    rdd = rdd.repartition(len(arrow_paths))
    rdd = rdd.flatMap(arrow_table_to_dict)
    rdd = rdd.repartition(repartition)

    return rdd 
