"""
Copyright (c) 2024-present Upstage Co., Ltd.
Apache-2.0 license
"""

from typing import Union

from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from dataverse.etl.registry import register_etl


@register_etl
def cleaning___table___merge_col_vertical(
    spark,
    data: Union[RDD, DataFrame],
    col1: str = None,
    col2: str = None,
    merge_col_name: str = "merge_col",
    *args,
    **kwargs
) -> RDD:
    """
    Merges two columns vertically into one column.

    Example:
        Before:

        +------+------+---------+
        | col1 | col2 | species |
        +======+======+=========+
        | 1    | 2    | duck    |
        +------+------+---------+
        | 3    | 4    | duck    |
        +------+------+---------+
        | 5    | 6    | ducky   |
        +------+------+---------+

        After calling ``cleaning_table_merge_col_vertical(...)``:

        +--------+---------+
        | number | species |
        +========+=========+
        | 1      | duck    |
        +--------+---------+
        | 3      | duck    |
        +--------+---------+
        | 5      | ducky   |
        +--------+---------+
        | 2      | duck    |
        +--------+---------+
        | 4      | duck    |
        +--------+---------+
        | 6      | ducky   |
        +--------+---------+

    Args:
        spark (SparkSession): The Spark session object.
        data (Union[RDD, DataFrame]): The input data to be processed. It can be either an RDD or a DataFrame.
        col1 (str): The name of the first column to merge.
        col2 (str): The name of the second column to merge.
        merge_col_name (str, optional): The name of the merged column.

    Returns:
        The processed data with the merged column.

    Raises:
        ValueError: If col1 or col2 is not specified.
    """
    if isinstance(data, RDD):
        data = data.toDF()

    assert col1 is not None, "col1 must be specified"
    assert col2 is not None, "col2 must be specified"

    rest_cols = [c for c in data.columns if c not in [col1, col2]]
    df1 = data.select(*rest_cols, F.col(col1).alias(merge_col_name))
    df2 = data.select(*rest_cols, F.col(col2).alias(merge_col_name))

    # union the dataframes
    data = df1.union(df2)

    return data
