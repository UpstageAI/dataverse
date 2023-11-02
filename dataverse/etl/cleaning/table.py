

from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from typing import Union

from dataverse.etl.registry import register_etl



@register_etl
def cleaning___table___merge_col_vertical(
    spark,
    data: Union[RDD, DataFrame],
    col1=None,
    col2=None,
    merge_col_name="merge_col",
    *args,
    **kwargs
):
    """
    merge 2 column vertically to 1 column
    what does this mean? here is example

    [ before ]
    -----------------------
    col1 col2 species
    1    2    duck
    3    4    duck
    5    6    ducky
    -----------------------

    cleaning___table___merge_col_vertical(
        ...,
        col1="col1",
        col2="col2",
        merge_col_name="number",
        ...
    )

    [ after ]
    -----------------------
    number species
    1      duck
    2      duck
    3      duck
    4      duck
    5      ducky
    6      ducky
    -----------------------

    args:
        col1: column to merge
        col2: column to merge
        merge_col_name: name of merged column
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
