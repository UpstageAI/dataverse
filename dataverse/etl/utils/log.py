"""
Copyright (c) 2024-present Upstage Co., Ltd.
Apache-2.0 license
"""

from typing import Union

from pyspark.rdd import RDD
from pyspark.sql import DataFrame

from dataverse.etl import register_etl


@register_etl
def utils___log___count(
    spark, data: Union[RDD, DataFrame], prev_etl_name: str = None, *args, **kwargs
) -> Union[RDD, DataFrame]:
    """
    Simply count the number of rows in the data

    Args:
        spark (SparkSession): The Spark session object.
        data (Union[RDD, DataFrame]): The input data to extract the nouns from.
        prev_etl_name (str, optional): name of the previous ETL process. Defaults to None.

    Returns:
        Union[RDD, DataFrame]: The input data. Nothing is changed.
    """
    total_data = data.count()
    print("=" * 50)
    print(f"After [ {prev_etl_name} ] - Total data: {total_data}")
    print("=" * 50)

    return data
