

from pyspark.rdd import RDD
from pyspark.sql import DataFrame

from typing import Union, List, Tuple
from operator import add

from dataverse.etl import register_etl


@register_etl
def utils___log___count(
    spark,
    data: Union[RDD, DataFrame],
    prev_etl_name=None,
    *args,
    **kwargs
) -> RDD[List[Tuple[str, int]]]:
    """
    Simply count the number of rows in the data

    Args:
        spark (SparkSession): SparkSession
        data (Union[RDD, DataFrame]): data to extract the nouns from
    """
    total_data = data.count()
    print('=' * 50)
    print(f"After [ {prev_etl_name} ] - Total data: {total_data}")
    print('=' * 50)

    return data