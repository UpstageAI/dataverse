

from pyspark.rdd import RDD
from pyspark.sql import DataFrame

from dataverse.etl import register_etl

from typing import Union


@register_etl
def __sample___ducky___make_your_own_etl_processor(data: Union[RDD, DataFrame], *args, **kwargs):
    """
    decorator will convert this function to BaseETL class
    """
    print("make_your_own_etl_processor")
    return data