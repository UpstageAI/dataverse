
from pyspark.rdd import RDD
from pyspark.sql import DataFrame

from dataverse.etl.registry import BaseETL
from dataverse.etl.registry import register_etl
from dataverse.etl.registry import ETLRegistry

from typing import Union


@register_etl
def deduplication___subarray___boilerplate(data: Union[RDD, DataFrame], *args, **kwargs):
    """
    exact deduplication
    """
    NotImplementedError()
