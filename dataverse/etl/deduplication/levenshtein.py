
from pyspark.rdd import RDD
from pyspark.sql import DataFrame

from dataverse.etl.registry import BaseETL
from dataverse.etl.registry import register_etl
from dataverse.etl.registry import ETLRegistry

from typing import Union


@register_etl
def deduplication___levenshtein___boilerplate(spark, data: Union[RDD, DataFrame], *args, **kwargs):
    """
    fuzzy deduplication
    """
    NotImplementedError()
