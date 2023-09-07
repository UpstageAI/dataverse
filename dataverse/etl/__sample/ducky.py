

from pyspark.rdd import RDD
from dataverse.etl.registry import BaseETL
from dataverse.etl.registry import register_etl
from dataverse.etl.registry import ETLRegistry


@register_etl
def __sample___ducky___make_your_own_etl_processor(rdd: RDD, config: dict = None, *args, **kwargs):
    """
    decorator will convert this function to BaseETL class
    """
    print("make_your_own_etl_processor")
    return rdd