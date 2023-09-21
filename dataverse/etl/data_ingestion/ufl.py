
"""
Load UFL (Upstage Format for LLM) data
"""

import os
from dataverse.etl import register_etl


@register_etl
def data_ingestion___ufl___parquet2ufl(spark, input_paths, repartition=20, *args, **kwargs):
    """
    load ufl saved as parquet
    """
    df = spark.read.parquet(*input_paths)
    rdd = df.rdd.repartition(repartition)
    rdd = rdd.map(lambda row: row.asDict())
    return rdd