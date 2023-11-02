
"""
"""

import os
from dataverse.etl import register_etl


@register_etl
def data_ingestion___parquet___pq2ufl(spark, input_paths, repartition=20, *args, **kwargs):
    df = spark.read.parquet(*input_paths)
    rdd = df.rdd.repartition(repartition)
    rdd = rdd.map(lambda row: row.asDict())
    return rdd