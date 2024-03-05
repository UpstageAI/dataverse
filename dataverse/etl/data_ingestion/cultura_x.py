"""
Copyright (c) 2024-present Upstage Co., Ltd.
Apache-2.0 license
"""

import json

from pyspark.rdd import RDD

from dataverse.etl import register_etl
from dataverse.utils.format import get_uuidv1


@register_etl
def data_ingestion___cultura_x___raw2ufl(spark, ufl: RDD, *args, **kwargs):
    """
    Converts raw format to UFL with custom template.

    Args:
        spark (SparkSession): The Spark session object.
        ufl(RDD): The input DataFrame in raw format.

    Returns:
        RDD: The transformed DataFrame in UFL format.
    """

    def templatev1(row):
        new_row = {}
        new_row["id"] = get_uuidv1()
        new_row["name"] = "cultura_x"
        new_row["text"] = row["text"]
        new_row["meta"] = json.dumps(
            {
                "url": row["url"],
                "timestamp": row["timestamp"],
                "source": row["source"],
            }
        )
        return new_row

    ufl = ufl.map(lambda x: templatev1(x))

    return ufl
