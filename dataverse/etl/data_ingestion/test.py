"""
special purpose to create fake data for testing or debugging

Copyright (c) 2024-present Upstage Co., Ltd.
Apache-2.0 license
"""

import json

from faker import Faker
from pyspark.rdd import RDD

from dataverse.etl import register_etl


@register_etl
def data_ingestion___test___generate_fake_ufl(
    spark, n: int = 100, repartition: int = 20, verbose: bool = True, *args, **kwargs
) -> RDD:
    """
    Generate fake data for testing or debugging.

    Args:
        spark (SparkSession): The Spark session object.
        n (int, optional): The number of data to generate. Default is 100.
        repartition (int, optional): The number of partitions. Default is 20.
        verbose (bool, optional): Whether to print the information of the dataset. Default is True.

    Returns:
        RDD: The generated fake data RDD.
    """
    faker = Faker()

    def _generate_fake_ufl(n=100):
        while n > 0:
            n -= 1
            yield {
                "id": faker.uuid4(),
                "name": "test_fake_ufl",
                "text": faker.text(),
                "meta": json.dumps(
                    {
                        "name": faker.name(),
                        "age": faker.random_int(0, 100),
                        "address": faker.address(),
                        "job": faker.job(),
                    }
                ),
            }

    rdd = spark.sparkContext.parallelize(_generate_fake_ufl(n=n))
    rdd = rdd.repartition(repartition)

    return rdd
