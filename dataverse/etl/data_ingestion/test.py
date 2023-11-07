
"""
special purpose to create fake data for testing or debugging
"""

from dataverse.etl import register_etl
from dataverse.utils.format import huggingface2parquet
from dataverse.utils.format import load_huggingface_dataset

import json
from faker import Faker
from typing import Union, List


@register_etl
def data_ingestion___test___generate_fake_ufl(
    spark,
    n=100,
    repartition=20,
    verbose=True,
    *args,
    **kwargs
):
    """
    generate fake data for testing or debugging

    Args:
        spark (SparkSession): spark session
        n (int): the number of data to generate
        repartition (int): the number of partitions
        verbose (bool): whether to print the information of the dataset
    """
    faker = Faker()
    def _generate_fake_ufl(n=100):
        while n > 0:
            n -= 1
            yield {
                "id": faker.uuid4(),
                "name": 'test_fake_ufl',
                "text": faker.text(),
                "meta": json.dumps({
                    "name": faker.name(),
                    "age": faker.random_int(0, 100),
                    "address": faker.address(),
                    "job": faker.job(),
                }),
            }
    rdd = spark.sparkContext.parallelize(_generate_fake_ufl(n=n))
    rdd = rdd.repartition(repartition)

    return rdd
