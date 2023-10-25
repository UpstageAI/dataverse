
"""
Code is from ChenghaoMou/text-dedup
https://github.com/ChenghaoMou/text-dedup/blob/main/text_dedup/minhash_spark.py

This is a migration of the code to Dataverse.
"""

from pyspark.rdd import RDD
from pyspark.sql import DataFrame

from dataverse.etl.registry import register_etl

from typing import Union




