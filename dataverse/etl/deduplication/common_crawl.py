"""
Copyright (c) 2024-present Upstage Co., Ltd.
Apache-2.0 license
"""

import functools
from typing import Union

from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import collect_list, posexplode, split

from dataverse.etl.registry import register_etl


def filter_lines(row, subset="text"):
    row = row.asDict()
    text = row[subset]
    line_ids = row["line_ids"]

    text_lines = text.split("\n")
    filtered_texts = "\n".join([text_lines[line_i] for line_i in sorted(line_ids)])

    del row["line_ids"]
    row[subset] = filtered_texts

    return row


@register_etl
def deduplication___common_crawl___exact_line(
    spark, data: Union[RDD, DataFrame], subset="text", *args, **kwargs
) -> RDD:
    """
    Performs exact line by line deduplication on the given data.

    Strip and lower is applied to the line text before deduplication
    but this will not be applied to the original text.

    Examples:
        - input

            +--------+
            |    text|
            +========+
            |   DuckY|
            +--------+
            |   dUKCY|
            +--------+

        - output

            +--------+
            |    text|
            +========+
            |   DuckY|
            +--------+

    Args:
        spark (SparkSession): The Spark session object.
        data (Union[RDD, DataFrame]): The input data to be deduplicated..
        subset (str, optional): A subset or column to consider. Defaults to 'text'.

    Returns:
        rdd: The deduplicated data.

    Raises:
        AssertionError: If the input data is not a DataFrame.
    """
    if isinstance(data, RDD):
        data = data.toDF()

    data = data.cache()
    data = data.withColumn("__id__", F.monotonically_increasing_id())

    assert isinstance(data, DataFrame), f"data must be DataFrame, got {type(data)}"
    line_data = data.select(
        "__id__", posexplode(split(data[subset], "\n")).alias("line_id", "line")
    )
    line_data = line_data.withColumn("line", F.lower(F.trim(line_data["line"])))
    line_data = line_data.dropDuplicates(subset=["line"])
    line_data = line_data.groupBy("__id__").agg(collect_list("line_id").alias("line_ids"))

    merged_data = data.join(line_data, on=["__id__"], how="inner")
    data.unpersist()
    line_data.unpersist()

    # remove __id__
    merged_data = merged_data.drop("__id__")

    # filter the lines using the line_ids
    merged_data = merged_data.rdd.map(functools.partial(filter_lines, subset=subset))

    return merged_data
