

from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.sql.functions import split, posexplode
from pyspark.sql.functions import collect_list
from pyspark.sql import functions as F

from dataverse.etl.registry import register_etl

import re
import functools
from typing import Union


def filter_lines(row, subset='text'):
    row = row.asDict()
    text = row[subset]
    line_ids = row['line_ids']

    text_lines = text.split('\n')
    filtered_texts = "\n".join([text_lines[line_i] for line_i in sorted(line_ids)])

    del row['line_ids']
    row[subset] = filtered_texts

    return row


@register_etl
def deduplication___common_crawl___exact_line(spark, data: Union[RDD, DataFrame], subset='text', *args, **kwargs):
    """
    exact line deduplication - which is a line by line deduplication.

    strip & lower is applied to the line text before deduplication
    but this will not be applied to the original text

    - input
        - 'DuckY'
        - 'dUKCY '
    - after normalization
        - 'ducky'
        - 'ducky'
    - after deduplication
        - 'ducky'
    - output
        - 'DuckY'


    args:
        subset (str): subset or columns to consider if duplicated
    """
    if isinstance(data, RDD):
        data = data.toDF()

    data = data.cache()
    data = data.withColumn("__id__", F.monotonically_increasing_id())

    assert isinstance(data, DataFrame), f"data must be DataFrame, got {type(data)}"
    line_data = data.select('__id__', posexplode(split(data[subset], '\n')).alias('line_id', 'line'))
    line_data = line_data.withColumn('line', F.lower(F.trim(line_data['line'])))
    line_data = line_data.dropDuplicates(subset=['line'])
    line_data = line_data.groupBy('__id__').agg(collect_list('line_id').alias('line_ids'))

    merged_data = data.join(line_data, on=['__id__'], how='inner')
    data.unpersist()
    line_data.unpersist()

    # remove __id__
    merged_data = merged_data.drop('__id__')

    # filter the lines using the line_ids
    merged_data = merged_data.rdd.map(functools.partial(filter_lines, subset=subset))

    return merged_data