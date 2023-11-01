

from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.sql.functions import split, posexplode
from pyspark.sql.functions import collect_list

from dataverse.etl.registry import register_etl

import re
from typing import Union


def filter_lines(row):
    row = row.asDict()
    text = row['text']
    line_ids = row['line_ids']

    text_lines = text.split('\n')
    filtered_texts = "\n".join([text_lines[line_i] for line_i in sorted(line_ids)])

    del row['line_ids']
    row['text'] = filtered_texts

    return row


@register_etl
def deduplication___common_crawl___exact_line(spark, data: Union[RDD, DataFrame], subset='text', *args, **kwargs):
    """
    exact line deduplication - which is a line by line deduplication.

    args:
        subset (str): subset or columns to consider if duplicated
    """
    if isinstance(data, RDD):
        data = data.toDF()

    # if we don't cache, the id will be different when creating the line_df
    data = data.cache()

    assert isinstance(data, DataFrame), f"data must be DataFrame, got {type(data)}"
    line_data = data.select('id', posexplode(split(data[subset], '\n')).alias('line_id', 'line'))
    line_data = line_data.dropDuplicates(subset=['line'])
    line_data = line_data.groupBy('id').agg(collect_list('line_id').alias('line_ids'))

    # join the line_ids to the original data
    # we are not going to use the normalized line text, but the original text
    data = data.join(line_data, on=['id'], how='inner')
    line_data.unpersist()

    # filter the lines using the line_ids
    data = data.rdd.map(filter_lines)

    return data