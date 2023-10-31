

from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.sql.functions import split, posexplode
from pyspark.sql.functions import collect_list

from dataverse.etl.registry import register_etl

import re
from typing import Union



"""
Below Regex related CONSTANT & Code is from facebookresearch/cc_net
https://github.com/facebookresearch/cc_net/blob/main/cc_net/text_normalizer.py
"""
UNICODE_PUNCT = {
    "，": ",",
    "。": ".",
    "、": ",",
    "„": '"',
    "”": '"',
    "“": '"',
    "«": '"',
    "»": '"',
    "１": '"',
    "」": '"',
    "「": '"',
    "《": '"',
    "》": '"',
    "´": "'",
    "∶": ":",
    "：": ":",
    "？": "?",
    "！": "!",
    "（": "(",
    "）": ")",
    "；": ";",
    "–": "-",
    "—": " - ",
    "．": ". ",
    "～": "~",
    "’": "'",
    "…": "...",
    "━": "-",
    "〈": "<",
    "〉": ">",
    "【": "[",
    "】": "]",
    "％": "%",
    "►": "-",
}
UNICODE_PUNCT_RE = re.compile(f"[{''.join(UNICODE_PUNCT.keys())}]")
DIGIT_RE = re.compile(r"\d")
NON_PRINTING_CHARS_RE = re.compile(
    f"[{''.join(map(chr, list(range(0,32)) + list(range(127,160))))}]"
)
PUNCT_OR_NON_PRINTING_CHARS_RE = re.compile(
    (UNICODE_PUNCT_RE.pattern + NON_PRINTING_CHARS_RE.pattern).replace("][", "")
)

def normalize_for_dedup(line: str) -> str:
    """
    This is used to normalize text for deduplication.

    Code is from facebookresearch/cc_net
    https://github.com/facebookresearch/cc_net/blob/main/cc_net/text_normalizer.py
    """
    line = line.strip()
    if not line:
        return line

    # case
    line = line.lower()

    # numbers
    line = DIGIT_RE.sub("0", line)
    line = PUNCT_OR_NON_PRINTING_CHARS_RE.sub("", line)

    return line

def normalize_line_text(row):
    row = row.asDict()
    row['line'] = normalize_for_dedup(row['line'])
    return row

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
    text normalization is done before deduplication.

    normalized text is just used for deduplication,
    and the original text is used for the output.
    - example
        - input
            - text1: "Hello。"
            - text2: "Hello."
        - normalized text
            - text1: "hello."
            - text2: "hello."
        - after deduplication 
            - text: "Hello."
        - output (original text is used)
            - text: "Hello。"

    args:
        subset (str): subset or columns to consider if duplicated
    """
    if isinstance(data, RDD):
        data = data.toDF()

    # if we don't cache, the id will be different when creating the line_df
    data = data.cache()

    assert isinstance(data, DataFrame), f"data must be DataFrame, got {type(data)}"
    line_data = data.select('id', posexplode(split(data[subset], '\n')).alias('line_id', 'line'))
    line_data = line_data.rdd.map(normalize_line_text).toDF()
    line_data = line_data.dropDuplicates(subset=['line'])
    line_data = line_data.groupBy('id').agg(collect_list('line_id').alias('line_ids'))

    # join the line_ids to the original data
    # we are not going to use the normalized line text, but the original text
    data = data.join(line_data, on=['id'], how='inner')
    line_data.unpersist()

    # filter the lines using the line_ids
    data = data.rdd.map(filter_lines)

    return data