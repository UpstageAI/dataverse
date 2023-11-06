

from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.sql.functions import split, posexplode
from pyspark.sql.functions import collect_list

from dataverse.etl.registry import register_etl

import re
import unicodedata
from typing import Union



def strip_accents(text: str) -> str:
    """Strips accents from a piece of text."""
    nfd = unicodedata.normalize("NFD", text)
    output = [c for c in nfd if unicodedata.category(c) != "Mn"]
    if len(output) == text:
        return text
    return "".join(output)

@register_etl
def cleaning___accent___remove(
    spark,
    data: Union[RDD, DataFrame],
    subset='text',
    *args,
    **kwargs
):
    """
    strip accents from a piece of text
    - example
        - "café" -> "cafe"
        - "résumé" -> "resume"

    Code is from facebookresearch/cc_net
    https://github.com/facebookresearch/cc_net/blob/main/cc_net/text_normalizer.py

    args:
        subset (str): subset or columns to consider
    """
    if isinstance(data, DataFrame):
        data = data.rdd
        data = data.map(lambda row: row.asDict())

    def _strip_accents(row):
        row[subset] = strip_accents(row[subset])
        return row

    data = data.map(_strip_accents)

    return data
