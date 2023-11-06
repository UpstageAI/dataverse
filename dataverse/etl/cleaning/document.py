

from pyspark.rdd import RDD
from pyspark.sql import DataFrame

from dataverse.etl.registry import register_etl

import re
import unicodedata
from typing import Union


@register_etl
def cleaning___document___split_by_word(
    spark,
    data: Union[RDD, DataFrame],
    subset='text',
    word_per_chunk=100,
    delimiter=" ",
    *args,
    **kwargs
):
    """
    split documents to smaller chunks by word

    [ example ]
    - word_per_chunk = 2
    - delimiter = " "
    - text = "hello world, how are you?"
    - result = ["hello world,", "how are", "you?"]

    [ caveat ]
    - NO normalization is done here!
        - this doesn't consider the whitespace normalization
        - recommend to use other normalization before this
    - All the keys from the original row are copied to all the new rows created
        - `id` is not unique anymore
        - make sure `id` is assigned after this step

    args:
        subset (str): subset or columns to consider
        word_per_chunk (int): number of words per chunk
        delimiter (str): delimiter to split the text
            - default: " "
            - decide the unit of word
    """
    if isinstance(data, DataFrame):
        data = data.rdd
        data = data.map(lambda row: row.asDict())

    def _split_by_word(row):
        words = row[subset].split(delimiter)

        # Create chunks
        chunks = []
        for i in range(0, len(words), word_per_chunk):
            chunks.append(delimiter.join(words[i: i+word_per_chunk]))

        # Create a new dictionary for each chunk with all the keys from the original row
        return [{**row, subset: chunk} for chunk in chunks]

    data = data.flatMap(_split_by_word)

    return data
