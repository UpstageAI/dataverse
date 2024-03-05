"""
A collection of modules for cleaning data at the document level.

Copyright (c) 2024-present Upstage Co., Ltd.
Apache-2.0 license
"""

from typing import Union

from pyspark.rdd import RDD
from pyspark.sql import DataFrame

from dataverse.etl.registry import register_etl


@register_etl
def cleaning___document___split_by_word(
    spark,
    data: Union[RDD, DataFrame],
    subset: str = "text",
    word_per_chunk: int = 100,
    delimiter: str = " ",
    *args,
    **kwargs
) -> RDD:
    """
    Split documents into smaller chunks by word.

    Args:
        spark (SparkSession): The Spark session object.
        data (Union[RDD, DataFrame]): The input data to be processed.
        subset (str, optional): A subset or column to consider. Defaults to 'text'.
        word_per_chunk (int, optional): Number of words per chunk. Defaults to 100.
        delimiter (str, optional): Delimiter to split the text. Defaults to " ".

    Returns:
        RDD: The processed data with documents split into smaller chunks.

    Raises:
        ValueError: If word_per_chunk is not a positive integer.

    Examples:
        - word_per_chunk = 2
        - delimiter = " "
        - input

            +-----------------------------+
            |            text             |
            +=============================+
            | "hello world, how are you?" |
            +-----------------------------+

        - output

            +----------------+
            |      text      |
            +================+
            | "hello world," |
            +----------------+
            | "how are"      |
            +----------------+
            | "you?"         |
            +----------------+

    Caveats:
        - NO normalization is done here!
            - This doesn't consider the whitespace normalization.
            - Recommend using other normalization before this.
        - All the keys from the original row are copied to all the new rows created.
            - ``id`` is not unique anymore.
            - Make sure ``id`` is assigned after this step.
    """

    if isinstance(data, DataFrame):
        data = data.rdd
        data = data.map(lambda row: row.asDict())

    def _split_by_word(row):
        words = row[subset].split(delimiter)

        # Create chunks
        chunks = []
        for i in range(0, len(words), word_per_chunk):
            chunks.append(delimiter.join(words[i : i + word_per_chunk]))

        # Create a new dictionary for each chunk with all the keys from the original row
        return [{**row, subset: chunk} for chunk in chunks]

    data = data.flatMap(_split_by_word)

    return data
