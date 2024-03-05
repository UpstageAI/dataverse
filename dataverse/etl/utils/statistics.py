"""
Copyright (c) 2024-present Upstage Co., Ltd.
Apache-2.0 license
"""

from operator import add
from typing import Union

from pyspark.rdd import RDD
from pyspark.sql import DataFrame

from dataverse.etl import register_etl


@register_etl
def utils___statistics___korean_nouns(
    spark, data: Union[RDD, DataFrame], subset: str = "text", *args, **kwargs
) -> RDD:
    """
    Get the frequency of each noun in the given subset of the data.

    Args:
        spark: The SparkSession object.
        data: The data to extract the nouns from.
        subset: The subset of the data to extract the nouns from. Defaults to 'text'.

    Returns:
        RDD[List[Tuple[str, int]]]: The frequency of each noun in the given subset of the data.

    Raises:
        ImportError: If konlpy or Mecab is not installed.

    Examples:
        >>> data = [
        ...     {'text': '오리는 꽥꽥 웁니다. 거위는'},
        ...     {'text': '안녕 세상!'},
        ...     {'text': '사람들은 꽥꽥 울지 않습니다. 오리가 웁니다'},
        ... ]
        >>> result = utils___statistics___korean_nouns()(spark, data, subset='text')
        >>> result.collect()
        [('오리', 2), ('거위', 1), ('세상', 1), ('사람', 1)]

    Caveats:
        - This function works for Korean text only.
        - The function returns the frequency of each noun, not the unique noun list.
    """

    # konlpy & mecab
    try:
        from konlpy.tag import Mecab
    except ImportError:
        raise ImportError(
            "Please install konlpy & Mecab:\n" "pip install konlpy\n" "pip install mecab-python3\n"
        )

    if isinstance(data, DataFrame):
        data = data.rdd

    mecab = Mecab()

    def _parse_korean_nouns(text):
        try:
            if text is not None:
                return mecab.nouns(text)
            else:
                return []
        except Exception:
            # Log the exception for debugging purposes
            return []

    # Count the frequency of each noun
    data = data.flatMap(lambda x: _parse_korean_nouns(x[subset]))
    noun_counts = data.map(lambda noun: (noun, 1)).reduceByKey(add)

    return noun_counts
