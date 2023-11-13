

from pyspark.rdd import RDD
from pyspark.sql import DataFrame

from typing import Union, List, Tuple
from operator import add

from dataverse.etl import register_etl


@register_etl
def utils___statistics___korean_nouns(
    spark,
    data: Union[RDD, DataFrame],
    subset='text',
    *args,
    **kwargs
) -> RDD[List[Tuple[str, int]]]:
    """
    get the frequency of each noun in the given subset of the data

    [ example ]
    Input:
        [
            {'text': '오리는 꽥꽥 웁니다. 거위는'},
            {'text': '안녕 세상!'},
            {'text': '사람들은 꽥꽥 울지 않습니다. 오리가 웁니다'},
        ]
    Output:
        [('오리', 2), ('거위', 1), ('세상', 1), ('사람', 1)]

    caveat:
        - this works for `KOREAN` only
        - this is not returning the ufl rather the frequency of each noun

    Args:
        spark (SparkSession): SparkSession
        data (Union[RDD, DataFrame]): data to extract the nouns from
        subset (str): subset of the data to extract the nouns from
    """

    # konlpy & mecab
    try:
        from konlpy.tag import Mecab
    except ImportError:
        raise ImportError(
            'Please install konlpy & Mecab:\n'
            'pip install konlpy\n'
            'pip install mecab-python3\n'
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
        except Exception as e:
            # Log the exception for debugging purposes
            return []

    # Count the frequency of each noun
    data = data.flatMap(lambda x: _parse_korean_nouns(x[subset]))
    noun_counts = data.map(lambda noun: (noun, 1)).reduceByKey(add)

    return noun_counts