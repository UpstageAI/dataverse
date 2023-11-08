
"""
NIN (National Identification Number)
=====================================
A national identification number, national identity number, or
national insurance number or JMBG/EMBG is used by the governments
of many countries as a means of tracking their citizens, permanent residents,
and temporary residents for the purposes of work, taxation,
government benefits, health care, and other governmentally-related functions.

https://en.wikipedia.org/wiki/National_identification_number
"""

from pyspark.rdd import RDD
from pyspark.sql import DataFrame

from dataverse.etl.registry import register_etl

import re
import random
from typing import Union



@register_etl
def pii___nin___replace_korean_rrn(
    spark,
    data: Union[RDD, DataFrame],
    subset='text',
    pattern=r'\d{6}-\d{7}',
    random_pii=True,
    replace_pii=False,
    replace_token='[NIN]',
    start_token='',
    end_token='',
    *args,
    **kwargs
):
    """
    Replace Korean RRN (Resident Registration Number) with a random number or a token

    `replace_pii` takes precedence over `random_pii`
    `start_token` and `end_token` are used to append the token to the start and end of the number
    - it doens't matter with `random_pii` or `replace_pii` is True or False

    [ example ]
    <input>
        - text = 'nin is 123456-1234567'

    <output>
        - random pii 
            - text = 'nin is 141124-1244121'
        - replace pii
            - replace_token = '[NIN]'
            - text = 'nin is [NIN].'
        - start token
            - start_token = '[NIN_START]'
            - text = 'nin is [NIN_START]123456-1234567'
        - end token
            - end_token = '[NIN_END]'
            - text = 'nin is 123456-1234567[NIN_END].'

    args:
        subset (str): subset or columns to consider
        pattern (str): regex pattern to find
        random_pii (bool): if True, replace the pii with a random number
        replace_pii (bool): if True, replace the pii with the `replace_token`
        replace_token (str): token to replace the pii with
        start_token (str): start token to append where the pattern is found
        end_token (str): end token to append where the pattern is found
    """
    if isinstance(data, DataFrame):
        data = data.rdd
        data = data.map(lambda row: row.asDict())

    def _replace_match(match):
        match = match.group()
        if replace_pii:
            match = replace_token
        elif random_pii:
            match = re.sub('\d', lambda x: str(random.randint(0, 9)), match)

        return f"{start_token}{match}{end_token}"

    def _replace_pii(row):
        row[subset] = re.sub(pattern, _replace_match, row[subset])
        return row

    data = data.map(_replace_pii)

    return data