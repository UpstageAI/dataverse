"""
NIN (National Identification Number)
=====================================
A national identification number, national identity number, or
national insurance number or JMBG/EMBG is used by the governments
of many countries as a means of tracking their citizens, permanent residents,
and temporary residents for the purposes of work, taxation,
government benefits, health care, and other governmentally-related functions.

https://en.wikipedia.org/wiki/National_identification_number

Copyright (c) 2024-present Upstage Co., Ltd.
Apache-2.0 license
"""

import random
import re
from typing import Union

from pyspark.rdd import RDD
from pyspark.sql import DataFrame

from dataverse.etl.registry import register_etl


@register_etl
def pii___nin___replace_korean_rrn(
    spark,
    data: Union[RDD, DataFrame],
    subset: str = "text",
    pattern: str = r"\d{6}-\d{7}",
    random_pii: bool = True,
    replace_pii: bool = False,
    replace_token: str = "[NIN]",
    start_token: str = "",
    end_token: str = "",
    *args,
    **kwargs,
) -> RDD:
    r"""
    Replace Korean RRN (Resident Registration Number) with a random number or a token

    Args:
        spark (SparkSession): The Spark session object.
        data(Union[RDD, DataFrame]): The input data to be processed.
        subset(str, optional): A subset or column to consider. Defaults to 'text'.
        pattern(str, optional): The regex pattern to find. Defaults to r'\d{6}-\d{7}'.
        random_pii(str, optional): If True, replace the pii with a random number. Defaults to True.
        replace_pii(bool, optional): If True, replace the pii with the `replace_token`. Defaults to False.
        replace_token(bool, optional): The token to replace the pii with. Defaults to '[NIN]'.
        start_token(str, optional): The start token to append where the pattern is found. Defaults to ''.
        end_token(str, optional): The end token to append where the pattern is found. Defaults to ''.

    Returns:
        rdd: The processed data with replaced Korean RRN.

    Caveats:
        - `replace_pii` takes precedence over `random_pii`
        - `start_token` and `end_token` are used to append the token to the start and end of the number
            - it doens't matter with `random_pii` or `replace_pii` is True or False


    Examples:
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
    """
    if isinstance(data, DataFrame):
        data = data.rdd
        data = data.map(lambda row: row.asDict())

    def _replace_match(match):
        match = match.group()
        if replace_pii:
            match = replace_token
        elif random_pii:
            match = re.sub(r"\d", lambda x: str(random.randint(0, 9)), match)

        return f"{start_token}{match}{end_token}"

    def _replace_pii(row):
        row[subset] = re.sub(pattern, _replace_match, row[subset])
        return row

    data = data.map(_replace_pii)

    return data
