"""
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
def pii___card___replace_card_number(
    spark,
    data: Union[RDD, DataFrame],
    subset: str = "text",
    pattern: str = r"(\d{4}-\d{4}-\d{4}-\d{4})",
    random_pii: bool = True,
    replace_pii: bool = False,
    replace_token: str = "[CARD_NUMBER]",
    start_token: str = "",
    end_token: str = "",
    *args,
    **kwargs,
) -> RDD:
    r"""
    Replace card number with a random number or a token

    Args:
        spark: The SparkSession object.
        data (Union[RDD, DataFrame]): The input data to process.
        subset (str, optional): The subset or columns to consider. Defaults to 'text'.
        pattern (str, optional): The regex pattern to find. Defaults to r'(\d{4}-\d{4}-\d{4}-\d{4})'.
        random_pii (bool, optional): If True, replace the pii with a random number. Defaults to True.
        replace_pii (bool, optional): If True, replace the pii with the `replace_token`. Defaults to False.
        replace_token (str, optional): The token to replace the pii with. Defaults to '[CARD_NUMBER]'.
        start_token (str, optional): The start token to append where the pattern is found. Defaults to ''.
        end_token (str, optional): The end token to append where the pattern is found. Defaults to ''.

    Returns:
        RDD: The processed data.

    Caveats:
        - `replace_pii` takes precedence over `random_pii`
            - e.g when both are True, the card number will be replaced with the token
            - e.g. this is 1234-1234-1234-1234 -> this is [CARD_NUMBER]
        - `start_token` and `end_token` are used to append the token to the start and end of the card number
            - it doens't matter with `random_card_number` or `replace_card_number` is True or False

    Examples:
        <input>
            - text = 'card number is 1234-1234-1234-1234.'

        <output>
            - random pii
                - text = 'card number is 2238-1534-1294-1274.'
            - replace pii
                - replace_token = '[CARD_NUMBER]'
                - text = 'card number is [CARD_NUMBER].'
            - start token
                - start_token = '[CARD_NUMBER_START]'
                - text = 'card number is [CARD_NUMBER_START]1234-1234-1234-1234.'
            - end token
                - end_token = '[CARD_NUMBER_END]'
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
