

from pyspark.rdd import RDD
from pyspark.sql import DataFrame

from dataverse.etl.registry import register_etl

import re
import random
from typing import Union



@register_etl
def pii___card___replace_card_number(
    spark,
    data: Union[RDD, DataFrame],
    subset='text',
    pattern=r'(\d{4}-\d{4}-\d{4}-\d{4})',
    random_pii=True,
    replace_pii=False,
    replace_token='[CARD_NUMBER]',
    start_token='',
    end_token='',
    *args,
    **kwargs
):
    """
    Replace card number with a random number or a token

    `replace_pii` takes precedence over `random_pii`
    - e.g when both are True, the card number will be replaced with the token
    - e.g. this is 1234-1234-1234-1234 -> this is [CARD_NUMBER]

    `start_token` and `end_token` are used to append the token to the start and end of the card number
    - it doens't matter with `random_card_number` or `replace_card_number` is True or False

    [ example ]
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
            - text = 'card number is 1234-1234-1234-1234[CARD_NUMBER_END].'

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
