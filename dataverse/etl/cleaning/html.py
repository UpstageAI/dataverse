"""
A collection of modules for cleaning data includes html.

Copyright (c) 2024-present Upstage Co., Ltd.
Apache-2.0 license
"""

from typing import Union

import html2text
from pyspark.rdd import RDD
from pyspark.sql import DataFrame

from dataverse.etl.registry import register_etl


@register_etl
def cleaning___html___extract_plain_text(
    spark,
    data: Union[RDD, DataFrame],
    subset: str = "text",
    use_trafilatura: bool = False,
    *args,
    **kwargs
) -> RDD:
    r"""
    Extracts plain text from HTML.

    Args:
        spark (SparkSession): The Spark session object.
        data (Union[RDD, DataFrame]): The input data to be processed.
        subset (str, optional): A subset or column to consider. Defaults to 'text'.
        use_trafilatura (bool, optional): Whether to use trafilatura instead of html2text. Defaults to False.

    Returns:
        The plain data extracted from html.

    Caveats:
        - ``html2text`` adds a double newline after each paragraph, which is not handled at this point.
        - The option to use `trafilatura` is provided because extracting plain text with ``trafilatura`` does not seem to work well in some cases.

            - [OK] Case::

                text = "<body><h1>My First Heading</h1><p>My first paragraph.</p></body>"

                # html2text
                print(html2text.html2text(text))
                >>> '# My First Heading\n\nMy first paragraph.\n\n'

                # trafilatura
                print(trafilatura.html2txt(text))
                >>> 'My First HeadingMy first paragraph.'

            - [ERROR] Case (trafilatura removes all the text)::

                text = "<p>hello <br> nice to meet you.</p>"

                # html2text
                print(html2text.html2text(text))
                >>> 'hello  \nnice to meet you.\n\n'

                # trafilatura
                print(trafilatura.html2txt(text))
                >>> ''
    """
    if isinstance(data, DataFrame):
        data = data.rdd
        data = data.map(lambda row: row.asDict())

    # this is optional
    if use_trafilatura:
        import trafilatura

        def _html2txt(row):
            row[subset] = trafilatura.html2txt(row[subset])
            return row

    else:

        def _html2txt(row):
            row[subset] = html2text.html2text(row[subset])
            return row

    data = data.map(_html2txt)

    return data
