

from pyspark.rdd import RDD
from pyspark.sql import DataFrame

from dataverse.etl.registry import register_etl

import html2text

from typing import Union


@register_etl
def cleaning___html___extract_plain_text(
    spark,
    data: Union[RDD, DataFrame],
    subset='text',
    use_trafilatura=False,
    *args,
    **kwargs
):
    """
    extract plain text from html

    [ caveat ]
    - `html2text` addes double newline after each paragraph
        - we are not handling it at this point
    - why I've put `trafilatura` as an option?
        - because the extracting plain text with `trafilatura` does not seems to work well
            - [ OK ] case
                ```python
                text = "<body><h1>My First Heading</h1><p>My first paragraph.</p></body>"

                # html2text
                print(html2text.html2text(text))
                >>> '# My First Heading\n\nMy first paragraph.\n\n'

                # trafilatura
                print(trafilatura.html2txt(text))
                >>> 'My First HeadingMy first paragraph.'
                ```
            - [ ERROR ] case (trafilatura removes all the text)
                ```python
                text = "<p>hello <br> nice to meet you.</p>"

                # html2text
                print(html2text.html2text(text))
                >>> 'hello  \nnice to meet you.\n\n'

                # trafilatura
                print(trafilatura.html2txt(text))
                >>> ''
                ```

    args:
        subset (str): subset or columns to consider
        use_trafilatura (bool): use trafilatura instead of html2text
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
