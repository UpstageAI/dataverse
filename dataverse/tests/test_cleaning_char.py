import random
import re
from typing import Union

from omegaconf import OmegaConf
from pyspark.rdd import RDD
from pyspark.sql import DataFrame

from dataverse.etl import ETLPipeline
from dataverse.etl.registry import register_etl

faker_seed = 42
unprintable_chars = list(map(chr, range(32))) + list(map(chr, range(127, 160)))


@register_etl
def helper___test___generate_whitespace(
    spark, data: Union[RDD, DataFrame], subset="text", *args, **kwargs
):
    if isinstance(data, DataFrame):
        data = data.rdd
        data = data.map(lambda row: row.asDict())

    def _generate_whitespace(row):
        row[subset] = row[subset].replace(" ", " " * random.randint(1, 5))
        row[subset] = " " * random.randint(0, 5) + row[subset] + " " * random.randint(0, 5)
        return row

    data = data.map(_generate_whitespace)

    return data


def test_cleaning___char___normalize_whitespace():
    from etl.cleaning.char import cleaning___char___normalize_whitespace  # noqa: F401
    from etl.data_ingestion.test import (  # noqa: F401
        data_ingestion___test___generate_fake_ufl,
    )

    etl_pipeline = ETLPipeline()
    ETL_config = OmegaConf.create(
        {
            "spark": {
                "appname": "Test-cleaning-char",
                "driver": {"memory": "16g"},
                "verbose": True,
            },
            "etl": [
                {"name": "data_ingestion___test___generate_fake_ufl"},
                {
                    "name": "helper___test___generate_whitespace",
                    "args": {"subset": "text"},
                },
                {"name": "cleaning___char___normalize_whitespace"},
            ],
        }
    )
    _, result = etl_pipeline.run(ETL_config)

    double_space_pattern = re.compile(r"[\s\r\n]{2,}")

    for row in result.collect():
        assert row["text"] == row["text"].strip()
        assert re.findall(double_space_pattern, row["text"]) == []


@register_etl
def helper___test___generate_unprintable(
    spark, data: Union[RDD, DataFrame], subset="text", *args, **kwargs
):
    if isinstance(data, DataFrame):
        data = data.rdd
        data = data.map(lambda row: row.asDict())

    def _insert_unprintable_chars(text):
        unprintable_chars = list(map(chr, range(32))) + list(map(chr, range(127, 160)))
        for _ in range(random.randint(1, 20)):
            position = random.randint(0, len(text) - 1)
            char = random.choice(unprintable_chars)
            text = text[:position] + char + text[position:]
        return text

    def _generate_unprintable(row):
        row[subset] = _insert_unprintable_chars(row[subset])
        return row

    data = data.map(_generate_unprintable)

    return data


def test_cleaning___char___remove_unprintable():
    from etl.cleaning.char import cleaning___char___remove_unprintable  # noqa: F401
    from etl.data_ingestion.test import (  # noqa: F401
        data_ingestion___test___generate_fake_ufl,
    )

    etl_pipeline = ETLPipeline()
    ETL_config = OmegaConf.create(
        {
            "spark": {
                "appname": "Test-cleaning-char",
                "driver": {"memory": "16g"},
                "verbose": True,
            },
            "etl": [
                {
                    "name": "data_ingestion___test___generate_fake_ufl",
                    "args": {"faker_seed": faker_seed},
                },
                {
                    "name": "helper___test___generate_unprintable",
                    "args": {"subset": "text"},
                },
            ],
        }
    )
    _, unprintable_check = etl_pipeline.run(ETL_config)
    for row in unprintable_check.collect():
        assert any(chars in row["text"] for chars in unprintable_chars)

    etl_pipeline = ETLPipeline()
    ETL_config = OmegaConf.create(
        {
            "spark": {
                "appname": "Test-cleaning-char",
                "driver": {"memory": "16g"},
                "verbose": True,
            },
            "etl": [
                {
                    "name": "data_ingestion___test___generate_fake_ufl",
                    "args": {"faker_seed": faker_seed},
                },
                {
                    "name": "helper___test___generate_unprintable",
                    "args": {"subset": "text"},
                },
                {
                    "name": "cleaning___char___remove_unprintable",
                    "args": {"subset": "text"},
                },
            ],
        }
    )
    _, result = etl_pipeline.run(ETL_config)

    for row in result.collect():
        assert any(chars not in row["text"] for chars in unprintable_chars)
