from omegaconf import OmegaConf
from pyspark.sql import Row

from dataverse.etl import ETLPipeline
from dataverse.etl.registry import register_etl


@register_etl
def helper___test___generate_exact_line(spark, *args, **kwagrs):
    data = [
        Row(text="DataversE\ndATAVERSE\nQuack\nQUaCk\nquack", line_ids=[0, 2]),
        Row(text="hello\nHELLO\nWorld\nWoRLD", line_ids=[0, 2]),
    ]
    df = spark.createDataFrame(data)
    return df


def test_deduplication___common_crawl___exact_line():
    from etl.deduplication.common_crawl import (  # noqa: F401
        deduplication___common_crawl___exact_line,
    )

    etl_pipeline = ETLPipeline()
    ETL_config = OmegaConf.create(
        {
            "etl": [
                {"name": "helper___test___generate_exact_line"},
                {"name": "deduplication___common_crawl___exact_line"},
            ]
        }
    )

    spark, result = etl_pipeline.run(ETL_config)
    result = spark.createDataFrame(result)

    expected = [{"text": "DataversE\nQuack"}, {"text": "hello\nWorld"}]
    expected = spark.createDataFrame(expected)

    assert set(result.collect()) == set(expected.collect())
