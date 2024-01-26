import re

from omegaconf import OmegaConf

from dataverse.etl import ETLPipeline
from dataverse.etl.registry import register_etl

sample_nin = "240101-0111111"


@register_etl
def helper___test___create_data_for_korean_rnn(spark, *args, **kwargs):
    return spark.createDataFrame([{"text": f"nin is {sample_nin}"}])


def test_pii___nin___replace_korean_rnns():
    from etl.pii.nin import pii___nin___replace_korean_rrn  # noqa: F401

    etl_pipeline = ETLPipeline()

    # Case 1: Random PII
    ETL_config = OmegaConf.create(
        {
            "etl": [
                {"name": "helper___test___create_data_for_korean_rnn"},
                {
                    "name": "pii___nin___replace_korean_rrn",
                    "args": {"random_pii": True, "replace_pii": False},
                },
            ]
        }
    )
    _, result = etl_pipeline.run(ETL_config)
    assert re.search(r"\d{6}-\d{7}", result.collect()[0]["text"]) != sample_nin

    # Case 2: Replace PII with Token
    ETL_config = OmegaConf.create(
        {
            "etl": [
                {"name": "helper___test___create_data_for_korean_rnn"},
                {
                    "name": "pii___nin___replace_korean_rrn",
                    "args": {"replace_pii": True, "replace_token": "[REDACTED]"},
                },
            ]
        }
    )
    _, result = etl_pipeline.run(ETL_config)
    assert result.collect()[0]["text"] == "nin is [REDACTED]"

    # Case 3: Add start and end tokens
    ETL_config = OmegaConf.create(
        {
            "etl": [
                {"name": "helper___test___create_data_for_korean_rnn"},
                {
                    "name": "pii___nin___replace_korean_rrn",
                    "args": {
                        "replace_pii": True,
                        "replace_token": "[REDACTED]",
                        "start_token": "[START]",
                        "end_token": "[END]",
                    },
                },
            ]
        }
    )
    _, result = etl_pipeline.run(ETL_config)
    assert result.collect()[0]["text"] == "nin is [START][REDACTED][END]"
