import re

from omegaconf import OmegaConf

from dataverse.etl import ETLPipeline
from dataverse.etl.registry import register_etl

sample_card = "1234-1234-1234-1234"


@register_etl
def helper___test___create_data_for_pii_card(spark, *args, **kwargs):
    return spark.createDataFrame([{"text": f"Your card No. is {sample_card}"}])


def test_pii___card___replace():
    from etl.pii.card import pii___card___replace_card_number  # noqa: F401

    etl_pipeline = ETLPipeline()

    # Case 1: replace with random number
    ETL_config = OmegaConf.create(
        {
            "etl": [
                {"name": "helper___test___create_data_for_pii_card"},
                {
                    "name": "pii___card___replace_card_number",
                    "args": {"random_pii": True},
                },
            ]
        }
    )
    _, result = etl_pipeline.run(ETL_config)
    assert re.match(r"(\d{4}-\d{4}-\d{4}-\d{4})", result.collect()[0]["text"]) != sample_card

    # Case 2: replace with replace token
    ETL_config = OmegaConf.create(
        {
            "etl": [
                {"name": "helper___test___create_data_for_pii_card"},
                {
                    "name": "pii___card___replace_card_number",
                    "args": {"replace_pii": True, "replace_token": "[CARD_NUMBER]"},
                },
            ]
        }
    )
    _, result = etl_pipeline.run(ETL_config)
    expected = "Your card No. is [CARD_NUMBER]"

    assert result.collect()[0]["text"] == expected

    # Case 3: replace with replace token and add start, end token
    ETL_config = OmegaConf.create(
        {
            "etl": [
                {"name": "helper___test___create_data_for_pii_card"},
                {
                    "name": "pii___card___replace_card_number",
                    "args": {
                        "replace_pii": True,
                        "replace_token": "[CARD_NUMBER]",
                        "start_token": "[CARD_START]",
                        "end_token": "[CARD_END]",
                    },
                },
            ]
        }
    )
    _, result = etl_pipeline.run(ETL_config)
    expected = "Your card No. is [CARD_START][CARD_NUMBER][CARD_END]"
    assert result.collect()[0]["text"] == expected

    # Case 4: can't detect with different pattern
    ETL_config = OmegaConf.create(
        {
            "etl": [
                {"name": "helper___test___create_data_for_pii_card"},
                {
                    "name": "pii___card___replace_card_number",
                    "args": {
                        "pattern": r"(\d{5}-\d{5}-\d{5}-\d{5})",
                        "replace_pii": True,
                        "replace_token": "[CARD_NUMBER]",
                        "start_token": "[CARD_START]",
                        "end_token": "[CARD_END]",
                    },
                },
            ]
        }
    )
    _, result = etl_pipeline.run(ETL_config)
    expected = f"Your card No. is {sample_card}"
    assert result.collect()[0]["text"] == expected
