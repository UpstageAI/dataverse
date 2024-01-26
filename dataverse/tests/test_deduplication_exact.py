from omegaconf import OmegaConf

from dataverse.etl import ETLPipeline
from dataverse.etl.registry import register_etl


@register_etl
def helper___test___generate_duplicated_data(spark, *args, **kwargs):
    data = [(1, "dataverse"), (2, "apple"), (3, "dataverse"), (4, "carrot")]
    columns = ["id", "text"]
    df = spark.createDataFrame(data, columns)
    return df


def test_deduplication___exact_column():
    from etl.deduplication.exact import deduplication___exact___column  # noqa: F401

    etl_pipeline = ETLPipeline()
    columns = ["id", "text"]

    # subset : text
    ETL_config_text = OmegaConf.create(
        {
            "etl": [
                {"name": "helper___test___generate_duplicated_data"},
                {
                    "name": "deduplication___exact___column",
                    "args": {"subset": ["text"]},
                },
            ]
        }
    )
    spark, result_text = etl_pipeline.run(ETL_config_text)
    expected_text = [(1, "dataverse"), (2, "apple"), (4, "carrot")]
    expected_text = spark.createDataFrame(expected_text, schema=columns)
    assert sorted(result_text.collect()) == sorted(expected_text.collect())

    # subset : text and id
    ETL_config_text_id = OmegaConf.create(
        {
            "etl": [
                {"name": "helper___test___generate_duplicated_data"},
                {
                    "name": "deduplication___exact___column",
                    "args": {"subset": ["text", "id"]},
                },
            ]
        }
    )
    spark, result_text_id = etl_pipeline.run(ETL_config_text_id)
    expected_text_id = [(1, "dataverse"), (2, "apple"), (3, "dataverse"), (4, "carrot")]
    expected_text_id = spark.createDataFrame(expected_text_id, schema=columns)
    assert set(result_text_id.collect()) == set(expected_text_id.collect())
