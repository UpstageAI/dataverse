from omegaconf import OmegaConf

from dataverse.etl import ETLPipeline, register_etl


@register_etl
def helper___test___generate_accent(spark, *args, **kwargs):
    data = [("café",), ("résumé",), ("piñata",)]
    df = spark.createDataFrame(data, ["text"])
    return df


def test_cleaning___accent____remove():
    from etl.cleaning.accent import cleaning___accent___remove  # noqa: F401

    etl_pipeline = ETLPipeline()
    ETL_config = OmegaConf.create(
        {
            "spark": {
                "appname": "TEST-cleaning-accent",
                "driver": {"memory": "4g"},
                "args": {
                    "verbose": True,
                },
            },
            "etl": [
                {"name": "helper___test___generate_accent"},
                {"name": "cleaning___accent___remove"},
            ],
        }
    )
    spark, result = etl_pipeline.run(ETL_config)
    result_df = result.toDF()
    expected_data = [("cafe",), ("resume",), ("pinata",)]
    expected_df = spark.createDataFrame(expected_data, ["text"])

    assert expected_df.collect() == result_df.collect()
