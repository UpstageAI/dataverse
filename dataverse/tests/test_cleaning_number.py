from omegaconf import OmegaConf

from dataverse.etl import ETLPipeline
from dataverse.etl.registry import register_etl


@register_etl
def helper___test___generate_number(spark, *args, **kwargs):
    data = [
        ("1234 apples and 5678 oranges ",),
        ("9876.54321 dollars",),
        ("This is random 1-3462-01.xx 87",),
        ("**6*342* history 0.6242 00002",),
        ("#eff000, af2f33, random color codes 1110 (013-0802-1143)",),
        ("88888888888888888888-888",),
    ]
    df = spark.createDataFrame(data, ["text"])
    return df


def test_cleaning___number___normalize():
    from etl.cleaning.number import cleaning___number___normalize  # noqa: F401

    etl_pipeline = ETLPipeline()
    ETL_config = OmegaConf.create(
        {
            "etl": [
                {"name": "helper___test___generate_number"},
                {"name": "cleaning___number___normalize", "args": {"assign_number": 8}},
            ]
        }
    )
    spark, result = etl_pipeline.run(ETL_config)
    result_df = result.toDF()
    expected_data = [
        ("8888 apples and 8888 oranges ",),
        ("8888.88888 dollars",),
        ("This is random 8-8888-88.xx 88",),
        ("**8*888* history 8.8888 88888",),
        ("#eff888, af8f88, random color codes 8888 (888-8888-8888)",),
        ("88888888888888888888-888",),
    ]
    expected_df = spark.createDataFrame(expected_data, ["text"])
    assert expected_df.collect() == result_df.collect()
