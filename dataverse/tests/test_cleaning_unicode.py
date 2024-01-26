from omegaconf import OmegaConf

from dataverse.etl import ETLPipeline
from dataverse.etl.registry import register_etl


@register_etl
def helper___test___generate_unicode_data(spark, *args, **kwargs):
    data = [
        ("，。、„”“«»１」「《》´∶：？！（）；–—．～’…━〈〉【】％►",),
        ("Hello， world！！0 1 2 ñ",),
        ("This is fun。ction for —dataverse━",),
        ("You can use 《dataverse》” for your ETL cycle？",),
        ("Test sentence.",),
        ("～～～～",),
    ]
    df = spark.createDataFrame(data, ["text"])
    return df


def helper___test___generate_expected_unicode_data(spark, type="remove"):
    assert type in ["remove", "replace", "normalize"]

    if type == "remove":
        expected_data = [
            ("",),
            ("Hello world0 1 2 ñ",),
            ("This is function for dataverse",),
            ("You can use dataverse for your ETL cycle",),
            ("Test sentence.",),
            ("",),
        ]
    elif type == "replace":
        expected_data = [
            (''',.,""""""""""'::?!();- - . ~'...-<>[]%-''',),
            ("Hello, world!!0 1 2 ñ",),
            ("This is fun.ction for  - dataverse-",),
            ('You can use "dataverse"" for your ETL cycle?',),
            ("Test sentence.",),
            ("~~~~",),
        ]

    else:  # type == "normalize"
        expected_data = [
            ("，。、„”“«»１」「《》´∶：？！（）；–—．～’…━〈〉【】％►",),
            ("Hello， world！！0 1 2 ñ",),
            ("This is fun。ction for —dataverse━",),
            ("You can use 《dataverse》” for your ETL cycle？",),
            ("Test sentence.",),
            ("～～～～",),
        ]

    return spark.createDataFrame(expected_data, ["text"])


def test_cleaning___unicode___remove_punct():
    from etl.cleaning.unicode import cleaning___unicode___remove_punct  # noqa: F401

    etl_pipeline = ETLPipeline()
    ETL_config = OmegaConf.create(
        {
            "etl": [
                {"name": "helper___test___generate_unicode_data"},
                {"name": "cleaning___unicode___remove_punct"},
            ]
        }
    )
    spark, result = etl_pipeline.run(ETL_config)
    expected = helper___test___generate_expected_unicode_data(spark, type="remove")

    assert all(
        result_row["text"] == expected_row["text"]
        for (result_row, expected_row) in zip(result.collect(), expected.collect())
    )


def test_cleaning___unicode___replace_punct():
    from etl.cleaning.unicode import cleaning___unicode___replace_punct  # noqa: F401

    etl_pipeline = ETLPipeline()
    ETL_config = OmegaConf.create(
        {
            "etl": [
                {"name": "helper___test___generate_unicode_data"},
                {"name": "cleaning___unicode___replace_punct"},
            ]
        }
    )
    spark, result = etl_pipeline.run(ETL_config)
    expected = helper___test___generate_expected_unicode_data(spark, type="replace")

    assert all(
        result_row["text"] == expected_row["text"]
        for (result_row, expected_row) in zip(result.collect(), expected.collect())
    )


def test_cleaning___unicode___normalize():
    from etl.cleaning.unicode import cleaning___unicode___normalize  # noqa: F401

    etl_pipeline = ETLPipeline()
    ETL_config = OmegaConf.create(
        {
            "etl": [
                {"name": "helper___test___generate_unicode_data"},
                {"name": "cleaning___unicode___normalize"},
            ]
        }
    )
    spark, result = etl_pipeline.run(ETL_config)
    expected = helper___test___generate_expected_unicode_data(spark, type="normalize")
    assert all(
        result_row["text"] == expected_row["text"]
        for (result_row, expected_row) in zip(result.collect(), expected.collect())
    )
