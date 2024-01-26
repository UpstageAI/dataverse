from omegaconf import OmegaConf

from dataverse.etl import ETLPipeline
from dataverse.etl.registry import register_etl


@register_etl
def helper___test___generate_table(spark, *args, **kwargs):
    data = [(1, 2, "duck"), (3, 4, "duck"), (5, 6, "ducky")]
    df = spark.createDataFrame(data, ["column1", "column2", "species"])
    return df


def test_cleaning___table___merge_col_vertical():
    from etl.cleaning.table import cleaning___table___merge_col_vertical  # noqa: F401

    etl_pipeline = ETLPipeline()
    ETL_config = OmegaConf.create(
        {
            "etl": [
                {"name": "helper___test___generate_table"},
                {
                    "name": "cleaning___table___merge_col_vertical",
                    "args": {
                        "col1": "column1",
                        "col2": "column2",
                        "merge_col_name": "number",
                    },
                },
            ]
        }
    )
    spark, result = etl_pipeline.run(ETL_config)
    expected_data = [
        ("duck", 1),
        ("duck", 3),
        ("ducky", 5),
        ("duck", 2),
        ("duck", 4),
        ("ducky", 6),
    ]
    expected_df = spark.createDataFrame(expected_data, ["species", "number"])
    assert result.collect() == expected_df.collect()
