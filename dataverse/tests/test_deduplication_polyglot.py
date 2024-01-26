from omegaconf import OmegaConf

from dataverse.etl import ETLPipeline
from dataverse.etl.registry import register_etl


@register_etl
def helper___test___create_data_for_polyglot_minhash(spark, *args, **kwargs):
    data = [
        {"text": "hello wolrd! Welcome to dataverse."},
        {"text": "hello wolrd! Welcome to dataverrrrse."},
        {"text": "a totally different sentence"},
    ]
    df = spark.createDataFrame(data)
    return df


def test_deduplication___polyglot___minhash():
    from etl.deduplication.polyglot import (  # noqa: F401
        deduplication___polyglot___minhash,
    )

    etl_pipeline = ETLPipeline()
    ETL_config = OmegaConf.create(
        {
            "etl": [
                {"name": "helper___test___create_data_for_polyglot_minhash"},
                {
                    "name": "deduplication___polyglot___minhash",
                    "args": {
                        "expand_size": 64,
                        "n_gram": 2,
                        "seed": 1,
                        "char_level": False,
                        "sim_threshold": 0.2,
                    },
                },
            ]
        }
    )
    _, result = etl_pipeline.run(ETL_config)

    assert result.count() == 2

    texts = set(map(lambda x: x["text"], result.collect()))
    assert ("hello wolrd! Welcome to dataverse." in texts) or (
        "hello wolrd! Welcome to dataverrrrse." in texts
    )
    assert "a totally different sentence" in texts
