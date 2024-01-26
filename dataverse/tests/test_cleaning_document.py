import pytest
from faker import Faker
from omegaconf import OmegaConf

from dataverse.etl import ETLPipeline

faker_seed = 42
word_per_chunk = 5
delimiter = " "


@pytest.fixture(scope="function", autouse=True)
def fake_data_rdd():
    Faker.seed(faker_seed)


def test_cleaning___document___split_by_word():
    from etl.cleaning.document import cleaning___document___split_by_word  # noqa: F401
    from etl.data_ingestion.test import (  # noqa: F401
        data_ingestion___test___generate_fake_ufl,
    )

    etl_pipeline = ETLPipeline()
    ETL_Config = OmegaConf.create(
        {
            "spark": {
                "appname": "TestCleaningDocument",
                "driver": {"memory": "16g"},
                "verbose": True,
            },
            "etl": [
                {"name": "data_ingestion___test___generate_fake_ufl"},
            ],
        }
    )
    _, original = etl_pipeline.run(ETL_Config)

    etl_pipeline = ETLPipeline()
    ETL_Config = OmegaConf.create(
        {
            "spark": {
                "appname": "TestCleaningDocument",
                "driver": {"memory": "16g"},
                "verbose": True,
            },
            "etl": [
                {
                    "name": "data_ingestion___test___generate_fake_ufl",
                    "args": {"faker_seed": faker_seed},
                },
                {
                    "name": "cleaning___document___split_by_word",
                    "args": {
                        "word_per_chunk": word_per_chunk,
                        "subset": "text",
                        "delimiter": delimiter,
                    },
                },
            ],
        }
    )
    _, result = etl_pipeline.run(ETL_Config)

    # check it is splitted properly
    assert all(len(row["text"].split(delimiter)) <= word_per_chunk for row in result.collect())

    # check combined version of splitted is same with the original
    result_combine = delimiter.join(result.map(lambda x: x["text"]).collect())
    original_combine = delimiter.join(original.map(lambda x: x["text"]).collect())

    assert len(original_combine) == len(result_combine)
    assert original_combine == result_combine
