import pytest
from faker import Faker
from omegaconf import OmegaConf

from dataverse.etl import ETLPipeline
from dataverse.etl.registry import register_etl

faker_seed = 42


@register_etl
def helper___test___generate_data_for_test_length(spark, n=10, faker_seed=None, *args, **kwargs):
    faker = Faker()
    if faker_seed is not None:
        Faker.seed(faker_seed)

    data = []
    for _ in range(n):
        fake_data = faker.paragraph()
        data.append((fake_data, len(fake_data), len(fake_data.split())))
    data.append(("", len(""), len("".split())))
    df = spark.createDataFrame(data, ["text", "char_length", "word_length"])
    return df


def test_cleaning___length___char_len_filter():
    from etl.cleaning.length import cleaning___length___char_len_filter  # noqa: F401

    etl_pipeline = ETLPipeline()
    original = OmegaConf.create(
        {"etl": [{"name": "helper___test___generate_data_for_test_length"}]}
    )
    _, result = etl_pipeline.run(original)

    max_value, min_value = (
        result.select("char_length").rdd.max()[0],
        result.select("char_length").rdd.min()[0],
    )
    print("*----------------------------*")
    print(f"max length of test data is {max_value}. min length of test data is {min_value}")

    min_max_both = OmegaConf.create(
        {
            "etl": [
                {"name": "helper___test___generate_data_for_test_length"},
                {
                    "name": "cleaning___length___char_len_filter",
                    "args": {"min_len": 3, "max_len": 40},
                },
            ]
        }
    )
    _, result = etl_pipeline.run(min_max_both)
    assert all((row["char_length"] >= 3) and (row["char_length"] <= 40) for row in result.collect())
    assert result.filter(lambda row: row["text"] == "").count() == 0

    min_only = OmegaConf.create(
        {
            "etl": [
                {"name": "helper___test___generate_data_for_test_length"},
                {"name": "cleaning___length___char_len_filter", "args": {"min_len": 3}},
            ]
        }
    )
    _, result = etl_pipeline.run(min_only)
    assert all(row["char_length"] >= 3 for row in result.collect())
    assert result.filter(lambda row: row["text"] == "").count() == 0

    max_only = OmegaConf.create(
        {
            "etl": [
                {"name": "helper___test___generate_data_for_test_length"},
                {"name": "cleaning___length___char_len_filter", "args": {"max_len": 3}},
            ]
        }
    )
    _, result = etl_pipeline.run(max_only)
    assert all(row["char_length"] <= 3 for row in result.collect())
    assert result.filter(lambda row: row["text"] == "").count() > 0

    nothing_given = OmegaConf.create(
        {
            "etl": [
                {"name": "helper___test___generate_data_for_test_length"},
                {"name": "cleaning___length___char_len_filter"},
            ]
        }
    )
    with pytest.raises(AssertionError):
        _, result = etl_pipeline.run(nothing_given)


def test_cleaning___length___word_len_filter():
    from etl.cleaning.length import cleaning___length___word_len_filter  # noqa: F401

    etl_pipeline = ETLPipeline()
    original = OmegaConf.create(
        {"etl": [{"name": "helper___test___generate_data_for_test_length"}]}
    )
    _, result = etl_pipeline.run(original)

    max_value, min_value = (
        result.select("word_length").rdd.max()[0],
        result.select("word_length").rdd.min()[0],
    )
    print("*----------------------------*")
    print(f"max length of test data is {max_value}. min length of test data is {min_value}")

    min_max_both = OmegaConf.create(
        {
            "etl": [
                {"name": "helper___test___generate_data_for_test_length"},
                {
                    "name": "cleaning___length___word_len_filter",
                    "args": {"min_len": 3, "max_len": 40},
                },
            ]
        }
    )
    _, result = etl_pipeline.run(min_max_both)
    assert all((row["word_length"] >= 3) and (row["word_length"] <= 40) for row in result.collect())
    assert result.filter(lambda row: row["text"] == "").count() == 0

    min_only = OmegaConf.create(
        {
            "etl": [
                {"name": "helper___test___generate_data_for_test_length"},
                {"name": "cleaning___length___word_len_filter", "args": {"min_len": 3}},
            ]
        }
    )
    _, result = etl_pipeline.run(min_only)
    assert all(row["word_length"] >= 3 for row in result.collect())
    assert result.filter(lambda row: row["text"] == "").count() == 0

    max_only = OmegaConf.create(
        {
            "etl": [
                {"name": "helper___test___generate_data_for_test_length"},
                {"name": "cleaning___length___word_len_filter", "args": {"max_len": 3}},
            ]
        }
    )
    _, result = etl_pipeline.run(max_only)
    assert all(row["word_length"] <= 3 for row in result.collect())
    assert result.filter(lambda row: row["text"] == "").count() > 0

    nothing_given = OmegaConf.create(
        {
            "etl": [
                {"name": "helper___test___generate_data_for_test_length"},
                {"name": "cleaning___length___word_len_filter"},
            ]
        }
    )
    with pytest.raises(AssertionError):
        _, result = etl_pipeline.run(nothing_given)
