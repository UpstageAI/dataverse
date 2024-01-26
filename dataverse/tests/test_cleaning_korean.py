import random

from faker import Faker
from omegaconf import OmegaConf

from dataverse.etl import ETLPipeline
from dataverse.etl.registry import register_etl

faker_seed = 42
random_seed = 42


@register_etl
def helper___test___generate_korean(
    spark,
    n=100,
    repartition=20,
    create_type="word",
    faker_seed=None,
    random_seed=None,
    verbose=True,
    *args,
    **kwargs,
):
    """
    generate fake data that is mixed with korean and english.
    This creates data based on random ratio for each row.

    Args:
        spark (SparkSession): spark session
        n (int): the number of data to generate
        repartition (int): the number of partitions
        create_type (str): handles type of creating random data.
        faker_seed (int, optional): Random seed of faker library. Defaults to None.
        random_seed (int, optional): Random seed of random library. Defaults to None.
        verbose (bool): whether to print the information of the dataset
    """
    assert create_type in [
        "char",
        "word",
    ], "this is following filter_type of function `cleaning___korean___filter_by_ratio`"

    faker = Faker(["en_US", "ko_KR"])
    if faker_seed is not None:
        Faker.seed(faker_seed)
    faker_en = faker["en_US"]
    faker_ko = faker["ko_KR"]

    if random_seed is not None:
        random.seed(random_seed)

    from etl.cleaning.korean import JAUM, KOR_BEGIN, KOR_END, MOUM

    jamo = JAUM + MOUM

    def _generate_fake_korean_english_mixed(
        korean_ratio, total_count, create_type, space_ratio=0.3
    ):
        if create_type == "word":
            words = []
            for _ in range(total_count):
                if random.random() < korean_ratio:
                    words.append(faker_ko.name())
                else:
                    words.append(faker_en.last_name())
            return " ".join(words)
        else:  # create_type == "char"
            chars = ""
            korean_length = int(total_count * korean_ratio)
            for _ in range(korean_length):
                korean_type = random.choice(["jamo", "eumjeol"])
                cur_kor = (
                    chr(random.randint(KOR_BEGIN, KOR_END))
                    if korean_type == "eumjeol"
                    else random.choice(jamo)
                )
                chars += cur_kor
                if random.random() < space_ratio:
                    chars += " "
            english_length = total_count - korean_length
            english_text = faker_en.sentence(nb_words=english_length)
            chars += english_text[:english_length]
            return chars

    def _generate_fake_korean(n=100, create_type="word"):
        while n > 0:
            n -= 1
            korean_ratio = random.random()
            total_count = random.randint(0, 300)
            fake_korean = _generate_fake_korean_english_mixed(
                korean_ratio, total_count, create_type
            )
            yield {
                "id": faker.uuid4(),
                "text": fake_korean,
                "korean_ratio": korean_ratio,
            }

    rdd = spark.sparkContext.parallelize(_generate_fake_korean(n=n, create_type=create_type))
    rdd = rdd.repartition(repartition)

    return rdd


def test_cleaning___korean___filter_by_ratio():
    from etl.cleaning.korean import cleaning___korean___filter_by_ratio  # noqa

    filter_type = "word"
    korean_ratio = 0.6
    etl_pipeline = ETLPipeline()
    ETL_config = OmegaConf.create(
        {
            "etl": [
                {
                    "name": "helper___test___generate_korean",
                    "args": {
                        "faker_seed": faker_seed,
                        "random_seed": random_seed,
                        "n": 1000,
                        "create_type": filter_type,
                    },
                },
                {
                    "name": "cleaning___korean___filter_by_ratio",
                    "args": {
                        "subset": "text",
                        "korean_ratio": korean_ratio,
                        "filter_type": filter_type,
                    },
                },
            ]
        }
    )
    _, result = etl_pipeline.run(ETL_config)

    assert any(row["korean_ratio"] < korean_ratio for row in result.collect())


def test_cleaning___korean___filter_by_ratio_chars():
    from etl.cleaning.korean import cleaning___korean___filter_by_ratio  # noqa

    filter_type = "char"
    korean_ratio = 0.6
    etl_pipeline = ETLPipeline()
    ETL_config = OmegaConf.create(
        {
            "etl": [
                {
                    "name": "helper___test___generate_korean",
                    "args": {
                        "faker_seed": faker_seed,
                        "random_seed": random_seed,
                        "n": 1000,
                        "create_type": filter_type,
                    },
                },
                {
                    "name": "cleaning___korean___filter_by_ratio",
                    "args": {
                        "subset": "text",
                        "korean_ratio": korean_ratio,
                        "filter_type": filter_type,
                    },
                },
            ]
        }
    )
    _, result = etl_pipeline.run(ETL_config)

    print(result.collect())
    assert any(row["korean_ratio"] < korean_ratio for row in result.collect())


@register_etl
def helper___test___generate_korean_emoticon(spark, *args, **kwargs):
    data = spark.createDataFrame(
        [(1, "안녕하세요ㅋㅋㅋㅋㅋ"), (2, "ㅎㅎㅎㅎㅎ잘 지내세요?"), (3, "그래요ㅜㅜㅜㅜ"), (4, "ㅋㅋ쿵ㅜㅜㅋ쿠ㅜㅜ")],
        ["id", "text"],
    )
    return data


def test_cleaning___korean___reduce_emoticon():
    from etl.cleaning.korean import cleaning___korean___reduce_emoticon  # noqa

    etl_pipeline = ETLPipeline()
    ETL_config = OmegaConf.create(
        {
            "etl": [
                {"name": "helper___test___generate_korean_emoticon"},
                {"name": "cleaning___korean___reduce_emoticon"},
            ]
        }
    )
    _, result = etl_pipeline.run(ETL_config)

    expected_result = [
        (1, "안녕하세요ㅋㅋ"),
        (2, "ㅎㅎ잘 지내세요?"),
        (3, "그래요ㅜㅜ"),
        (4, "ㅋㅋ쿵ㅜㅜㅋㅋㅜㅜ"),
    ]

    for expected, result_row in zip(expected_result, result.collect()):
        assert (
            expected[1] == result_row["text"]
        ), f'Expected {expected[1]}, but got {result_row["text"]}'
