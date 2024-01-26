import random

from faker import Faker
from omegaconf import OmegaConf

from dataverse.etl import ETLPipeline
from dataverse.etl.registry import register_etl

faker_seed = 42
random_seed = 42


@register_etl
def helper___test___generate_html(
    spark,
    n=100,
    repartition=20,
    faker_seed=None,
    random_seed=None,
    verbose=True,
    *args,
    **kwargs,
):
    faker = Faker()
    if faker_seed is not None:
        Faker.seed(faker_seed)
    if random_seed is not None:
        random.seed(random_seed)

    def _generate_fake_html_format():
        tags = ["p", "h1", "h2", "div", "span", "a", "ul", "ol", "li", "strong", "em"]
        html_content = ""
        for _ in range(random.randint(3, 10)):
            tag = random.choice(tags)

            if tag in ["ul", "ol"]:
                items = ""
                for _ in range(random.randint(2, 4)):
                    items += f"<li>{faker.sentence()}</li>"
                html_content += f"<{tag}>{items}</{tags}>"

            elif tag == "a":
                html_content += f'<a href="{faker.url()}">{faker.word()}</a>'

            else:
                html_content += f"<{tag}>{faker.text()}</{tag}>"

        return html_content

    def _generate_fake_html(n=100):
        while n > 0:
            n -= 1
            fake_html = _generate_fake_html_format()
            yield {"id": faker.uuid4(), "text": fake_html}

    rdd = spark.sparkContext.parallelize(_generate_fake_html(n=n))
    rdd = rdd.repartition(repartition)
    return rdd


def test_cleaning___html___extract_plain_text():
    from etl.cleaning.html import cleaning___html___extract_plain_text  # noqa: F401

    etl_pipeline = ETLPipeline()
    ETL_config = OmegaConf.create(
        {
            "etl": [
                {
                    "name": "helper___test___generate_html",
                    "args": {"faker_seed": faker_seed},
                },
                {
                    "name": "cleaning___html___extract_plain_text",
                    "args": {"subset": "text"},
                },
            ]
        }
    )
    _, result = etl_pipeline.run(ETL_config)

    for row in result.collect():
        assert "<" not in row["text"]
        assert ">" not in row["text"]


def test_cleaning___html___extract_plain_text_trafilatura():
    from etl.cleaning.html import cleaning___html___extract_plain_text  # noqa: F401

    etl_pipeline = ETLPipeline()
    ETL_config = OmegaConf.create(
        {
            "etl": [
                {
                    "name": "helper___test___generate_html",
                    "args": {"faker_seed": faker_seed},
                },
                {
                    "name": "cleaning___html___extract_plain_text",
                    "args": {"subset": "text", "trafilatura": True},
                },
            ]
        }
    )
    _, result = etl_pipeline.run(ETL_config)

    for row in result.collect():
        assert "<" not in row["text"]
        assert ">" not in row["text"]
