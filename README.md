<div align="center">

<br>
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/images/dataverse_logo-white.png" width=300>
  <source media="(prefers-color-scheme: light)" srcset="docs/images/dataverse_logo-color.png" width=300>
  <img alt="DATAVERSE" src="docs/images/dataverse_logo-color.png" width=300>
</picture>

<br>

The Universe of Data. 
All about Data, Data Science, and Data Engineering.

[Docs](https://data-verse.gitbook.io/docs/) • [Examples](https://github.com/UpstageAI/dataverse/tree/main/examples) • [API Reference](https://data-verse.readthedocs.io/en/latest/) • [FAQ](https://data-verse.gitbook.io/docs/documents/faqs) • [Contribution Guide](https://github.com/UpstageAI/dataverse/blob/main/contribution/CONTRIBUTING.md)  • [Contact](mailto:dataverse@upstage.ai)  • [Discord](https://discord.gg/uC7cmSKwvm)
<br><br>
<div align="left">

## Welcome to Dataverse!
Dataverse is a freely-accessible open-source project that supports your ETL(Extract, Transform and Load) pipeline with Python. We offer a simple, standardized and user-friendly solution for data processing and management, catering to the needs of data scientists, analysts, and developers in LLM era. Even though you don't know much about Spark, you can use it easily via _dataverse_.

### Why should I use Dataverse?
- **Enhanced productivity**: Dataverse streamline your workflow by integrating multiple preprocessing libraries into one, eliminating the hassle of settings and searching for the right tools. Furthermore, you can easily take advantages of Spark’s efficiency even if you’re not an Spark expert.
- **Improved data quality**: Elevate your data quality with a variety of preprocessing functions. Dataverse helps you to make high-quality data for analysis, manage, and train LLM, etc.
- **Facilitated collaboration**: Offer uniform preprocessing codes to ensure consistent results whether who runs the code. Dataverse also enable collaboration among users with varying levels of Spark proficiency.

### Key Features of Dataverse
- **Block-Based:** Dataverse lets you build Spark code like putting together puzzle pieces. You can easily add, take away, or rearrange pieces to get the results you want.
- **Configure-Based:** Dataverse has a user-friendly setup where you don't need to know all the code. Just set up the options, and you're good to go.
- **Extensible:** It's designed to meet your specific demands, allowing for custom features that fit perfectly with your project.
<br>

## 🌌 Installation
### 🌠 Prerequisites
To use this library, the following conditions are needed:
- Python (version between 3.10 and 3.11)
- JDK (version 11)
- PySpark
Detail installation guide for prerequisites can be found on [here](https://data-verse.gitbook.io/docs/installation).

### 🌠 Install via PyPi
```bash
pip install dataverse
```

<br>

## 🌌 Quickstart
Various and more detailed tutorials are [here](https://github.com/UpstageAI/dataverse/tree/main/examples).

- [add_new_etl_process.ipynb](https://github.com/UpstageAI/dataverse/blob/main/examples/etl/ETL_04_add_new_etl_process.ipynb) : If you want to use your custom function, you have to register the function on Dataverse. This will guide you from register to apply it on pipeline.
- [test_etl_process.ipynb](https://github.com/UpstageAI/dataverse/blob/main/examples/etl/ETL_05_test_etl_process.ipynb) : When you want to get test(sample) data to quickly test your ETL process, or need data from a certain point to test your ETL process.
- [scaleout_with_EMR.ipynb](https://github.com/UpstageAI/dataverse/blob/main/examples/etl/ETL_06_scaleout_with_EMR.ipynb) : For people who want to run their pipeline on EMR cluster.


<details>
    <summary><u>Detail to the example etl configure.</u></summary>
    <ul></ul>
    <ul>
        <li style="line-height:250%;"> <b>data_ingestion___huggingface___hf2raw </b></li>
        Load dataset from <a href="https://huggingface.co/datasets/allenai/ai2_arc">Hugging Face</a>, which contains a total of 2.59k rows.
    </ul>
    <ul>
        <li style="line-height:250%;"> <b>utils___sampling___random </b></li>
        To decrease the dataset size, randomly subsample 50% of data to reduce the size of dataset, with a default seed value of 42. <br/>
        This will reduce the dataset to 1.29k rows. 
    </ul>
    <ul>
        <li style="line-height:250%;"> <b>deduplication___minhash___lsh_jaccard </b></li>
        Deduplicate by <code>question</code> column, 5-gram minhash jaccard similarity threshold of 0.1.
    </ul>
    <ul>
        <li style="line-height:250%;"> <b>data_load___parquet___ufl2parquet </b></li>
        Save the processed dataset as a Parquet file to <code>./guideline/etl/sample/quickstart.parquet</code>.<br/>
        The final dataset comprises around 1.14k rows.
    </ul>
</details>

```python
# 1. Set your ETL process as config.

from omegaconf import OmegaConf

ETL_config = OmegaConf.create({
    'spark': {
        'appname': 'ETL',
        'driver': {'memory': '4g'},
    },
    'etl': [
        { 
            'name': 'data_ingestion___huggingface___hf2raw', # Extract
            'args': {'name_or_path': ['ai2_arc', 'ARC-Challenge']}
        },
        {
            'name': 'utils___sampling___random',
            'args': {'sample_n_or_frac': 0.5}
        },
        {
            'name': 'deduplication___minhash___lsh_jaccard', # Transform
            'args': {'threshold': 0.1,
                    'ngram_size': 5,
                    'subset': 'question'}
        },
        {
          'name': 'data_load___parquet___ufl2parquet', # Load
          'args': {'save_path': './guideline/etl/sample/quickstart.parquet'}
        }
      ]
  })
```


```python
# 2. Run ETLpipeline.

from dataverse.etl import ETLPipeline

etl_pipeline = ETLPipeline()
spark, dataset = etl_pipeline.run(config=ETL_config, verbose=True)
```
```python
# 3. Result file is saved on the save_path
```
<br>


## 🌌 Contributors
<a href="https://github.com/UpstageAI/dataverse/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=UpstageAI/dataverse" />
</a>

## 🌌 Acknowledgements

Dataverse is an open-source project orchestrated by the **Data-Centric LLM Team** at `Upstage`, designed as an data ecosystem for LLM(Large Language Model). Launched in March 2024, this initiative stands at the forefront of advancing data handling in the realm of LLM.

## 🌌 License
Dataverse is completely freely-accessible open-source and licensed under the Apache-2.0 license.


## 🌌 Citation
> If you want to cite our 🌌 Dataverse project, feel free to use the following bibtex

```bibtex
@misc{dataverse,
  title = {Dataverse},
  author = {Hyunbyung Park, Sukyung Lee, Gyoungjin Gim, Yungi Kim, Seonghoon Yang, Jihoo Kim, Changbae Ahn, Chanjun Park},
  year = {2024},
  publisher = {GitHub, Upstage AI},
  howpublished = {\url{https://github.com/UpstageAI/dataverse}},
}
```
