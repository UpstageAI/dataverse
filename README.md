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

[Docs](https://data-verse.gitbook.io/docs/) • [Examples](https://github.com/UpstageAI/dataverse/tree/main/examples) • [API Reference](https://data-verse.readthedocs.io/en/latest/) • [FAQ](https://data-verse.gitbook.io/docs/documents/faqs) • [Contribution Guide](https://github.com/UpstageAI/dataverse/blob/main/contribution/CONTRIBUTING.md)  • [Contact](mailto:dataverse@upstage.ai)  • [Discord](https://discord.gg/aAqF7pyq4h)
<br><br>
<div align="left">

## Welcome to Dataverse!
Dataverse is a freely-accessible open-source project that supports your **ETL(Extract, Transform and Load) pipeline with Python**. We offer a simple, standardized and user-friendly solution for data processing and management, catering to the needs of data scientists, analysts, and developers in LLM era. Even though you don't know much about Spark, you can use it easily via _dataverse_.

### With Dataverse, you are empowered to

- utilize a range of preprocessing functions without the need to install multiple libraries.
- create high-quality data for analysis and training of Large Language Models (LLM).
- leverage Spark with ease, regardless of your expertise level.
- facilitate smoother collaboration among users with varying degress of Spark proficiency.
- enjoy freedom from the limitations of local environments by harnessing the capabilities of AWS EMR.

### Architecture of Dataverse
![Architecture of Dataverse](./docs/images/dataverse_system_architecture.jpg)

### Key Features of Dataverse
- **Block-Based**: In Dataverse, a `block` means a `registered ETL function` which is running on Spark. You can build Spark code like putting together puzzle pieces. You can easily add, take away, or re-arrange pieces to get the results you want via configure.
- **Configure-Based**: All the setups for Spark and steps of block can be defined with configure. You don't need to know all the code. Just set up the options, and you're good to go.
- **Extensible**: It's designed to meet your specific demands, allowing for custom features that fit perfectly with your project.

If you want to know more about Dataverse, please checkout our [docs](https://data-verse.gitbook.io/docs/).

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
    # Set up Spark
    'spark': { 
        'appname': 'ETL',
        'driver': {'memory': '4g'},
    },
    'etl': [
        { 
          # Extract; You can use HuggingFace datset from hub directly!
          'name': 'data_ingestion___huggingface___hf2raw', 
          'args': {'name_or_path': ['ai2_arc', 'ARC-Challenge']}
        },
        {
          # Reduce dataset scale
          'name': 'utils___sampling___random',
          'args': {'sample_n_or_frac': 0.5}
        },
        {
          # Transform; deduplicate data via minhash
          'name': 'deduplication___minhash___lsh_jaccard', 
          'args': {'threshold': 0.1,
                  'ngram_size': 5,
                  'subset': 'question'}
        },
        {
          # Load; Save the data
          'name': 'data_load___parquet___ufl2parquet',
          'args': {'save_path': './guideline/etl/sample/quickstart.parquet'}
        }
      ]
  })
```
Above code block is an example of an ETL process in Dataverse. In Dataverse, the available registered ETL functions are referred to as `blocks`, and this example is comprised of four blocks. You can freely combine these blocks using config to create the ETL processes for your needs. The list of available functions and args of them can be found in the [API Reference](https://data-verse.readthedocs.io/en/latest/). Each functions 'args' should be added in dictionary format.

```python
# 2. Run ETLpipeline.

from dataverse.etl import ETLPipeline

etl_pipeline = ETLPipeline()
spark, dataset = etl_pipeline.run(config=ETL_config, verbose=True)
```
ETLPipeline is an object designed to manage the ETL processes. By inserting `ETL_config` which is defined in the previous step into ETLpipeline object and calling the `run` method, stacked ETL blocks will execute in the order they were stacked.

```python
# 3. Result file is saved on the save_path
```
As the example gave `save_path` argument to the last block of `ETL_config`, data passed through the process will be saved on the given path.

<br>

## 🌌 Modules
Currently, about 50 functions are registered as the ETL process, which means they are eagerly awaiting your use!
| Type      | Package         | description                                                                                       |
|-----------|-----------------|---------------------------------------------------------------------------------------------------|
| Extract   | data_ingestion  | Loading data from any source to the preferred format                                              |
| Transform | bias            | (WIP) Reduce skewed or prejudiced data, particularly data that reinforce stereotypes.             |
|           | cleaning        | Remove irrelevant, redundant, or noisy information, such as stop words or special characters.     |
|           | decontamination | (WIP) Remove contaminated data including benchmark.                                               |
|           | deduplication   | Remove duplicated data, targeting not only identical matches but also similar data.               |
|           | pii             | PII stands for Personally Identifiable Information. Removing sensitive information from data.     |
|           | quality         | Improving the data quality, in the perspective of accuracy, consistency, and reliability of data. |
|           | toxicity        | (WIP) Removing harmful, offensive, or inappropriate content within the data.                      |
| Load      | data_load       | Saving the processed data to a preferred source like data lake, database, etc.                    |
| Utils     | utils           | Essential tools for data processing, including sampling, logging, statistics, etc.                |
<br>

## 🌌 Dataverse supports AWS
Dataverse supports AWS! Step by step guide to setting up is [here](https://data-verse.gitbook.io/docs/lets-start/aws-s3-support).
</br>

## 🌌 Dataverse use-case
> If you have any use-cases of your own, please feel free to let us know. </br>We would love to hear about them and possibly feature your case.


*✨* [`Upstage`](https://www.upstage.ai/) is using Dataverse for preprocessing the data for the training of [Solar Mini](https://console.upstage.ai/services/solar?utm_source=upstage.ai&utm_medium=referral&utm_campaign=Main+hero+Solar+card&utm_term=Try+API+for+Free&utm_content=home). </br>
*✨* [`Upstage`](https://www.upstage.ai/) is using Dataverse for preprocessing the data for the [Up 1T Token Club](https://en.content.upstage.ai/1tt).



## 🌌 Contributors
<a href="https://github.com/UpstageAI/dataverse/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=UpstageAI/dataverse" />
</a>

## 🌌 Acknowledgements

Dataverse is an open-source project orchestrated by the **Data-Centric LLM Team** at [`Upstage`](https://www.upstage.ai/), designed as an data ecosystem for LLM(Large Language Model). Launched in March 2024, this initiative stands at the forefront of advancing data handling in the realm of LLM.

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
