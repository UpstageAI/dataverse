# ETL (Extract, Transform, Load)
> Dataverse ETL is "Block-based coding powered by Spark"

- Each block is called `ETL process`
- Combination of ETL processes is called `ETL pipeline`
- ETL pipeline is managed by `config` file


## 🌌 What is ETL process?
> ETL process is the small code snippet, that is considered as a single unit of ETL pipeline. It is meant to be form various combinations to accommodate different kinds of data sources and transformations in ETL pipeline so it should be as generic as possible.

```python
def ETL_process(data, config):
    return data
```


## 🌌 What is ETL pipeline?
> ETL pipeline is the sequence of ETL processes.

```python
data = ETL_process_1()
data = ETL_process_2(data)
data = ETL_process_3(data)
```


## 🌌 How to run ETL Pipeline?
> Define the ETL process, and add in the config file to run the ETL pipeline.

```python
from dataverse.etl import ETLPipeline
from dataverse.config import Config

# 1. Define the ETL process in the config file
config = Config.get_config("TBD")

# 2. Run the ETL pipeline
etl_pipeline = ETLPipeline()
etl_pipeline.run(config)
```

### 🌠 How to set ETL Process to Configuration
> Once you have ETL process registered, you can define the ETL process in the config file. 

Let's say you have the following ETL processes registered
```python
from dataverse.etl import register_etl

@register_etl
def etl_process_start(spark, load_path, repartition=3):
    data = spark.read.load(load_path).repartition(repartition)
    return data

@register_etl
def etl_process_middle(data, threshold=0.5):
    data = data.filter(data['stars'] > threshold)
    return data

@register_etl
def etl_process_end(data, save_path, repartition=1):
    data.repartition(repartition).write.save(save_path)
    return None
```

You can use the following config to run the above ETL processes in order
- `etl_process_start` -> `etl_process_middle` -> `etl_process_end`

```yaml
spark:
  appname: dataverse_etl_sample
  driver:
    memory: 4g
etl:
  - name: etl_process_start
    args:
      load_path: ./sample/raw.parquet
      repartition: 3
  - name: etl_process_middle
    args:
      threshold: 0.5
  - name: etl_process_end
    args:
      save_path: ./sample/ufl.parquet
      repartition: 1
```

**Check the following real example for more details**
- This is used for `data ingestion` example
    - convert `raw` format to `ufl` format and save it to `parquet` format
- Config located at `dataverse/config/etl/sample/data_ingestion___one_stage.yaml`

```yaml
spark:
  appname: dataverse_etl_sample
  driver:
    memory: 4g
etl:
  - name: data_ingestion___red_pajama___hf2ufl
  - name: data_load___parquet___ufl2parquet
    args:
      save_path: ./sample/pajama_1G_ufl_1s.parquet
```


## 🌌 Principles for ETL Process
1. No `DRY` (Don't Repeat Yourself)
2. One file Only


### 🌠 No `DRY` (Don't Repeat Yourself)
> No `DRY` is applied between **ETL sub-categories**.
- So if similar ETL processes are used in same sub-categories, it could be shared.
- But if it's used in different sub-categories, it should not be shared.

As you can see in the following example, there are 2 ETL processes `common_process_a` and `common_process_b`seems nice to be shared. But as you can see, they are not shared. They are repeated. This is because of the No `DRY` principle.


```python
- deduplication/
    - exact.py
        - "def common_process_a():"
        - "def common_process_b():"
        - def deduplication___exact___a():
    - exact_datasketch.py
        - "def common_process_a():"
        - "def common_process_b():"
        - def deduplication___exact_datasketch___a():
        - def deduplication___exact_datasketch___b():
```

### 🌠 One file Only
Code that ETL process uses should be in the same file. This is because of the `One file Only` principle. Except **ETL Base class, few required utils functions, and open sources** there should be no dependency outside the file.

```python
# This is OK ✅
- deduplication/
    - exact.py
        - def helper_a():
        - def helper_b():
        - def etl_process():
            helper_a()
            helper_b()

                    
# This is not allowed ❌
- deduplication/
    - helper.py
        - def helper_a():
        - def helper_b():
    - exact.py
        from helper import helper_a
        from helper import helper_b

        - def etl_process():
            helper_a()
            helper_b()

```

ETL process itself is meant to be built to be used in various combination of ETL pipeline **So try to make it as generic as possible.** 😊


## 🌌 How to add new ETL process?
> ETL is managed by registry. What ever ETL you make, you need to register it to registry.
- `registry.py` - registry for ETL process classes


### 🌠 How to add a new ETL Process

<details>

First you need to check the category and sub-category of the ETL process you want to add. 
- `category` is the folder. This is pre-defined and you can add a new category if needed. Check below for how to add a new category
- `sub-category` is the python file. This is not pre-defined and you have to decide which name could be appropriate for the ETL process you want to add.

Now when you know the category and sub-category, you can add a new ETL process.
There are 2 ways to add a new ETL process
1. Inherit `BaseETL` for ETL `class`
2. Use decorator `@register_etl` to register your ETL `function`

```python
# check the __sample/ folder for example
from dataverse.etl import BaseETL
from dataverse.etl import register_etl

@register_etl
def category___subcategory___etl(rdd, config):
    # do something
    return rdd

class category___subcategory___etl(BaseETL):
    def run(rdd, config):
        # do something
        return rdd
```

</details>

### 🌠 ETL Processor Class Naming Convention
> This shared the same documentary with README.md in `__sample/` folder


<details>

```python
[ETL Category]___[ETL Sub-Category]___[ETL Name]
======================================
- "__sample/"
    - github.py
        - def __sample___github___remove_url()
        - def __sample___github___filter_by_stars()
- "bias/"
    - mmlu.py
        - def bias___mmlu___remove_word()
        - def bias___mmlu___to_parquet()
    - ducky.py
        - def bias___ducky___fly()
        - def bias___ducky___quark()
======================================
```

> caveat: the combination of `[ETL Category]___[ETL Sub-Category]___[ETL Name]` MUST be unique

1. `[ETL Category]` is the folder and category where the ETL is defined
    - `[ETL Category]` MUST be one of the following pre-defined list
        - `cleaning`
        - `decontamination`
        - `deduplication`
        - `data_ingestion`
        - `pil`
        - `quality`
        - `toxicity`
        - `bias`
        - `data_load`
        - `utils`
2. `[ETL Sub-Category]` is the name of the file where the ETL is defined
    - no pre-defined list
        - it could be a dataset name
        - or a nickname of yours
        - or whatever you think it's appropriate
    - e.g. `github` or `kaggle` or `mmlu` whatever you want
3. `[ETL Name]` naming should follow `function` naming convention, even it's `class`
    - all lower case
    - use underscore `_` to separate words
4. Each is separated by `___` (triple underscore)
    - e.g. `bias___mmlu___remove_word()`


#### Why does folder, file name included in the ETL class name?
- To avoid the following tmp names on dynamic construction of ETL class
    - e.g. `tmp___ipykernel_181248___remove_url` <- jupyter notebook env
    - e.g. `python3.10___abc___remove_url` <- dynamic class construction by `type`
- so decided to control the name space by only `ETL class name` which includes folder, file name



</details>

### 🌠 How to add a new ETL Category

<details>

- add a new category to `ETL_CATEGORY` in `registry.py`
```python
ETL_CATEGORIES = [
    'data_ingestion',
    'decontamination',
    'deduplication',
    'bias',
    'toxicity',
    'cleaning',
    'pii',
    'quality',
    'data_load',
    'utils',
]
```
</details>

### 🌠 Ignoring ETL Sub-Category python files
> If you want to ignore some of the ETL sub-category python files, you can add the file name to `ETL_IGNORE` in `registry.py`

when you want to make a file just for storage purpose, you can add the file name to `ETL_IGNORE` in `registry.py`

```python
ETL_IGNORE = [
    '__init__.py',
    'storage.py'
]
```


## 🌌 ETL Categories
> This is predefined and you can modify the list if needed. Just make sure you update the `ETL_CATEGORIES` list in `registry.py` as well.

### 🌠 __Sample__
> This is to show how to use the etl package

### 🌠 Data Ingestion
> converting data from one format, schema to another

### 🌠 Data Loading
> saving data to desired location

### 🌠 Deduplications
> includes removing duplication inside data

### 🌠 Decontamination
> removing contamination from data
- e.g. removing benchmark data from data

### 🌠 Cleaning
> cleaning data
- e.g. removing HTML tags from text
- e.g. data normalization

### 🌠 PII (Personally Identifiable Information)
> removing PII from data

### 🌠 Quality
> improving data quality
- e.g. removing data with low quality

### 🌠 Toxicity
> removing toxic data
- e.g. removing data with toxic words

### 🌠 Bias
> removing bias from data
- e.g. removing data with gender bias words

### 🌠 Utils
> utilities for the ETL process
- e.g. sampling, logging, error handling, etc