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
config = Config.load("TBD")
config = Config.set_default(config)

# 2. Run the ETL pipeline
etl_pipeline = ETLPipeline()
spark, data = etl_pipeline.run(config)
```

### 🌠 What is returned after running ETL pipeline?
> `spark` and `data` is returned after running ETL pipeline

- `spark` - spark session
- `data` - data after running ETL pipeline


#### `spark` status depends on the last ETL process
- `data_load` ETL process at the end
    - spark will be terminated
- otherwise
    - spark will be alive
    - you can use `spark` to do whatever you want


## 🌌 How to add new ETL process?
> ETL is managed by registry. Whatever ETL you make, you need to register it to registry.

### 🌠 Choose what `Category` & `Sub-Category` to put your ETL process
> First you need to check the category and sub-category of the ETL process you want to add. 

```python
======================================
- etl/
    - CATEGORY/
        - __init__.py
        - SUBCATEGORY.py
            - def CATEGORY___SUBCATEGORY___ETL_PROCESS()
======================================
```

- `category` is the folder. This is pre-defined and you can add a new category if needed. **Check below to learn more about category**
- `sub-category` is the python file. This is not pre-defined and you have to decide which name could be appropriate for the ETL process you want to add.

Now when you know the `category` and `sub-category`, you can add a new ETL process.
There are only one way to add a new ETL process

### 🌠 Use decorator `@register_etl` to register your ETL `function`

```python
# check the __sample/ folder for example
from dataverse.etl import register_etl

@register_etl
def category___subcategory___etl(rdd, config):
    # do something
    return rdd
```

#### ☣️ Inheriting `BaseETL` is NOT ALLOWED ☣️
```python
from dataverse.etl import BaseETL
class category___subcategory___etl(BaseETL):
    def run(rdd, config):
        # do something
        return rdd
```

### 🌠 ETL Process Class Naming Convention
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

## 🌌 Principles for ETL Process
> When you create your own ETL process, you should follow the following principles

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



## 🌌 How to use ETL Process by Configuration
> Now let's learn how to use ETL process by configuration

### 🌠 Register ETL process
> This is same as above. Register ETL process using `@register_etl` decorator

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

### 🌠 Define ETL process in the config file
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
- Config located at `dataverse/config/etl/sample/ETL___one_cycle.yaml`

```yaml
spark:
  appname: dataverse_etl_sample
  driver:
    memory: 16g
etl:
  - name: data_ingestion___test___generate_fake_ufl
  - name: utils___sampling___random
    args:
      sample_n_or_frac: 0.1
  - name: deduplication___minhash___lsh_jaccard
  - name: data_load___huggingface___ufl2hf_obj
```


## 🌌 How to add a new ETL Category

### 🌠 Add a new folder to `etl/` folder

```python
======================================
- etl/
    - YOUR_NEW_CATEGORY/
        - __init__.py
        - YOUR_NEW_SUBCATEGORY.py
    - data_ingestion/
    ...
======================================
```

### 🌠 Add a new category to `ETL_CATEGORY` in `registry.py`
> Only added category will be recognized by the ETL pipeline

```python
ETL_CATEGORIES = [
    YOUR_NEW_CATEGORY,
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

### 🌠 Pre-defined ETL Categories

```python
======================================
- etl/
    - "__sample/"
        - This is to show how to use the etl package
    - "data_ingestion/"
        - converting data from one format, schema to another
    - "data_load/"
        - saving data to desired location
    - "quality/"
        - improving data quality
        - e.g. removing data with low quality
    - "cleaning/"
        - cleaning data
        - e.g. removing HTML tags from text
        - e.g. data normalization
    - "decontamination/"
        - removing contamination from data
        - e.g. removing benchmark data from data
    - "deduplication/"
        - removing duplication inside data
    - "pii/"
        - removing PII from data
    - "bias/" - 
        - removing bias from data
        - e.g. removing data with gender bias words
    - "toxicity/"
        - removing toxic data
        - e.g. removing data with toxic words
    - "utils/"
        - utilities for the ETL process
        - e.g. sampling, logging, error handling, etc
======================================
```

## 🌌 How to Ignore specific ETL Sub-Category
> If you want to ignore some of the `ETL sub-category` python files, you can add the file name to `ETL_IGNORE` in `registry.py`

when you want to make a file just for storage purpose, you can add the file name to `ETL_IGNORE` in `registry.py`

```python
ETL_IGNORE = [
    '__init__.py',
    'storage.py'
]
```
