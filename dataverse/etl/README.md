# ETL (Extract, Transform, Load)
> The ETL only includes the process backed by Spark. There is currently 8 steps in the ETL pipeline which the following and this will be modified in the future.

## 🌌 What is ETL process?
> ETL process is the small code snippet, that is considered as a single unit of ETL pipeline. It is meant to be used in various combination of ETL pipeline so it should be as generic as possible.

## 🌌 Principles for ETL Process
1. No `DRY` (Don't Repeat Yourself)
2. One file Only


### 🌠 No `DRY` (Don't Repeat Yourself)
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
from dataverse.etl.registry import BaseETL
from dataverse.etl.registry import register_etl

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
        - `junk`
        - `decontamination`
        - `deduplication`
        - `data_ingestion`
        - `pil`
        - `quality`
        - `toxicity`
        - `bias`
        - `data_load`
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
    'junk',
    'pii',
    'quality',
    'data_load',
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

## 🌌 How to run ETL process?
- TBD


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

### 🌠 Junk
> removing junk data
- e.g. removing HTML tags from text

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
