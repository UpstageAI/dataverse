# Sample
> This is to show how to use the etl package

ETL is managed by Registry.
What ever ETL you make, you need to register it to Registry.

## How to register ETL
1. Inherit `BaseETL` for ETL `class`
2. Use decorator `@register_etl` to register your ETL `function`

## ETL Processor Class Naming Convention

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


### Why does folder, file name included in the ETL class name?
- To avoid the following tmp names on dynamic construction of ETL class
    - e.g. `tmp___ipykernel_181248___remove_url` <- jupyter notebook env
    - e.g. `python3.10___abc___remove_url` <- dynamic class construction by `type`
- so decided to control the name space by only `ETL class name` which includes folder, file name
