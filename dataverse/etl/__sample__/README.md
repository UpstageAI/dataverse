# Sample
> This is to show how to use the etl package

ETL is managed by Registry.
What ever ETL you make, you need to register it to Registry.

## How to register ETL
1. Inherit `BaseETL` for ETL `class`
2. Use decorator `@register_etl` to register your ETL `function`

## Naming
ETL name should be named with the following convention
```python
[ETL Type]___[ETL Name]
==================================================
e.g. junk___remove_junk
e.g. ingestion___github_to_parquet
```
1. it MUST be separated by `___` (3 underscores)
2. naming should follow function naming convention
    - all lower case
    - use underscore `_` to separate words
3. REMEMBER, all ETL are `Class`! but follow 2
    - It could be awkward, but please follow this
4. ETL Type MUST be one of the following
    - `junk`
    - `decontamination`
    - `deduplication`
    - `data_ingestion`
    - `pil`
    - `quality`
    - `toxicity`
    - `bias`
5. ETL Name depends on yourself
    - it doens't need to be unique
    - but the combination of ETL Type and ETL Name MUST be unique
        - unless you will get an error when you register it