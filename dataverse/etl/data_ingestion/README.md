# Data Ingestion
> Ingest various data sources into the desired format

**Recommendation for Data Ingestion**
> Use Data Ingestion to convert all datasets to unified format you choose before preprocessing(transform)
- for `Text Only` Dataset, recommend using `ufl` format
    - for details on `ufl` format, see below
- for `other` dataset, consider creating a new unified format


## Naming Convention
> This is a strong recommendation. You can use your own naming convention if you want.

- Name sub-category (python file) to the data source name
- Name the ETL process as the final format
    
```python
- "data_ingestion/"
    # converting raw data to desired format
    - mmlu.py
        - def data_ingestion___mmlu___parquet()
        - def data_ingestion___mmlu___ufl()
    - squad.py
        - def data_ingestion___squad___ufl()
    - mnist.py
        - def data_ingestion___mnist___ufl()

    # this is used when loading UFL format
    - ufl.py
        - def data_ingestion___ufl___ufl()
```

## UFL (Upstage Format for LLM)
> This is the data format recommended by the Upstage LLM. Dataverse standard format for preparing pretraining dataset.
```python
{
	"id":"uuid",
	"name": "string",
	"text":"string",
	"meta": "string",
}
```

- `id` - uuid v1
- `name` - name of the dataset
- `text` - text of the dataset
- `meta` - meta data of the dataset
    - meta data is a stringified json object

### Why stringified for meta data?
> Meta data does not have a fixed schema. It can be anything. So, it is stringified to avoid any issues with the schema.

**huggingface datasets** 
- when 2 datasets have different meta data schema, it will throw an error when merging the datasets