# Data Ingestion
> Ingest various data sources into the desired format

**Recommendation for Data Ingestion**
> Use Data Ingestion to convert all datasets to unified format you choose before preprocessing(transform)
- for `Text Only` Dataset, recommend using `ufl` format
    - for details on `ufl` format, see below
- for `other` dataset, consider creating a new unified format

## 📚 Data Ingestion Flow
> This is the recommended flow for data ingestion, but not mandatory

There is 2 types of data ingestion flow for standard
- **1 step flow** (load & template)
    - load `raw data` to `desired format` directly
- **2 step flow** (load -> template)
    - load `raw data` to `raw format` first with **dict type**
    - convert `raw format` to `desired format`

If you want to create 3 steps, thats on you. Remember this is just a guideline.

### 📗 Why 2 step flow?
> To support various templates for the same data source

Let's suppose we are ingesting `mmlu` dataset and our desired format is `ufl` format.
And with the following 2 templates, we can create 2 different data with `ufl` format.
To give user a broader choice, multiple templates for the same data source is necessary and 2 step flow is the way to go.

```python
# original format
"Let p = (1, 2, 5, 4)(2, 3) in S_5 . Find the index of <p> in S_5."	
"8"	
"2"	
"24"	
"120"	
"C"

# template v1 - only question (q)
ufl = {
    'id': "b1c2d3e4f5g6h7i8j9k0",
    'name': "mmlu",
    'text': "Let p = (1, 2, 5, 4)(2, 3) in S_5 . Find the index of <p> in S_5.",
    'meta': {},
}

# template v2 - question, answer (qa)
ufl = {
    'id': "a1b2c3d4e5f6g7h8i9j0",
    'name': "mmlu",
    'text': "question: Let p = (1, 2, 5, 4)(2, 3) in S_5 . Find the index of <p> in S_5.\nanswer: 8",
    'meta': {},
}

```


## 📚 Naming Convention
> This is a strong recommendation. You can use your own naming convention if you want.

**caveat**
- `ufl` is not a file format rather a schema(data format). 

### 📗 1 step flow
- Name sub-category (python file) to the data source name
- Name the ETL process as the `raw source` -> `target format`
    - e.g. `parquet2ufl` means converting parquet to ufl format
    - e.g. `hf2ufl` means converting huggingface dataset to ufl format

    
```python
def [ETL Category]___[ETL Sub-Category]___[raw source]2[target format]()

- "data_ingestion/"
    # converting raw data to desired format
    - mmlu.py
        - def data_ingestion___mmlu___parquet2ufl()
        - def data_ingestion___mmlu___hf2ufl()
    - squad.py
        - def data_ingestion___squad___hf2ufl()
    - mnist.py
        - def data_ingestion___mnist___csv2ufl()

    # this is used when loading UFL format saved in parquet
    - ufl.py
        - def data_ingestion___ufl___parquet2ufl()
```

### 📗 2 step flow
- Name sub-category (python file) to the data source name

#### 📖 Step 1 - load raw data to raw format
- Name the ETL process as the `raw source` -> `raw format`
    - e.g. `parquet2raw` means converting parquet to raw format
    - e.g. `hf2raw` means converting huggingface dataset to raw format

```python
def [ETL Category]___[ETL Sub-Category]___[raw source]2raw()

- "data_ingestion/"
    # converting raw data to raw format
    - mmlu.py
        - def data_ingestion___mmlu___parquet2raw()
        - def data_ingestion___mmlu___hf2raw()
    - squad.py
        - def data_ingestion___squad___hf2raw()
    - mnist.py
        - def data_ingestion___mnist___csv2raw()
```

#### 📖 Step 2 - convert raw format to desired format
- Name the ETL process as the `raw format` -> `target format`
    - e.g. `raw2ufl` means converting raw format to ufl format
- Add template name to the end of the function name
    - e.g. `raw2ufl_q` means converting raw format to ufl format with `question` template
    - e.g. `raw2ufl_qa` means converting raw format to ufl format with `question & answer` template

```python
def [ETL Category]___[ETL Sub-Category]___raw2[target format]_[template name]()

- "data_ingestion/"
    # converting raw format to desired format
    - mmlu.py
        - def data_ingestion___mmlu___raw2ufl_q()
        - def data_ingestion___mmlu___raw2ufl_qa()
    - squad.py
        - def data_ingestion___squad___raw2ufl()
    - mnist.py
        - def data_ingestion___mnist___raw2ufl()
```


## 📚 UFL (Upstage Format for LLM)
> This is the schema(data format) recommended by the Upstage LLM. Dataverse standard format for preparing pretraining dataset.
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

### 📗 Why stringified for meta data?
> Meta data does not have a fixed schema. It can be anything. So, it is stringified to avoid any issues with the schema.

**huggingface datasets** 
- when 2 datasets have different meta data schema, it will throw an error when merging the datasets