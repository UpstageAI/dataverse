
===================
Quickstart
===================


Various and more detailed tutorials are `here <https://github.com/UpstageAI/dataverse/tree/main/examples>`__.

- `add_new_etl_process.ipynb <https://github.com/UpstageAI/dataverse/blob/main/examples/etl/ETL_04_add_new_etl_process.ipynb>`__ : If you want to use your custom function, you have to register the function on Dataverse. This will guide you from register to apply it on pipeline.
- `test_etl_process.ipynb <https://github.com/UpstageAI/dataverse/blob/main/examples/etl/ETL_05_test_etl_process.ipynb>`__ : When you want to get test(sample) data to quickly test your ETL process, or need data from a certain point to test your ETL process.
- `scaleout_with_EMR.ipynb <https://github.com/UpstageAI/dataverse/blob/main/examples/etl/ETL_06_scaleout_with_EMR.ipynb>`__ : For people who want to run their pipeline on EMR cluster.


1. Set your ETL process as config.
``````````````````````````````````
.. code-block:: python

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
            'name': 'data_save___parquet___ufl2parquet',
            'args': {'save_path': './guideline/etl/sample/quickstart.parquet'}
          }
        ]
    })

Above code block is an example of an ETL process in *Dataverse*.
In *Dataverse*, the available registered ETL functions are referred to as ``blocks``, and this example is comprised of four blocks. You can freely combine these blocks using config to create the ETL processes for your needs.
The list of available functions and args of them can be found in the `API Reference <https://data-verse.readthedocs.io/en/latest/>`__. Each functions 'args' should be added in dictionary format.


2. Run ETLpipeline.
```````````````````

.. code-block:: python

  from dataverse.etl import ETLPipeline

  etl_pipeline = ETLPipeline()
  spark, dataset = etl_pipeline.run(config=ETL_config, verbose=True)

ETLPipeline is an object designed to manage the ETL processes.
By inserting ``ETL_config`` which is defined in the previous step into ETLpipeline object and calling the ``run`` method,
stacked ETL blocks will execute in the order they were stacked.


3. Result file is saved on the save_path
```````````````````````````````````````````

.. code-block:: python

  import pandas as pd
  pd.read_parquet('./guideline/etl/sample/quickstart.parquet')

As the example gave ``save_path`` argument to the last block of ``ETL_config``, 
data passed through the process will be saved on the given path.