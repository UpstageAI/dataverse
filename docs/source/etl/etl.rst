etl package
===========

Subpackages
-----------

.. toctree::
   :maxdepth: 4

   etl.bias
   etl.cleaning
   etl.data_ingestion
   etl.data_save
   etl.decontamination
   etl.deduplication
   etl.pii
   etl.quality
   etl.toxicity
   etl.utils

Submodules
----------

etl.pipeline module
-------------------

.. automodule:: etl.pipeline
   :members:
   :undoc-members:
   :show-inheritance:

.. autofunction:: etl.pipeline.ETLPipeline.status
.. autofunction:: etl.pipeline.ETLPipeline.search
.. autofunction:: etl.pipeline.ETLPipeline.get
.. autofunction:: etl.pipeline.ETLPipeline.setup_spark_conf
.. autofunction:: etl.pipeline.ETLPipeline.sample
.. autofunction:: etl.pipeline.ETLPipeline.run
.. autofunction:: etl.pipeline.ETLPipeline.run_emr

etl.registry module
-------------------

.. automodule:: etl.registry
   :members:
   :undoc-members:
   :show-inheritance:

.. autofunction:: etl.registry.auto_register
.. autofunction:: etl.registry.register_etl
.. autofunction:: etl.registry.ETLRegistry.register
.. autofunction:: etl.registry.ETLRegistry.search
.. autofunction:: etl.registry.ETLRegistry.get
.. autofunction:: etl.registry.ETLRegistry.get_all
