
"""
ETL Interface 
----------------------
user will be interacting with this interface
"""

import os
import sys
from typing import Union
from omegaconf import OmegaConf
from omegaconf import DictConfig

from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

from dataverse.etl.registry import ETLRegistry
from dataverse.etl.registry import auto_register



class ETLPipeline:
    """
    ETL Pipeline
    """
    def __init__(self):
        self.registry = ETLRegistry()

        # reset registry to avoid duplicate registration
        self.registry.reset()
        auto_register()

    def run(self, config: Union[str, dict, DictConfig, OmegaConf], *args, **kwargs):
        """
        Args:
            config (Union[str, dict, OmegaConf]): config for the etl
                - str: path to the config file
                - dict: config dict
                - OmegaConf: config object
        """
        # =============== [ Load Config ] ==================
        if isinstance(config, str):
            config = OmegaConf.load(config)
        elif isinstance(config, dict):
            config = OmegaConf.create(config)
        elif isinstance(config, (OmegaConf, DictConfig)):
            pass
        else:
            raise TypeError(f"config should be str, dict, or OmegaConf but got {type(config)}")

        # ================ [ Set Spark ] ===================
        spark_config = config.spark

        # FIXME: Temp spark initialization
        # TODO: Initialize spark depends on configuration
        spark = SparkSession.builder \
            .master('local[20]') \
            .appName(spark_config.appname) \
            .config("spark.driver.memory", spark_config.driver.memory) \
            .getOrCreate()

        # ================= [ Run ETL ] ====================
        # [ Load RDD/DataFrame ] - data ingestion
        # [ Preprocessing ]
        # [ Save RDD/DataFrame ] - data load
        etl_configs = config.etl
        total_etl_n = len(etl_configs)
        for etl_i, etl_config in enumerate(etl_configs):
            # etl_config.name format
            # =====>[ etl_cate___etl_sub_cate___etl_name ]
            etl_category = etl_config.name.split('___')[0]
            etl_class = self.registry.get(key=etl_config.name)
            
            # instantiate etl class
            etl_instance = etl_class()

            # loading data MUST be the first ETL process
            if etl_i == 0 and etl_category != 'data_ingestion':
                raise ValueError(f"First ETL process should be data ingestion but got {etl_category}")

            # saving data MUST be the last ETL process
            if etl_i == total_etl_n - 1 and etl_category != 'data_load':
                raise ValueError(f"Last ETL process should be data load but got {etl_category}")

            # for first ETL process w/ data ingestion, `spark` is required
            # for the rest, even data ingestion, it is about reformating data
            # so treated as other ETL process
            if etl_i == 0 and etl_category == 'data_ingestion':
                data = etl_instance(spark, **etl_config.args)
            else:
                data = etl_instance(data, **etl_config.args)

        # =============== [ Stop Spark ] ==================
        spark.stop()

        return data