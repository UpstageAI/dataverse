
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



class ETLInterface:
    """
    ETL Interface
    """
    def __init__(self):
        self.registry = ETLRegistry()

        # reset registry to avoid duplicate registration
        self.registry.reset()
        auto_register()

    def run(self, config: Union[str, dict, OmegaConf], *args, **kwargs):
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
        for etl_config in etl_configs:
            # etl_config.name format
            # =====>[ etl_cate___etl_sub_cate___etl_name ]
            etl_cate = etl_config.name.split('___')[0]
            etl_class = self.registry.get(key=etl_config.name)
            
            # instantiate etl class
            etl_instance = etl_class()

            # for data ingestion specially, `spark` is required
            if etl_cate == 'data_ingestion':
                data = etl_instance(spark, **etl_config.args)
            else:
                data = etl_instance(data, **etl_config.args)

        # data load
        ...

        # =============== [ Stop Spark ] ==================
        spark.stop()

        return data