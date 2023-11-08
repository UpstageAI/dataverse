
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

from dataverse.etl import ETLRegistry
from dataverse.etl.registry import auto_register



class ETLPipeline:
    """
    ETL Pipeline
    """
    def __init__(self):
        self.registry = ETLRegistry()

    def get(self, key):
        """get ETL class from registry"""
        return self.registry.get(key=key)

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

        # [switch] is the ETL process ended or not
        # if not, spark session & data will be returned to continue
        IS_ETL_FINISHED = True

        for etl_i, etl_config in enumerate(etl_configs):
            # etl_config.name format
            # =====>[ etl_cate___etl_sub_cate___etl_name ]
            etl_name = etl_config.name
            etl_category = etl_name.split('___')[0]
            etl_class = self.get(key=etl_name)
            
            # instantiate etl class
            etl_instance = etl_class()

            # loading data MUST be the first ETL process
            if etl_i == 0 and etl_category != 'data_ingestion':
                raise ValueError(f"First ETL process should be data ingestion but got {etl_category}")

            # this is middle creator mode
            # if the last ETL process is not data load  
            if etl_i == total_etl_n - 1 and etl_category != 'data_load':
                print((
                    f"{'=' * 50}\n"
                    "[ DEBUG MODE ]\n"
                    f"{'=' * 50}\n"
                    f"Last ETL process was assigned for [ {etl_category} ]\n"
                    "Spark session will not be stopped and will be returned\n"
                    "If this is not intended, please assign [ data_load ] at the end.\n"
                    f"{'=' * 50}\n"
                    "Example:\n"
                    "=> spark, data = etl_pipeline.run(config)\n"
                    "=> data = data.map(add awesome duck to column)\n"
                    f"{'=' * 50}\n"
                ))
                IS_ETL_FINISHED = False

            # when args is not defined, set it to empty dict
            if 'args' in etl_config:
                args = etl_config.args
            else:
                args = {}

            # `etl_name` is passed to args for tracking
            if etl_i == 0 and etl_category == 'data_ingestion':
                data = etl_instance(spark, **args, etl_name=etl_name)
            else:
                data = etl_instance(spark, data, **args, etl_name=etl_name)

        # =============== [ Stop Spark ] ==================
        if IS_ETL_FINISHED:
            spark.stop()
            return data
        else:
            return spark, data