
"""
ETL Interface 
----------------------
user will be interacting with this interface
"""

from typing import Union
from pathlib import Path
from omegaconf import OmegaConf
from omegaconf import DictConfig

from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

from dataverse.config import Config
from dataverse.etl import ETLRegistry


class ETLPipeline:
    """
    ETL Pipeline
    """
    def __init__(self):
        self.registry = ETLRegistry()

    def __len__(self):
        return len(self.registry)

    def status(self):
        """
        get the status of the registry. don't show you detailed information

        [ info type ]
        - category

        usage:
            >>> etl_pipeline.status()
        """
        print("If you need details of ETL Registry use `etl_pipeline.search()`")
        return str(self.registry)

    def search(self, category=None, sub_category=None):
        """
        get detailed status of the registry by searching

        [ info type ]
        - category
        - sub_category
        - etl_name

        usage:
            # return every etl
            >>> etl_pipeline.search()

            # only selected category
            >>> etl_pipeline.search('data_ingestion')
            >>> etl_pipeline.search(category='data_ingestion')

            # only selected category & sub_category
            >>> etl_pipeline.search('data_ingestion', 'ufl')
            >>> etl_pipeline.search(category='data_ingestion', sub_category='ufl')
        """
        return self.registry.search(category=category, sub_category=sub_category)

    def get(self, key):
        """get ETL class from registry"""
        return self.registry.get(key=key)

    def sample(
        self,
        n=100,
        sample_etl="data_ingestion___test___generate_fake_ufl",
        verbose=False,
    ):
        """
        get spark session and sample data

        use this function to test the ETL pipeline quickly without config

        args:
            n (int): the number of data to generate
            sample_etl (str): the name of the sample ETL process
            verbose (bool): if True, print the status
        """
        config = Config.default()
        config.etl.append({'name': sample_etl, 'args': {'n': n}})

        spark = SparkSession.builder \
            .master(config.spark.master) \
            .appName('sample') \
            .config("spark.driver.memory", config.spark.driver.memory) \
            .getOrCreate()

        sample_etl_class = self.get(key=sample_etl)
        data = sample_etl_class()(spark, n=n, etl_name=sample_etl)

        if verbose:
            print((
                f"{'=' * 50}\n"
                "[ SAMPLE MODE ]\n"
                f"{'=' * 50}\n"
                "This is a quick way to get the sample data for testing or debugging w/o config.\n"
                "If you want to test the ETL pipeline with your own data, please use `run` w/ config.\n"
                f"{'=' * 50}\n"
                "=> spark, data = etl_pipeline.sample()\n"
                "=> data = data.map(add awesome duck to column)\n"
                f"{'=' * 50}\n"
            ))

        return spark, data


    def run(
        self,
        config: Union[str, dict, DictConfig, OmegaConf, Path],
        verbose=False,
        *args,
        **kwargs,
    ):
        """
        Args:
            config (Union[str, dict, OmegaConf]): config for the etl
                - str: path to the config file
                - dict: config dict
                - OmegaConf: config object
            verbose (bool): if True, print the status of the etl pipeline
                - the verbose will be applied to the ETL process as well
                - ETL process `verbose` takes precedence over this
        """
        # =============== [ Set Config ] ==================
        # mainly this is to fill the missing config args with default
        config = Config.load(config)
        config = Config.set_default(config)

        # ================ [ Set Spark ] ===================
        # TODO: add more spark configurations
        spark = SparkSession.builder \
            .master(config.spark.master) \
            .appName(config.spark.appname) \
            .config("spark.driver.memory", config.spark.driver.memory) \
            .config("spark.executor.memory", config.spark.executor.memory) \
            .config("spark.local.dir", config.spark.local.dir) \
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

            # this is middle creator mode
            # if the last ETL process is not data load  
            if etl_i == total_etl_n - 1 and etl_category != 'data_load':
                if verbose:
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

            # if verbose is not defined, set it same to the pipeline
            if 'verbose' not in args:
                args['verbose'] = verbose

            # `etl_name` is passed to args for tracking
            if etl_i == 0:
                data = etl_instance(spark, **args, etl_name=etl_name)
            else:
                data = etl_instance(spark, data, **args, etl_name=etl_name)

        # =============== [ Stop Spark ] ==================
        if IS_ETL_FINISHED:
            spark.stop()

        return spark, data