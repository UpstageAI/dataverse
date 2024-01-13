
"""
ETL Interface 
----------------------
user will be interacting with this interface
"""

import time
import boto3

from typing import Union
from pathlib import Path
from omegaconf import OmegaConf
from omegaconf import DictConfig

from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

from dataverse.config import Config
from dataverse.etl import ETLRegistry
from dataverse.utils.setting import SystemSetting
from dataverse.utils.api import aws_check_credentials
from dataverse.utils.api import EMRManager
from dataverse.utils.api import AWSClient


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

    def setup_spark_conf(self, config, verbose=False):
        """
        AWS credential setting log is not influenced by the verbose by design
        """

        # TODO: add more spark configurations
        spark_conf = SparkConf()
        spark_conf.set('spark.master', config.spark.master)
        spark_conf.set('spark.app.name', config.spark.appname)
        spark_conf.set('spark.driver.memory', config.spark.driver.memory)
        spark_conf.set('spark.driver.maxResultSize', config.spark.driver.maxResultSize)
        spark_conf.set('spark.executor.memory', config.spark.executor.memory)
        spark_conf.set('spark.local.dir', config.spark.local.dir)
        spark_conf.set('spark.ui.port', config.spark.ui.port)

        # AWS S3 Support
        if aws_check_credentials(verbose=verbose):
            session = boto3.Session()
            credentials = session.get_credentials()

            spark_conf.set('spark.hadoop.fs.s3a.access.key', credentials.access_key)
            spark_conf.set('spark.hadoop.fs.s3a.secret.key', credentials.secret_key)
            spark_conf.set('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')

            hadoop_ver = SystemSetting().get('HADOOP_VERSION')
            spark_conf.set(
                'spark.jars.packages',
                (
                    f'org.apache.hadoop:hadoop-aws:{hadoop_ver}'
                    f',com.amazonaws:aws-java-sdk-bundle:1.12.592'
                )
            )

            # check if the credentials are temporary or not
            try:
                spark_conf.set('spark.hadoop.fs.s3a.session.token', credentials.token)
                spark_conf.set(
                    'spark.hadoop.fs.s3a.aws.credentials.provider',
                    "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"
                ) # this is for temporary credentials
                print('spark conf is set with [ temporary ] S3 credentials')
            except:
                print('spark conf is set with [ permanent ] S3 credentials')

        else:
            print('[ No AWS Credentials Found] - Failed to set spark conf for S3')

        return spark_conf


    def sample(
        self,
        n=100,
        config=None,
        sample_etl="data_ingestion___test___generate_fake_ufl",
        verbose=False,
    ):
        """
        get spark session and sample data

        use this function to test the ETL pipeline quickly without config

        args:
            n (int): the number of data to generate
            config (Union[str, dict, OmegaConf]): config for the etl
                - config is not necessary
            sample_etl (str): the name of the sample ETL process
            verbose (bool): if True, print the status
        """
        if config is None:
            config = Config.default()
        else:
            config = Config.load(config)
            config = Config.set_default(config)

            # remove all the ETL processes
            config.etl = []

        config.etl.append({'name': sample_etl, 'args': {'n': n}})
        if verbose:
            print('=' * 50)
            print('[ Configuration ]')
            print(OmegaConf.to_yaml(config))
            print('=' * 50)

        spark_conf = self.setup_spark_conf(config, verbose=verbose)
        spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
        if verbose:
            print('=' * 50)
            print('[ Spark Final Configuration ]')
            print(OmegaConf.to_yaml(spark_conf.getAll()))
            print('=' * 50)

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
        cache=False,
        emr=False,
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
            cache (bool): cache every stage of the ETL process
            emr (bool): if True, run the ETL process on EMR
        """
        # ================ [ EMR ] ===================
        if emr:
            return self.run_emr(
                config,
                verbose=verbose,
                cache=cache,
                *args,
                **kwargs,
            )

        # =============== [ Set Config ] ==================
        # mainly this is to fill the missing config args with default
        config = Config.load(config)
        config = Config.set_default(config)
        if verbose:
            print('=' * 50)
            print('[ Configuration ]')
            print(OmegaConf.to_yaml(config))
            print('=' * 50)

        # ================ [ Set Spark ] ===================
        spark_conf = self.setup_spark_conf(config, verbose=verbose)
        spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
        if verbose:
            print('=' * 50)
            print('[ Spark Final Configuration ]')
            print(OmegaConf.to_yaml(spark_conf.getAll()))
            print('=' * 50)

        # ================= [ Run ETL ] ====================
        # [ Load RDD/DataFrame ] - data ingestion
        # [ Preprocessing ]
        # [ Save RDD/DataFrame ] - data load
        etl_configs = config.etl
        total_etl_n = len(etl_configs)

        # [switch] is the ETL process ended or not
        # if not, spark session & data will be returned to continue
        IS_ETL_FINISHED = True

        data = None
        prev_etl_name = None
        prev_data = None # for caching
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
                data = etl_instance(spark, **args, etl_name=etl_name, prev_etl_name=None)
            else:
                data = etl_instance(spark, data, **args, etl_name=etl_name, prev_etl_name=prev_etl_name)

            # cache the data
            if cache:
                if prev_data is not None:
                    prev_data.unpersist()
                data.cache()
                prev_data = data

            prev_etl_name = etl_name


        # =============== [ Stop Spark ] ==================
        if IS_ETL_FINISHED:
            spark.stop()
            if verbose:
                print('=' * 50)
                print('[ Spark Successfully Done ]')
                print('=' * 50)

        return spark, data

    def run_emr(
        self,
        config: Union[str, dict, DictConfig, OmegaConf, Path],
        verbose=False,
        cache=False,
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
            cache (bool): cache every stage of the ETL process

        Returns:
            None, Config:
                - None for spark session
                - Config for the config
                    - originally data is returned, but it is not necessary for EMR
                    - so rather data, config is returned
        """
        if not aws_check_credentials(verbose=verbose):
            raise ValueError('AWS EMR requires AWS credentials')

        # =============== [ Set Config ] ==================
        config = Config.load(config)
        config = Config.set_default(config, emr=True)

        # EMR resource manager - yarn
        config.spark.master = 'yarn'

        # reset local_dir for EMR cluster
        config.spark.local.dir = '/tmp'

        # ================ [ EMR ] ===================
        # NOTE: config will be auto-updated by EMR Manager
        emr_manager = EMRManager()

        try:
            # EMR cluster launch
            emr_manager.launch(config)

            if verbose:
                print('=' * 50)
                print('[ Configuration ]')
                print(OmegaConf.to_yaml(config))
                print('=' * 50)

            # EMR cluster environment setup & run spark
            step_id = emr_manager.run(config, verbose=verbose)

            # wait until EMR cluster step is done
            emr_manager.wait(config, step_id)

            # EMR Cluster terminate
            # XXX: after EMR cluster is terminated, and confirmed by waiter
            #      there is still a chance that the cluster is not terminated and cause error
            #       - DependencyViolation (which depends on terminated cluster)
            # FIXME: this is a temporary solution, need to find a better way to handle this
            RETRY_TERMINATE = 5
            for _ in range(RETRY_TERMINATE):
                try:
                    emr_manager.terminate(config)
                    break
                except AWSClient().ec2.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == 'DependencyViolation':
                        print('DependencyViolation - retrying to terminate EMR cluster')
                        time.sleep(5)
                    else:
                        raise e
                except Exception as e:
                    raise e

        # ctrl + c
        except KeyboardInterrupt:
            print('KeyboardInterrupt - terminating EMR cluster')
            emr_manager.terminate(config)
            raise KeyboardInterrupt
        except Exception as e:
            print('Exception - terminating EMR cluster')
            emr_manager.terminate(config)
            raise e

        return None, config