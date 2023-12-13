
"""
Interface for system setting
"""

import os
import re
import uuid
import json
import boto3
import pyspark
from pathlib import Path

import dataverse
from dataverse.utils.api import aws_check_credentials
from dataverse.utils.api import aws_s3_create_bucket
from dataverse.utils.api import aws_s3_list_buckets


class SystemSetting:
    """
    System Setting CRUD interface

    system setting holds all the variables
    that influence the behavior of the dataverse system.

    Also, this class is a singleton class, so you can use it anywhere in the code
    
    [ MEMORY ONLY ]
    - system setting is stored in memory only
    - system setting is not persistent

    [ Update by Env Variable ]
    - system setting can be updated by env variable

    [ Manual Update ]
    - default system setting can be updated manually
    - check `default_setting()`

    [ No Update after Initialization ]
    - system could be updated but not reflected in the program
    - need to restart the program to use new system setting
    """
    # Singleton
    _initialized = False

    # TODO: system setting per user [Candidate]
    ...

    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(SystemSetting, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        # when the class is initialized, this is called everytime
        # regardless of the singleton. So adding the flag to check
        if self._initialized:
            return

        self.default_setting()
        self.update_by_env()
        self._initialized = True

    def _get_aws_bucket(self, verbose=True):
        """
        the bucket will be used to store the dataverse info
        - cache
        - log
        - etc

        - format
            - dataverse-{MAGIC_NUMBER}-{UUID}
        """
        # if aws credential is not valid, return None
        if not aws_check_credentials():
            return None

        identify_prefix = f'dataverse-{self.MAGIC_NUMBER}-'
        for bucket in aws_s3_list_buckets():
            if identify_prefix in bucket:

                # check if the last part is uuid
                uuid_part = bucket.replace(identify_prefix, "")
                try:
                    uuid.UUID(uuid_part)

                    # Use this bucket for your package operations
                    if verbose:
                        print("Detected Dataverse Bucket: " + bucket)

                    return bucket
                except ValueError:
                    # not a valid UUID, so ignore this bucket
                    pass 

        # if there is no relevant bucket, create one
        bucket = f'dataverse-{self.MAGIC_NUMBER}-{uuid.uuid1()}'
        aws_s3_create_bucket(bucket)

        return bucket


    def default_setting(self):
        """
        Reset the system setting to default

        Default setting:
        - `MAGIC_NUMBER`: magic number for dataverse
        - `CACHE_DIR`: default cache directory
        - `IS_CLI`: if the program is running in CLI mode
        - `AWS_BUCKET`: default aws bucket name for dataverse info
        - `SPARK_VERSION`: spark version
        - `HADOOP_VERSION`: hadoop version
        """
        self.system_setting = {}

        # MAGIC NUMBER
        # dv - Dataverse
        # 42 - (The Hitchhiker's Guide to the Galaxy)
        #      The Answer to the Ultimate Question of Life, the Universe, and Everything
        self.MAGIC_NUMBER = "dv42"

        # DATAVERSE
        self.DATAVERSE_HOME = os.path.dirname(dataverse.__file__)

        # HARD CODED DEFAULT SETTING
        self.CACHE_DIR = Path.home().as_posix()
        self.IS_CLI = False

        # AWS SETTING
        self.AWS_BUCKET = self._get_aws_bucket()

        # SPARK VERSION
        self.SPARK_VERSION = pyspark.__version__

        # HADOOP VERSION
        jars = Path(pyspark.__file__).parent / "jars"
        hadoop_jar = list(jars.glob("hadoop-client-runtime*.jar"))
        self.HADOOP_VERSION = re.findall(r"\d+\.\d+\.\d+", hadoop_jar[0].name)[-1]

        # TODO: add more default setting here
        ...

    def update_by_env(self):
        """
        Update the system setting by env variable
        """
        # check if the env variable is set
        for key in self.system_setting:
            if key in os.environ:
                self.system_setting[key] = os.environ[key]

    def check_naming_convention(self, key):
        """
        1. only CAPITALIZED format
            - e.g. CACHE_DIR (O)
            - e.g. cache_dir (X)
        2. only alphanumeric and underscore
            - e.g. CACHE_DIR2 (O)
            - e.g. cache-dir (X)
            - e.g. CACHE_@DIR (X)
        3. only one underscore between words
            - e.g. CACHE__DIR (X)
        4. no underscore at the start/end of the key
            - e.g. _CACHE_DIR (X)
            - e.g. CACHE_DIR_ (X)
        """
        # 1. only CAPITALIZED format
        if key != key.upper():
            raise ValueError(f"key [ {key} ] is not in Capitalized format")

        # 2. only alphanumeric and underscore
        for char in key:
            if not char.isalnum() and char != "_":
                raise ValueError(f"key [ {key} ] should only contains alphanumeric and underscore")

        # 3. only one underscore between words
        if "_" in key:
            # check if there is only one underscore
            divided_keys = key.split("_")
            if "" in divided_keys:
                raise ValueError(f"key [ {key} ] contains more than one underscore")

        # 4. no underscore at the start/end of the key
        if key.startswith("_") or key.endswith("_"):
            raise ValueError(f"key [ {key} ] contains underscore at the start/end of the key")

    def get(self, key):
        """
        """
        if key not in self.system_setting:
            raise KeyError(f"key [ {key} ] does not exist in SYSTEM setting")
        return self.system_setting[key]

    def set(self, key, value):
        """
        """
        self.check_naming_convention(key)
        self.system_setting[key] = value

    # Support dot-like access, e.g. setting.CACHE_DIR
    def __getattr__(self, key):
        if key in [
            "_initialized",
            "system_setting"
        ]:
            return super().__getattr__(key)
        else:
            return self.get(key)

    def __setattr__(self, key, value):
        if key in [
            "_initialized",
            "system_setting"
        ]:
            super().__setattr__(key, value)
        else:
            self.set(key, value)

    # Support dict-like access, e.g. setting["CACHE_DIR"]
    def __getitem__(self, key):
        return self.get(key)

    def __setitem__(self, key, value):
        self.set(key, value)

    def delete(self, key):
        """
        """
        if key in self.system_setting:
            self.system_setting.pop(key, None)
        else:
            raise KeyError(f"key [ {key} ] does not exist in SYSTEM setting")

    def list(self):
        """
        List all settings
        """
        print(self.system_setting)

    def __repr__(self):
        return json.dumps(self.system_setting, indent=4)

    def __str__(self):
        return json.dumps(self.system_setting, indent=4)
