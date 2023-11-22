
"""
Interface for system setting
"""

import os
import json
from pathlib import Path
from pyspark import SparkContext, SparkConf

import dataverse


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

    def default_setting(self):
        """
        Reset the system setting to default

        Default setting:
        - `CACHE_DIR`: default cache directory
        - `IS_CLI`: if the program is running in CLI mode
        """
        self.system_setting = {}

        # DATAVERSE
        self.DATAVERSE_HOME = os.path.dirname(dataverse.__file__)

        # HARD CODED DEFAULT SETTING
        self.CACHE_DIR = Path.home().as_posix()
        self.IS_CLI = False

        # SPARK VERSION
        conf = SparkConf()
        sc = SparkContext(conf=conf)
        self.SPARK_VERSION = sc.version
        self.HADOOP_VERSION = sc._jvm.org.apache.hadoop.util.VersionInfo.getVersion()
        sc.stop()


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
