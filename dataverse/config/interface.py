"""
Interface to check & load the configurations for installation environment

awesome_config = Config.load("/path/to/ducky_awesome_config.yaml")
awesome_config = Config.load({awesome: config})
"""

import re
import boto3

from pathlib import Path
from typing import Union
from omegaconf import OmegaConf
from omegaconf import DictConfig

from dataverse.utils.setting import SystemSetting
from dataverse.utils.api import aws_s3_read
from pathlib import Path



class Config:
    """
    Interface to check & load the configurations
    lightweight wrapper for OmegaConf
    """
    def __new__(cls, *args, **kwargs):
        raise NotImplementedError("Config is not allowed to be instantiated")

    @classmethod
    def load(cls, config: Union[str, dict, DictConfig, OmegaConf, Path]):
        """
        config (Union[str, dict, OmegaConf]): config for the etl
            - str or Path: (this could has several cases)
                - path to the config file
                - s3 path to the config file
                - config string
                    - this is like when you load `yaml` file with open()
                        config = yaml.load(f)
            - dict: config dict
            - OmegaConf: config object
        """
        if isinstance(config, (str, Path)):
            if isinstance(config, Path):
                config = str(config)

            # Local File
            if Path(config).is_file():
                config = OmegaConf.load(config)

            # AWS S3
            elif config.startswith(('s3://', 's3a://', 's3n://')):
                aws_s3_matched = re.match(r's3[a,n]?://([^/]+)/(.*)', config)
                if aws_s3_matched:
                    bucket, key = aws_s3_matched.groups()
                    config_content = aws_s3_read(bucket, key)
                    config = OmegaConf.create(config_content)
                else:
                    # assume it's a config string that starts with s3
                    config_str = config
                    config = OmegaConf.create(config_str)

                    # check if it's config string or not
                    # in case of config string it should create a config object
                    # if not, it will create {'config': None}
                    if config_str in config and config[config_str] is None:
                        raise ValueError(f"config {config_str} is not a valid s3 path")
            
            # String Config
            else:
                # assume it's a config string
                config_str = config
                config = OmegaConf.create(config_str)

                # same as above, check if it's config string or not
                if config_str in config and config[config_str] is None:
                    raise ValueError(f"config {config_str} is not a valid path")

        elif isinstance(config, dict):
            config = OmegaConf.create(config)
        elif isinstance(config, (OmegaConf, DictConfig)):
            pass
        else:
            raise TypeError(f"config should be str, dict, or OmegaConf but got {type(config)}")

        return config

    @classmethod
    def save(cls, config, path: Union[str, Path]):
        OmegaConf.save(config, Path(path))

    @classmethod
    def default(cls):
        """
        fill the missing config with default
        """
        local_dir = f"{SystemSetting().CACHE_DIR}/.cache/dataverse/tmp"

        default = OmegaConf.create({
            'spark': {
                'master': 'local[10]',
                'appname': 'default',
                'driver': {'memory': '8G'},
                'executor': {'memory': '1G'},
                'local': {'dir': local_dir},
                'ui': {'port': 4040},
            },
            'etl': []
        })

        return default

    @classmethod
    def set_default(cls, config: OmegaConf):
        """
        set the missing config args with default
        """
        return OmegaConf.merge(cls.default(), config)
