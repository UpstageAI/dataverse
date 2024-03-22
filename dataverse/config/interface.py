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
from dataverse.utils.api import aws_s3_write
from pathlib import Path



class Config:
    """
    Interface to check & load the configurations
    
    This class provides a lightweight wrapper for OmegaConf and allows checking and loading configurations.
    It supports loading configurations from various sources such as files, AWS S3, and config strings.
    The class also provides methods for saving configurations and setting default values for missing config arguments.
    """
    def __new__(cls, *args, **kwargs):
        raise NotImplementedError("Config is not allowed to be instantiated")

    @classmethod
    def load(cls, config: Union[str, dict, DictConfig, OmegaConf, Path]):
        """
        Load the configuration for the etl.

        Args:
            config (Union[str, dict, OmegaConf]): The configuration for the etl.
                - str or Path: This could have several cases:
                    - Path to the config file.
                    - S3 path to the config file.
                    - Config string. This is similar to loading a `yaml` file with `open()`.
                - dict: Config dictionary.
                - OmegaConf: Config object.

        Returns:
            The loaded configuration.

        Raises:
            ValueError: If the provided config is not a valid path or S3 path.
            TypeError: If the provided config is not of type str, dict, or OmegaConf.
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
                    # Assume it's a config string that starts with s3
                    config_str = config
                    config = OmegaConf.create(config_str)

                    # Check if it's a config string or not
                    # In case of a config string, it should create a config object
                    # If not, it will create {'config': None}
                    if config_str in config and config[config_str] is None:
                        raise ValueError(f"config {config_str} is not a valid s3 path")
            
            # String Config
            else:
                # Assume it's a config string
                config_str = config
                config = OmegaConf.create(config_str)

                # Same as above, check if it's a config string or not
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
        """
        Saves the configuration to a specified path.

        Args:
            config: The configuration to be saved.
            path (Union[str, Path]): The path where the configuration should be saved.

        Raises:
            ValueError: If the provided path is not a valid S3 path.
        """
        if path.startswith(('s3://', 's3a://', 's3n://')):
            aws_s3_matched = re.match(r's3[a,n]?://([^/]+)/(.*)', path)
            if aws_s3_matched:
                bucket, key = aws_s3_matched.groups()
                aws_s3_write(bucket, key, config)
            else:
                raise ValueError(f"config path {path} is not a valid s3 path")
        else:
            OmegaConf.save(config, Path(path))

    @classmethod
    def default(cls, emr: bool = False):
        """
        Fill the missing config with default values.

        Args:
            emr (bool, optional): Flag indicating whether the config is for EMR. Defaults to False.

        Returns:
            dict: Default configuration dictionary.
        """
        local_dir = f"{SystemSetting().CACHE_DIR}/.cache/dataverse/tmp"

        default = OmegaConf.create({
            'spark': {
                'master': 'local[10]',
                'appname': 'default',
                'driver': {
                    'memory': '8G',
                    'maxResultSize': '2G',
                },
                'executor': {'memory': '1G'},
                'local': {'dir': local_dir},
                'ui': {'port': 4040},
            },
            'etl': [],
        })

        if emr:
            default.update({
                'emr': {
                    'id': None,
                    'working_dir': None,
                    'name': 'dataverse_emr',
                    'release': 'emr-6.15.0',
                    'idle_timeout': 3600,

                    # master (driver)
                    'master_instance': {
                        'type': None,
                    },

                    # core (data node)
                    'core_instance': {
                        'type': None,
                        'count': 2,
                    },

                    # task (executors)
                    'task_instance': {
                        'type': None,
                        'count': 0,
                    },

                    # EMR cluster created by dataverse or user
                    'auto_generated': None,

                    # iam
                    'role': {
                        'ec2': {
                            'name': None,
                            'policy_arns': None,
                        },
                        'emr': {
                            'name': None,
                            'policy_arns': None,
                        }
                    },
                    'instance_profile': {
                        'name': None,
                        'ec2_role': None,
                    },

                    # TODO: allow more options to customize e.g. cidr, tag, etc.
                    #       but make sure vpc is temporary and not shared
                    'vpc': {
                        'id': None,
                    },
                    'subnet': {
                        'id': None,
                        'public_id': None,
                        'private_id': None,
                        'public': True,
                    },
                    'security_group': {
                        'id': None,
                    },
                    'gateway': {
                        'id': None,
                    },
                    'route_table': {
                        'id': None,
                    },
                    'elastic_ip': {
                        'id': None,
                    },
                    'nat_gateway': {
                        'id': None,
                    },
                }
            })

        return default

    @classmethod
    def set_default(cls, config: OmegaConf, emr: bool = False):
        """
        Sets the missing config arguments with default values.

        Args:
            config (OmegaConf): The configuration object to merge with default values.
            emr (bool, optional): Whether to use EMR configuration. Defaults to False.

        Returns:
            OmegaConf: The merged configuration object.

        """
        return OmegaConf.merge(cls.default(emr=emr), config)
