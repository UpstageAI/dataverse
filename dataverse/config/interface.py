"""
Interface to check & load the configurations for installation environment

awesome_config = Config.load("/path/to/ducky_awesome_config.yaml")
awesome_config = Config.load({awesome: config})
"""

from pathlib import Path
from typing import Union
from omegaconf import OmegaConf
from omegaconf import DictConfig

from dataverse.utils.setting import SystemSetting
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
            - str: path to the config file
            - dict: config dict
            - OmegaConf: config object
        """
        if isinstance(config, str):
            config = OmegaConf.load(config)
        elif isinstance(config, Path):
            config = OmegaConf.load(config)
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
