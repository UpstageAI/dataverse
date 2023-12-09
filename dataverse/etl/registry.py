
"""
base class to support the registration of the ETL classes
"""

import os
import abc
import inspect
import importlib.util

from pyspark.rdd import RDD
from pyspark.sql import DataFrame

from typing import Union
from dataverse.utils.setting import SystemSetting


# TODO: If you add category directories, add them here too
# _sample is a special directory that is not imported

# This is where you choose what categories to register
ETL_CATEGORIES = [
    'data_ingestion',
    'decontamination',
    'deduplication',
    'bias',
    'toxicity',
    'cleaning',
    'pii',
    'quality',
    'data_load',
    'utils',
]

IGNORE_FILES = [
    '__init__.py',
]

def auto_register(etl_categories=ETL_CATEGORIES):
    """
    This will automatically register all ETLs to the registry
    """
    etl_path = os.path.dirname(os.path.abspath(__file__))
    for etl_category in etl_categories:
        
        # Get the files(sub-categories) in the category
        category_path = os.path.join(etl_path, etl_category)
        files = os.listdir(category_path)

        # Filter out non-Python files
        files = [f for f in files if f.endswith('.py')]

        # Dynamically import all Python files in the directory
        for file in files:
            if file in IGNORE_FILES:
                continue

            file_path = os.path.join(category_path, file)

            # Remove .py at the end
            module_name = file[:-3]

            spec = importlib.util.spec_from_file_location(module_name, file_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)


# To avoid circular dependency
class ETLStructure: ...


class ETLRegistry:
    """Singleton class to register the ETL classes"""
    _initialized = False

    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(ETLRegistry, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        """
        when the class is initialized, this is called everytime
        regardless of the singleton. So adding the flag to check
        """
        if self._initialized:
            return
        self._initialized = True
        self._registry = {}
        self._status = {}
        auto_register()


    def __len__(self):
        return len(self._registry.keys())

    def __repr__(self):
        return self._convert_to_report_format(self._status)

    def __str__(self):
        return self.__repr__()

    def reset(self):
        """
        reset the registry
        """
        self._registry = {}

    def register(self, key: str, etl: ETLStructure):
        """
        register the etl
        """
        # the key should be the format of the following
        # ==================================================
        # <etl_type>___<file_key>___<etl_key>
        # <category>___<sub_category>___<etl_key>
        # ==================================================
        # 1. is it all lowercase
        # 2. is it separated by ___
        # 3. it should have 2 layers of category
        if not key.islower():
            raise ValueError(f"The key [ {key} ] should be all lowercase")
        if "___" not in key:
            raise ValueError(f"The key [ {key} ] should be separated by ___")
        if len(key.split("___")) != 3:
            raise ValueError(f"The key [ {key} ] should have 2 layers of category")

        # all the etl should be the subclass of ETLStructure
        if not issubclass(etl, ETLStructure):
            raise TypeError(f"ETL class should be subclass of ETLStructure not {etl}")

        # register already exists
        if key in self._registry:
            raise KeyError(f"The key [ {key} ] is already registered")

        self._registry[key] = etl
        self._update_status(key=key)

    def _update_status(self, key: str):
        category, sub_category, _ = key.split('___')
        if category not in self._status:
            self._status[category] = {}

        if sub_category not in self._status[category]:
            self._status[category][sub_category] = [key]
        else:
            self._status[category][sub_category].append(key)

    def search(self, category=None, sub_category=None):
        """
        search the etl

        printing all the information is fixed as default
        - print_sub_category: print the sub-category
        - print_etl_name: print the etl name
        """
        status = self._status

        filtered_status = {}
        if category is not None:
            assert type(category) != list, 'we do not support list search for category'
            assert type(category) == str, 'category must be a string'
            if sub_category is None:
                filtered_status[category] = status[category]
            else:
                assert type(sub_category) != list, 'we do not support list search for sub-category'
                assert type(sub_category) == str, 'sub_category must be a string'
                filtered_status[category] = {sub_category: status[category][sub_category]}
        else:
            if sub_category is not None:
                raise ValueError('sub-category cannot be specified without category')
            filtered_status = status

        return self._convert_to_report_format(
            filtered_status,
            print_sub_category=True,
            print_etl_name=True,
        )

    def _convert_to_report_format(
        self,
        status,
        print_sub_category=False,
        print_etl_name=False,
    ):
        """
        convert status to report format

        This includes the number of ETLs in each category and sub-category
        and depending on the options, it can include the name of the ETLs

        Args:
            status (dict): the status from `search`
        """
        # count the number of etls
        stats = {}
        total = 0
        categories = list(status.keys())
        for category in categories:
            if category not in stats:
                stats[category] = {}
                stats[category]['__total__'] = 0

            sub_categories = list(status[category].keys())
            for sub_category in sub_categories:
                sub_n = len(status[category][sub_category])
                stats[category][sub_category] = sub_n
                stats[category]['__total__'] += sub_n
                total += sub_n

        # convert to the report format
        infos = []

        infos.append('=' * 50)
        infos.append(f"Total [ {total} ]")
        infos.append('=' * 50)

        for category in categories:
            infos.append(f"{category} [ {stats[category]['__total__']} ]")
            sub_categories = list(status[category].keys())

            if print_sub_category:
                for sub_category in sub_categories:   
                    infos.append(f"{' ' * 4}- {sub_category} [ {stats[category][sub_category]} ]")

                    if print_etl_name:
                        for etl in status[category][sub_category]:
                            infos.append(f"{' ' * 8}- {etl}")

        return '\n'.join(infos)

    def get(self, key: str) -> ETLStructure:
        """
        get the etl
        """
        # the key should be the format of the following
        # ==================================================
        # <etl_type>___<file_key>___<etl_key>
        # <category>___<sub_category>___<etl_key>
        # ==================================================
        # 1. is it all lowercase
        # 2. is it separated by ___
        # 3. it should have 2 layers of category
        if not key.islower():
            raise ValueError(f"The key [ {key} ] should be all lowercase")
        if "___" not in key:
            raise ValueError(f"The key [ {key} ] should be separated by ___")
        if len(key.split("___")) != 3:
            raise ValueError(f"The key [ {key} ] should have 2 layers of category")

        if key not in self._registry:
            raise KeyError(f"The key {key} is not registered")

        return self._registry[key]

    def get_all(self):
        """
        get all the etls
        """
        return list(self._registry.values())

class ETLAutoRegistry(abc.ABCMeta, type):
    def __new__(cls, name, bases, attrs):
        """
        Metaclass to register the ETL classes automatically to the registry
        """
        # singleton registry
        new_class = super().__new__(cls, name, bases, attrs)

        # BaseETL is base class and should not be registered
        # Another reason is BaseETL is not initialized yet before `__new__` is done but 
        # the registry will verify the class is subclass of BaseETL and raise error
        # because BaseETL is not initialized yet :)
        if name != 'BaseETL':
            if "__file_path__" not in attrs:
                raise TypeError("Direct inheritance from BaseETL not allowed. Use @register_etl decorator.")

            registry = ETLRegistry()
            registry.register(key=name, etl=new_class)

        return new_class


class BaseETL(ETLStructure, metaclass=ETLAutoRegistry):
    """
    spark ETL
    """
    @abc.abstractmethod
    def run(self, data: Union[RDD, DataFrame], *args, **kwargs):
        """
        run the preprocessing
        """
        raise NotImplementedError()

    def __call__(self, *args, **kwargs):
        """
        call the method to do the preprocessing
        """
        return self.run(*args, **kwargs)


def add_self(func):
    """
    Decorator to add self to the function
    intent is to make the function as a method
    """
    def wrapper(self, *args, **kwargs):
        return func(*args, **kwargs)
    return wrapper

def register_etl(func):
    """
    Decorator to register a function as an ETL

    one downside is that the function cannot leverage the `self`
    if you need to use `self` directly inherit the BaseETL

    About Attributes:
    - __file_path__ (str): the file path of the function where it is defined
    - __etl_dir__ (bool): if the file is in the etl directory
        - if not, it means it's dynamically registered user-defined ETL
    """
    ETL_DIR = os.path.join(SystemSetting().DATAVERSE_HOME, 'etl')
    etl_file_path = inspect.getfile(func)

    # I know using class name without snake case is awkward
    # but I want to keep the class name as it is and user won't know it
    etl_cls = type(
        func.__name__,
        (BaseETL,),
        {
            "run": add_self(func),
            "__file_path__": etl_file_path,
            "__etl_dir__": etl_file_path.startswith(ETL_DIR),
        }
    )

    return etl_cls