
"""
base class to support the registration of the ETL classes
"""

import os
import abc
import importlib.util
from pyspark.rdd import RDD


# TODO: If you add category directories, add them here too
# _sample is a special directory that is not imported
ETL_CATEGORIES = [
    'data_ingestion',
    'decontamination',
    'deduplication',
    'bias',
    'toxicity',
    'junk',
    'pii',
    'quality',
    'data_load',
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
        self._registry = {}
        self._initialized = True

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
            registry = ETLRegistry()
            registry.register(key=name, etl=new_class)

        return new_class


class BaseETL(ETLStructure, metaclass=ETLAutoRegistry):
    """
    spark ETL
    """
    @abc.abstractmethod
    def run(self, rdd: RDD, config: dict = None, *args, **kwargs):
        """
        run the preprocessing
        """
        raise NotImplementedError()

    def __call__(self, rdd: RDD, config: dict = None, *args, **kwargs):
        """
        call the method to do the preprocessing
        """
        return self.run(rdd, config, *args, **kwargs)


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
    """
    # i know using class name without snake case is awkward
    # but i want to keep the class name as it is and user won't know it
    etl_cls = type(func.__name__, (BaseETL,), {"run": add_self(func)})

    return etl_cls