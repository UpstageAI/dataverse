"""
Base class to support the registration of the ETL classes

Copyright (c) 2024-present Upstage Co., Ltd.
Apache-2.0 license
"""

import abc
import importlib.util
import inspect
import os
from functools import wraps
from typing import Union

from pyspark.rdd import RDD
from pyspark.sql import DataFrame

from dataverse.utils.setting import SystemSetting

# TODO: If you add category directories, add them here too
# _sample is a special directory that is not imported

# This is where you choose what categories to register
ETL_CATEGORIES = [
    "data_ingestion",
    "decontamination",
    "deduplication",
    "bias",
    "toxicity",
    "cleaning",
    "pii",
    "quality",
    "data_load",
    "utils",
]

IGNORE_FILES = [
    "__init__.py",
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
        files = [f for f in files if f.endswith(".py")]

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
class ETLStructure:
    ...


class ETLRegistry:
    """Singleton class to register the ETL classes.

    This class provides a registry for ETL classes.
    It ensures that only one instance of the registry is created and provides
    methods to register, search, and retrieve ETL classes.

    Attributes:
        _initialized (bool): Flag to check if the class has been initialized.
        _registry (dict): Dictionary to store the registered ETL classes.
        _status (dict): Dictionary to store the status of the registered ETL classes.

    Methods:
        __new__(): Creates a new instance of the class if it doesn't exist.
        __init__(): Initializes the class and registers the ETL classes.
        __len__(): Returns the number of registered ETL classes.
        __repr__(): Returns a string representation of the registry.
        __str__(): Returns a string representation of the registry.
        reset(): Resets the registry.
        register(key, etl): Registers an ETL class with a given key.
        _update_status(key): Updates the status of the registry.
        search(category, sub_category): Searches for ETL classes based on category and sub-category.
        _convert_to_report_format(status, print_sub_category, print_etl_name): Converts the status to a report format.
        get(key): Retrieves an ETL class based on the key.
        get_all(): Retrieves all the registered ETL classes.
    """

    _initialized = False

    def __new__(cls):
        if not hasattr(cls, "instance"):
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
        Registers the ETL (Extract, Transform, Load) process.

        Args:
            key (str): The key used to identify the ETL process. Should be in the format below:
            etl (ETLStructure): The ETL process to be registered. It should be a subclass of ETLStructure.

        Raises:
            ValueError: If the key is not all lowercase, not separated by '___', or does not have 2 layers of category.
            TypeError: If the ETL class is not a subclass of ETLStructure.
            KeyError: If the key is already registered.

        Note:
            The key should be in the format of:
            - all lowercase
            - separated by ___
            - it should have 2 layers of category
            - Example: <etl_type>___<file_key>___<etl_key> or <category>___<sub_category>___<etl_key>.
        """
        if not key.islower():
            raise ValueError(f"The key [ {key} ] should be all lowercase")
        if "___" not in key:
            raise ValueError(f"The key [ {key} ] should be separated by ___")
        if len(key.split("___")) != 3:
            raise ValueError(f"The key [ {key} ] should have 2 layers of category")

        # all the etl should be the subclass of ETLStructure
        if not issubclass(etl, ETLStructure):
            raise TypeError(f"ETL class should be subclass of ETLStructure not {etl}")

        # register
        if key in self._registry:
            if (os.getenv("DATAVERSE_TEST_MODE") == "True") or (
                os.getenv("DATAVERSE_BUILD_DOC") == "true"
            ):
                pass
            else:
                raise KeyError(f"The key [ {key} ] is already registered")
        else:
            self._registry[key] = etl
            self._update_status(key=key)

    def _update_status(self, key: str):
        category, sub_category, _ = key.split("___")
        if category not in self._status:
            self._status[category] = {}

        if sub_category not in self._status[category]:
            self._status[category][sub_category] = [key]
        else:
            self._status[category][sub_category].append(key)

    def search(self, category: str = None, sub_category: str = None):
        """
        Search the ETL.

        Args:
            category (str, optional): The category to search for. Defaults to None.
            sub_category (str, optional): The sub-category to search for. Defaults to None.

        Returns:
            dict: A dictionary containing the filtered status information.

        Raises:
            AssertionError: If category is a list or not a string.
            AssertionError: If sub_category is a list or not a string.
            ValueError: If sub_category is specified without category.

        Note:
            - Printing all the information is fixed as default.
            - Set print_sub_category to True to print the sub-category.
            - Set print_etl_name to True to print the ETL name.
        """
        status = self._status

        filtered_status = {}
        if category is not None:
            assert type(category) != list, "we do not support list search for category"
            assert type(category) == str, "category must be a string"
            if sub_category is None:
                filtered_status[category] = status[category]
            else:
                assert type(sub_category) != list, "we do not support list search for sub-category"
                assert type(sub_category) == str, "sub_category must be a string"
                filtered_status[category] = {sub_category: status[category][sub_category]}
        else:
            if sub_category is not None:
                raise ValueError("sub-category cannot be specified without category")
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
                stats[category]["__total__"] = 0

            sub_categories = list(status[category].keys())
            for sub_category in sub_categories:
                sub_n = len(status[category][sub_category])
                stats[category][sub_category] = sub_n
                stats[category]["__total__"] += sub_n
                total += sub_n

        # convert to the report format
        infos = []

        infos.append("=" * 50)
        infos.append(f"Total [ {total} ]")
        infos.append("=" * 50)

        for category in categories:
            infos.append(f"{category} [ {stats[category]['__total__']} ]")
            sub_categories = list(status[category].keys())

            if print_sub_category:
                for sub_category in sub_categories:
                    infos.append(f"{' ' * 4}- {sub_category} [ {stats[category][sub_category]} ]")

                    if print_etl_name:
                        for etl in status[category][sub_category]:
                            infos.append(f"{' ' * 8}- {etl}")

        return "\n".join(infos)

    def get(self, key: str) -> ETLStructure:
        """
        Retrieves the ETLStructure associated with the given key.

        Args:
            key (str): The key used to retrieve the ETLStructure. Should be in the format below.

        Returns:
            ETLStructure: The ETLStructure associated with the given key.

        Raises:
            ValueError: If the key is not all lowercase, not separated by '___', or does not have 2 layers of category.
            KeyError: If the key is not registered in the registry.

        Note:
             The key should be in the format of:
            - all lowercase
            - separated by ___
            - it should have 2 layers of category
            - Example: <etl_type>___<file_key>___<etl_key> or <category>___<sub_category>___<etl_key>.
        """
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
        if name != "BaseETL":
            if "__file_path__" not in attrs:
                raise TypeError(
                    "Direct inheritance from BaseETL not allowed. Use @register_etl decorator."
                )

            registry = ETLRegistry()
            registry.register(key=name, etl=new_class)

        return new_class


class BaseETL(ETLStructure, metaclass=ETLAutoRegistry):
    """
    Base class for spark ETL.

    This class provides a base structure for implementing spark ETL processes.
    If you need to use `self` directly, inherit this class.

    Attributes:
        None

    Methods:
        run(data: Union[RDD, DataFrame], *args, **kwargs):
            Run the preprocessing logic.
            This method should be implemented by subclasses.

        __call__(*args, **kwargs):
            Call the `run` method to perform the preprocessing.
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

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        return func(*args, **kwargs)

    wrapper.__doc__ = func.__doc__
    return wrapper


def register_etl(func):
    """
    Decorator to register a function as an ETL.

    Args:
        func (callable): The function to be registered as an ETL.

    Returns:
        type: A dynamically created class that inherits from BaseETL and wraps the original function.

    Raises:
        None.

    About Attributes:
        - __file_path__ (str): The file path of the function where it is defined.
        - __etl_dir__ (bool): If the file is in the etl directory. If not, it means it's a dynamically registered user-defined ETL.

    Example:
        >>> @register_etl
        >>> def my_etl_function():
        >>>    pass

    Note:
        The registered ETL function should not rely on the `self` parameter.

        If you need to use `self`, directly inherit the BaseETL class.
    """
    ETL_DIR = os.path.join(SystemSetting().DATAVERSE_HOME, "etl")
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
        },
    )

    etl_cls.__doc__ = func.__doc__
    etl_cls.__is_etl__ = True
    return etl_cls
