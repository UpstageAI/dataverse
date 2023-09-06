
"""
base class to support the registration of the ETL classes
"""

import abc
from pyspark.rdd import RDD


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
            registry.register(name, new_class)

        return new_class

class BaseETL(metaclass=ETLAutoRegistry):
    """
    spark ETL
    """
    @abc.abstractmethod
    def run(self, rdd: RDD, **kwargs):
        """
        run the preprocessing
        """
        raise NotImplementedError()

    def __call__(self, rdd: RDD):
        """
        call the method to do the preprocessing
        """
        return self.run(rdd)

class ETLRegistry:
    """Singleton class to register the ETL classes"""
    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(ETLRegistry, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        self._registry = {}

    def register(self, name: str, etl: BaseETL):
        """
        register the etl
        """
        # TODO: check the name of the etl
        # the name should be the format of the following
        # <category_name>___<etl_name>
        # 1. is it all lowercase
        # 2. is it separated by ___
        if not name.islower():
            raise ValueError(f"The name [ {name} ] should be all lowercase")
        if "___" not in name:
            raise ValueError(f"The name [ {name} ] should be separated by ___")

        # all the etl should be the subclass of BaseETL
        if not issubclass(etl, BaseETL):
            raise TypeError(f"ETL class should be subclass of BaseETL not {etl}")

        # register already exists
        if name in self._registry:
            raise KeyError(f"The name [ {name} ] is already registered")

        self._registry[name] = etl

    def get(self, name: str) -> BaseETL:
        """
        get the etl
        """
        if name not in self._registry:
            raise KeyError(f"The name {name} is not registered")

        return self._registry[name]


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