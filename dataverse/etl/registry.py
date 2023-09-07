
"""
base class to support the registration of the ETL classes
"""

import sys
import inspect
import abc
from pyspark.rdd import RDD


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

    def register(
            self,
            category: str,
            sub_category: str,
            name: str,
            etl: ETLStructure
        ):
        """
        register the etl
        """
        key = f"{category}___{sub_category}___{name}"

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
        # if "___" not in key:
        #     raise ValueError(f"The key [ {key} ] should be separated by ___")
        # if len(key.split("___")) != 3:
        #     raise ValueError(f"The key [ {key} ] should have 2 layers of category")

        # all the etl should be the subclass of ETLStructure
        if not issubclass(etl, ETLStructure):
            raise TypeError(f"ETL class should be subclass of ETLStructure not {etl}")

        # register already exists
        if key in self._registry:
            raise KeyError(f"The key [ {key} ] is already registered")

        self._registry[key] = etl

    def get(
            self,
            category: str,
            sub_category: str,
            name: str,
        ) -> ETLStructure:
        """
        get the etl
        """
        key = f"{category}___{sub_category}___{name}"

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
        # if "___" not in key:
        #     raise ValueError(f"The key [ {key} ] should be separated by ___")
        # if len(key.split("___")) != 3:
        #     raise ValueError(f"The key [ {key} ] should have 2 layers of category")

        if key not in self._registry:
            raise KeyError(f"The key {key} is not registered")

        return self._registry[key]


def new_getfile(object, _old_getfile=inspect.getfile):
    """
    https://stackoverflow.com/questions/51566497/getting-the-source-of-an-object-defined-in-a-jupyter-notebook
    This is a HACK to get the source code of a function defined in a Jupyter notebook.
    """
    if not inspect.isclass(object):
        return _old_getfile(object)
    
    # Lookup by parent module (as in current inspect)
    if hasattr(object, '__module__'):
        object_ = sys.modules.get(object.__module__)
        if hasattr(object_, '__file__'):
            return object_.__file__
    
    # If parent module is __main__, lookup by methods (NEW)
    for name, member in inspect.getmembers(object):
        if inspect.isfunction(member) and object.__qualname__ + '.' + member.__name__ == member.__qualname__:
            return inspect.getfile(member)
    else:
        raise TypeError('Source for {!r} not found'.format(object))

inspect.getfile = new_getfile

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

            # category/sub_category.py/def name():
            filename = inspect.getfile(new_class)
            category = filename.split("/")[-2]
            sub_category = filename.split("/")[-1].split(".")[0]

            registry.register(
                category=category,
                sub_category=sub_category,
                name=name,
                etl=new_class
            )

        return new_class


class BaseETL(ETLStructure, metaclass=ETLAutoRegistry):
    """
    spark ETL
    """
    @abc.abstractmethod
    def run(self, rdd: RDD, *args, **kwargs):
        """
        run the preprocessing
        """
        raise NotImplementedError()

    def __call__(self, rdd: RDD, *args, **kwargs):
        """
        call the method to do the preprocessing
        """
        return self.run(rdd, *args, **kwargs)


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