

from pyspark.rdd import RDD
from pyspark.sql import DataFrame

from dataverse.etl import BaseETL
from dataverse.etl import register_etl
from dataverse.etl import ETLRegistry
from dataverse.etl.registry import ETLStructure

from typing import Union


@register_etl
def __sample___github___using_decorator(data: Union[RDD, DataFrame], *args, **kwargs):
    """
    decorator will convert this function to BaseETL class
    """
    print("sample using decorator")
    return data

@register_etl
def __sample___github___config(data: Union[RDD, DataFrame], config: dict = None, *args, **kwargs):
    """
    decorator will convert this function to BaseETL class
    """
    print("config says", config)
    return data

if __name__ == "__main__":
    registry = ETLRegistry()

    print("[ Testing ] registry etl using decorator")
    # this could seem like a function but it is actually a BaseETL class
    etl = __sample___github___using_decorator
    etl()(data=None)
    print("is subclass of ETLStructure?", issubclass(etl, ETLStructure), "\n")

    print("[ Testing ] registry etl using decorator with config")
    etl = __sample___github___config
    etl()(data=None, config={"hello": "world"})
    print("is subclass of ETLStructure?", issubclass(etl, ETLStructure), "\n")

    # check is it properly registryed
    print("[ Testing ] check is it properly registry")
    print("="*50)
    print(registry._registry)
    print("="*50)