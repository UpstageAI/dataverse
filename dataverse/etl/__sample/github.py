

from pyspark.rdd import RDD
from dataverse.etl.registry import BaseETL
from dataverse.etl.registry import register_etl
from dataverse.etl.registry import ETLRegistry
from dataverse.etl.registry import ETLStructure


@register_etl
def __sample___github___using_decorator(rdd: RDD, config: dict = None, *args, **kwargs):
    """
    decorator will convert this function to BaseETL class
    """
    print("sample using decorator")
    return rdd

@register_etl
def __sample___github___config(rdd: RDD, config: dict = None, *args, **kwargs):
    """
    decorator will convert this function to BaseETL class
    """
    print("config says", config)
    return rdd

class __sample___github___inheriting_base_etl(BaseETL):
    """
    Inheriting BaseETL class will automatically register the class to the registry
    but you must overwrite run method unless you will raise NotImplementedError
    decorator `register_etl` will do this automatically for you
    """
    def run(self, rdd: RDD, config: dict = None, *args, **kwargs):
        print("sample inheriting base etl run")
        return rdd


if __name__ == "__main__":
    registry = ETLRegistry()

    print("[ Testing ] registry etl using decorator")
    # this could seem like a function but it is actually a BaseETL class
    etl = __sample___github___using_decorator
    etl()(rdd=None)
    print("is subclass of ETLStructure?", issubclass(etl, ETLStructure), "\n")

    print("[ Testing ] registry etl using decorator with config")
    etl = __sample___github___config
    etl()(rdd=None, config={"hello": "world"})
    print("is subclass of ETLStructure?", issubclass(etl, ETLStructure), "\n")

    print("[ Testing ] registry etl using inheritance with BaseETL")
    etl = __sample___github___inheriting_base_etl
    etl()(rdd=None)
    print("is subclass of ETLStructure?", issubclass(etl, ETLStructure), "\n")

    # check is it properly registryed
    print("[ Testing ] check is it properly registry")
    print("="*50)
    print(registry._registry)
    print("="*50)