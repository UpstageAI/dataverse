

from pyspark.rdd import RDD
from dataverse.etl.registry import BaseETL
from dataverse.etl.registry import register_etl
from dataverse.etl.registry import ETLRegistry


@register_etl
def __sample___github___using_decorator(rdd: RDD):
    """
    decorator will convert this function BaseETL class
    """
    print("sample using decorator")
    return rdd

class __sample___github___inheriting_base_etl(BaseETL):
    """
    Inheriting BaseETL class will automatically register the class to the registry
    but you must overwrite run method unless you will raise NotImplementedError
    decorator `register_etl` will do this automatically for you
    """
    def run(self, rdd: RDD, **kwargs):
        print("sample inheriting base etl run")
        return rdd


if __name__ == "__main__":
    registry = ETLRegistry()

    print("[ Testing ] registry etl using decorator")
    # this could seem like a function but it is actually a BaseETL class
    __sample___github___using_decorator()(rdd=None)

    print("[ Testing ] registry etl using inheritance with BaseETL")
    __sample___github___inheriting_base_etl()(rdd=None)

    # check is it properly registryed
    print("[ Testing ] check is it properly registry")
    print("="*50)
    print(registry._registry)
    print("="*50)