

from pyspark.rdd import RDD
from dataverse.etl.register import BaseETL
from dataverse.etl.register import register_etl
from dataverse.etl.register import ETLRegistry


@register_etl
def sample___using_decorator(rdd: RDD):
    """
    decorator will convert this function BaseETL class
    """
    print("sample using decorator")
    return rdd

class sample___inheriting_base_etl(BaseETL):
    """
    you must overwrite run method unless you will raise NotImplementedError
    decorator `register_etl` will do this automatically for you
    """
    def run(self, rdd: RDD, **kwargs):
        print("sample inheriting base etl run")
        return rdd


if __name__ == "__main__":
    registry = ETLRegistry()

    print("[ Testing ] registry etl using decorator")
    # this could seem like a function but it is actually a BaseETL class
    sample___using_decorator()(rdd=None)

    print("[ Testing ] registry etl using inheritance with BaseETL")
    sample___inheriting_base_etl()(rdd=None)

    # check is it properly registryed
    print("[ Testing ] check is it properly registry")
    print("="*50)
    print(registry._registry)
    print("="*50)