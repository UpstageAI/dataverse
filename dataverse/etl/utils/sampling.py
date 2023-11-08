
"""
Sampling module for data ingestion
"""

from pyspark.rdd import RDD
from dataverse.etl import register_etl

@register_etl
def utils___sampling___random(
    spark,
    ufl: RDD,
    replace=False,
    sample_n_or_frac=0.1,
    seed=42,
    *args,
    **kwargs
):
    """
    random sampling

    Args:
        ufl: ufl rdd
        replace: with replacement
        sample_n_or_frac: sample_n or fraction
        seed: seed
    """
    if isinstance(sample_n_or_frac, float):
        ufl = ufl.sample(replace, sample_n_or_frac, seed)

    # XXX: Take too long, 1M sample takes over 10 mins and didn't finish
    elif isinstance(sample_n_or_frac, int):
        ufl = ufl.takeSample(replace, sample_n_or_frac, seed)
    return ufl
