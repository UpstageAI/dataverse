"""
Code is from ChenghaoMou/text-dedup
https://github.com/ChenghaoMou/text-dedup/blob/main/text_dedup/minhash_spark.py

This is a migration of the code to Dataverse.

Copyright (c) 2024-present Upstage Co., Ltd.
Apache-2.0 license
"""

import hashlib
import functools
import re
import os
import struct
import sys
from itertools import tee
from operator import add
from typing import Any, List, Text, Tuple, Union

import numpy as np
import pyspark
from pyspark.rdd import RDD
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.ml.feature import NGram, RegexTokenizer
from scipy.integrate import quad as integrate

from dataverse.etl.registry import register_etl

# region: Connected Components in MapReduce and Beyond, 2014
def generate_edges(nodes: List[int]) -> List[Tuple[int, int]]:
    """
    Generate edges from a cluster. Instead of generating N^2 edges, we only need all nodes align to a single node, since
    we will be running connected components on the edges later.

    Parameters
    ----------
    nodes : List[int]
        The list of nodes in the cluster.

    Returns
    -------
    List[Tuple[int, int]]
        The list of edges.

    Examples
    --------
    >>> generate_edges([1, 2, 3])
    [(2, 1), (3, 1)]
    """
    if len(nodes) <= 1:
        return []

    min_node = min(nodes)
    return [(n, min_node) for n in nodes if n != min_node]


def get_hash(text: str, n_bytes: int=8):
    return int.from_bytes(
        hashlib.sha1(text.encode("utf-8")).digest()[:n_bytes], 
        sys.byteorder
    ) 


def get_signatures(
    shingles: List[str], 
    band_n: int, 
    row_per_band: int, 
    mod_prime: int, 
    hash_params: Tuple[np.ndarray]
):
    if not shingles:
        return []
    
    shingles = np.array(
        [get_hash(shingle) for shingle in set(shingles)], 
        dtype=np.uint64
    )

    signatures = np.full(
        shape=(band_n * row_per_band), 
        fill_value=mod_prime, 
        dtype=np.uint64
    )

    chunk_size = 2 ** 10
    a, b = hash_params
    for i in range(0, len(shingles), chunk_size):
        shingles_chunk = shingles[i:i+chunk_size]
        signatures = np.minimum(
            signatures, 
            np.min((shingles_chunk.reshape(-1, 1) * a + b) % mod_prime, axis=0)
        )

    return [
        f"{idx:02d}" \
        + signatures[i*row_per_band:(i+1)*row_per_band].tobytes().hex() 
        for idx, i in enumerate(range(band_n))
    ]


# region: MinHashLSH
def optimal_param(
    threshold: float,
    num_perm: int,
    false_positive_weight: float = 0.5,
    false_negative_weight: float = 0.5,
):
    """
    Compute the optimal `MinHashLSH` parameter that minimizes the weighted sum
    of probabilities of false positive and false negative, taken from datasketch.

    Parameters
    ----------
    threshold : float
        The threshold for similarity.
    num_perm : int
        The number of permutations.
    false_positive_weight : float
        The weight of false positive.
    false_negative_weight : float
        The weight of false negative.

    Returns
    -------
    Tuple[int, int]
        The optimal `b` and `r` parameters.
        The number of bands, and the number of rows per band respectively.

    Examples
    --------
    >>> optimal_param(0.7, 256)
    (25, 10)
    """

    def false_positive_area(threshold: float, b: int, r: int):
        """Source: `datasketch.lsh`"""

        def area(s):
            return 1 - (1 - s ** float(r)) ** float(b)

        a, _ = integrate(area, 0.0, threshold)
        return a

    def false_negative_area(threshold: float, b: int, r: int):
        """Source: `datasketch.lsh`"""

        def area(s):
            return 1 - (1 - (1 - s ** float(r)) ** float(b))

        a, _ = integrate(area, threshold, 1.0)
        return a

    min_error = float("inf")
    opt = (0, 0)
    for b in range(1, num_perm + 1):
        max_r = int(num_perm / b)
        for r in range(1, max_r + 1):
            fp = false_positive_area(threshold, b, r)
            fn = false_negative_area(threshold, b, r)
            error = fp * false_positive_weight + fn * false_negative_weight
            if error < min_error:
                min_error = error
                opt = (b, r)
    return opt

# region: Quality Control
def process_cluster(cluster: List[Any]) -> List[Any]:
    return cluster[:1]

@register_etl
def deduplication___minhash___lsh_jaccard(
    spark: SparkSession,
    data: Union[RDD, DataFrame],
    threshold: float = 0.7,
    ngram_size: int = 5,
    min_length: int = 5,
    num_perm: int = 250,
    band_n: int = None,
    row_per_band: int = None,
    id_col: Union[str, None] = None,
    subset: str = "text",
    seed: int = 42,
    duplicates_save_path: Union[str, None] = None,
    *args,
    **kwargs,
) -> RDD:
    """
    Fuzzy deduplication using MinHash and Locality Sensitive Hashing (LSH).

    Args:
        spark (SparkSession): The SparkSession object.
        data (Union[RDD, DataFrame]): Input data to be deduplicated.
        threshold (float, optional): Similarity threshold. Default is 0.7.
        ngram_size (int, optional): Size of n-grams. Default is 5.
        min_length (int, optional): Minimum token length of document to be considered. Default is 5.
        num_perm (int, optional): Number of permutations. Default is 250.
        band_n (int, optional): Number of bands. If not provided, it will be calculated based on the threshold and num_perm.
        row_per_band (int, optional): Number of rows per band. If not provided, it will be calculated based on the threshold and num_perm.
        id_col (str, optional): Key column for extract duplicated rows. If not provided, temporary id column will be created.
        subset (str, optional): Column to deduplicate on. Default is "text".
        seed (int, optional): Random seed. Default is 42.
        duplicates_save_path (str, optional): Save path for duplicated entries. If not provided, not saving the duplicates.

    Returns:
        RDD: Deduplicated data as a DataFrame.
    """
    spark.sparkContext.setCheckpointDir("checkpoint")
    from graphframes import GraphFrame

    if isinstance(data, RDD):
        data_df = data.toDF()
    elif isinstance(data, DataFrame):
        data_df = data

    if os.path.exists(duplicates_save_path):
        assert "duplicates_save_path already exists."

    temp_id_col, component_col, tokens_col, ngrams_col = \
        "__id__", "__component__", "__tokens__", "__ngrams__"
    
    exist_cols = set(data_df.columns)
    while True:
        if temp_id_col in exist_cols:
            temp_id_col += "_"
        elif component_col in exist_cols:
            component_col += "_"
        elif tokens_col in exist_cols:
            tokens_col += "_"
        elif ngrams_col in exist_cols:
            ngrams_col += "_"
        else:
            break

    if id_col is None:
        id_col = temp_id_col
        print(f"create temp id col: {id_col}")
        data_df = data_df.withColumn(id_col, F.monotonically_increasing_id())
        data_df.persist(pyspark.StorageLevel.DISK_ONLY)

    if band_n is None or row_per_band is None:
        band_n, row_per_band = optimal_param(threshold, num_perm)

    mod_prime = 1 << 61 - 1 
    gen = np.random.RandomState(seed)
    hash_params = (
        gen.randint(1, mod_prime, dtype=np.uint64, size=band_n * row_per_band),
        gen.randint(0, mod_prime, dtype=np.uint64, size=band_n * row_per_band),
    )

    subset_type: str = [t for c, t in data_df.dtypes if c == subset][0]
    if subset_type.startswith("str"):
        # assume subset col should be tokenized
        tokens_df = RegexTokenizer(
            inputCol=subset, 
            outputCol=tokens_col,
            pattern="\\W"
        ).transform(
            data_df
            .select(id_col, F.col(subset).substr(1, 10_000_000).alias(subset)) 
        ).select(
            id_col, tokens_col
        ).filter(
            F.size(tokens_col) >= min_length
        )
    elif subset_type.startswith("array"):
        print("already tokenized.")
        tokens_col = subset
        tokens_df = data_df.select(id_col, tokens_col)

    shingles_df = NGram(
        n=ngram_size, 
        inputCol=tokens_col, 
        outputCol=ngrams_col
    ).transform(tokens_df).select(id_col, ngrams_col)

    sig_udf = F.udf(
        functools.partial(
            get_signatures,
            band_n=band_n,
            row_per_band=row_per_band,
            mod_prime=mod_prime,
            hash_params=hash_params
        ), 
        returnType=T.ArrayType(T.StringType())
    )
    signature_df = (
        shingles_df
        .select(id_col, F.explode(sig_udf(ngrams_col)).alias("band"))
        .groupby("band")
        .agg(
            F.collect_set(id_col).alias("ids")
        )
    )

    edge_udf = F.udf(
        generate_edges, 
        returnType=T.ArrayType(T.ArrayType(data_df.schema[id_col].dataType))
    )
    edges_df = (
        signature_df
        .select("ids")
        .filter(F.size("ids") > 1)
        .select(F.explode(edge_udf("ids")).alias("edges"))
        .distinct()
        .selectExpr("edges[0] as src", "edges[1] as dst")
    ).persist(pyspark.StorageLevel.DISK_ONLY)

    count = edges_df.count()
    if count == 0:
        print("no entry for deduplication.")
        edges_df.unpersist()
        data_df.unpersist()
        return data
    
    vertices_df = (
        edges_df
        .selectExpr("src as id")
        .union(edges_df.selectExpr("dst as id"))
        .distinct()
    )

    assignment = (
        GraphFrame(vertices_df, edges_df)
        .connectedComponents(broadcastThreshold=200 * (1024 ** 2))
    )

    join_df = data_df.join(
        assignment.select(
            F.col("id").alias(id_col), 
            F.col("component").alias(component_col)
        ),
        on=id_col,
        how="left"
    )

    if duplicates_save_path is not None:
        duplicates_df = (
            join_df
            .filter(F.col(component_col).isNotNull())
            .drop(ngrams_col)
        )
        if id_col == temp_id_col:
            duplicates_df = duplicates_df.drop(id_col)
        if tokens_col != subset:
            duplicates_df = duplicates_df.drop(tokens_col)
        
        duplicates_df.write.parquet(duplicates_save_path)
        duplicates_df.unpersist()

    final_df = (
        join_df
        .filter(F.col(component_col).isNull())
        .union(
            join_df
            .filter(F.col(component_col).isNotNull())
            .dropDuplicates([component_col])
        )
        .drop(component_col, ngrams_col)
    )

    if id_col == temp_id_col:
        final_df = final_df.drop(id_col)
    if tokens_col != subset:
        final_df = final_df.drop(tokens_col)

    edges_df.unpersist()
    return final_df.rdd