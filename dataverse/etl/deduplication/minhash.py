
"""
Code is from ChenghaoMou/text-dedup
https://github.com/ChenghaoMou/text-dedup/blob/main/text_dedup/minhash_spark.py

This is a migration of the code to Dataverse.
"""

import pyspark
from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from dataverse.etl.registry import register_etl
from typing import Union, List, Tuple, Any, Text

import re
import sys
import hashlib
import struct
import numpy as np

from operator import add
from itertools import tee
from scipy.integrate import quad as integrate


NON_ALPHA = re.compile(r"\W", re.UNICODE)
DTYPE = np.uint32
MAX_HASH = 4_294_967_295
MOD_PRIME = 4_294_967_291


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


def small_star(edges):
    def small_star_map(edge):
        x, y = edge
        if y <= x:
            return (x, y)
        else:
            return (y, x)

    def small_star_reduce(x):
        node, neighbors = x
        nodes = neighbors + [node]
        min_node = min(nodes)
        new_edges = set((neighbor, min_node) for neighbor in nodes if (neighbor <= node and neighbor != min_node))
        change = len(new_edges.difference(set([(node, neighbor) for neighbor in neighbors])))
        return (list(new_edges), change)

    neighbors = edges.map(small_star_map).groupByKey().map(lambda x: (x[0], list(set(x[1]))))
    edges_with_change = neighbors.map(small_star_reduce).cache()

    # no duplicate edges
    if edges_with_change.isEmpty():
        total_change = 0
    else:
        total_change = edges_with_change.map(lambda x: x[1]).reduce(add)

    edges = edges_with_change.flatMap(lambda x: x[0])
    edges_with_change.unpersist()

    return edges, total_change


def large_star(edges):
    def large_star_map(edge):
        if edge[0] == edge[1]:
            return [(edge[0], edge[1])]
        return [(edge[0], edge[1]), (edge[1], edge[0])]

    def large_star_reduce(x):
        node, neighbors = x
        nodes = neighbors + [node]
        min_node = min(nodes)
        new_edges = set((neighbor, min_node) for neighbor in (neighbors + [node]) if (neighbor > node))
        change = len(new_edges.difference(set([(node, neighbor) for neighbor in neighbors])))
        return list(new_edges), change

    neighbors = edges.flatMap(large_star_map).groupByKey().map(lambda x: (x[0], list(set(x[1]))))
    edges_with_change = neighbors.map(large_star_reduce).cache()

    # no duplicate edges
    if edges_with_change.isEmpty():
        total_change = 0
    else:
        total_change = edges_with_change.map(lambda x: x[1]).reduce(add)

    edges = edges_with_change.flatMap(lambda x: x[0])
    edges_with_change.unpersist()
    return edges, total_change


def alternating_algo(edges, max_iteration: int) -> Tuple[Any, bool, int]:
    prev_lchanges: int = sys.maxsize
    prev_schanges: int = sys.maxsize
    curr_iteration: int = 0

    while max_iteration:
        edges, curr_lchanges = large_star(edges)
        edges, curr_schanges = small_star(edges)

        if (curr_lchanges == prev_lchanges and curr_schanges == prev_schanges) or (
            curr_schanges == 0 and curr_lchanges == 0
        ):
            return edges, True, curr_iteration

        prev_lchanges = curr_lchanges
        prev_schanges = curr_schanges
        curr_iteration += 1
        max_iteration -= 1

    return edges, False, curr_iteration

# endregion


# region: Hashing
def ngrams(sequence: List[Text], n: int, min_length: int = 5):
    """
    Return the ngrams generated from a sequence of items, as an iterator.

    This is a modified version of nltk.util.ngrams.

    Parameters
    ----------
    sequence : List[Text]
        The sequence of items.
    n : int
        The length of each ngram.
    min_length : int, optional
        The minimum length of each ngram, by default 5

    Returns
    -------
    iterator
        The ngrams.

    Examples
    --------
    >>> list(ngrams(["a", "b", "c", "d"], 2, min_length=1))
    [('a', 'b'), ('b', 'c'), ('c', 'd')]
    >>> list(ngrams(["a", "b", "c", "d"], 2, min_length=5))
    []
    >>> list(ngrams(["a", "b"], 3, min_length=1))
    [('a', 'b')]
    """
    if len(sequence) < min_length:
        return []
    if len(sequence) < n:
        return [tuple(sequence)]
    iterables = tee(iter(sequence), n)
    for i, sub_iterable in enumerate(iterables):
        for _ in range(i):
            next(sub_iterable, None)
    return zip(*iterables)


def sha1_hash32(data):
    """
    Directly taken from datasketch package to avoid abstraction.

    Parameters
    ----------
    data : bytes

    Returns
    -------
    int
        The first 4 bytes (32 bits) of the SHA1 hash of the input data.

    Examples
    --------
    >>> sha1_hash32(b"hello")
    499578026
    >>> bin(sha1_hash32(b"hello"))
    '0b11101110001101111010010101010'
    >>> sha1_hash32(b"hello world").bit_length()
    30
    """
    return struct.unpack("<I", hashlib.sha1(data).digest()[:4])[0]


def generate_hash_values(
    content: str,
    idx: int,
    num_perm: int,
    ngram_size: int,
    min_length: int,
    hashranges: List[Tuple[int, int]],
    permutations: Tuple[np.ndarray, np.ndarray],
) -> List[Tuple[int, bytes, int]]:
    """
    Generate the MinHashLSH values for a given document.

    Parameters
    ----------
    content : str
        The content of the document.
    idx : int
        The index of the document.
    num_perm : int
        The number of permutations.
    ngram_size : int
        The size of the n-grams.
    min_length : int
        The minimum number of tokens in a document.
    hashranges : list
        The ranges of offsets for each hash value.
    permutations : Tuple[np.ndarray, np.ndarray]
        The permutations for the hash values.

    Returns
    -------
    List[Tuple[int, bytes, int]]
        The list of (band_idx, hash value, idx) for the document.

    Examples
    --------
    >>> content = "hello world"
    >>> idx = 0
    >>> num_perm = 250
    >>> ngram_size = 1
    >>> hashranges = [(i, i + 25) for i in range(0, 250, 25)]
    >>> PERMUTATIONS = (
    ...     RNG.randint(1, MOD_PRIME, size=(num_perm,), dtype=DTYPE),
    ...     RNG.randint(0, MOD_PRIME, size=(num_perm,), dtype=DTYPE),
    ... )
    >>> res = generate_hash_values(content, idx, num_perm, ngram_size, 0, hashranges, PERMUTATIONS)
    >>> len(res)
    10
    >>> sum(len(h) for _, h, _ in res) == len(res) * 25 * np.dtype(DTYPE).itemsize
    True
    """
    tokens = {" ".join(t).encode("utf-8") for t in ngrams(NON_ALPHA.split(content.lower()), ngram_size, min_length)}
    a, b = permutations
    hv = np.array([sha1_hash32(token) for token in tokens], dtype=DTYPE)
    phv = np.bitwise_and(((hv * np.tile(a, (len(tokens), 1)).T).T + b) % MOD_PRIME, MAX_HASH)  # type: ignore
    hash_values = np.vstack([phv, np.ones(num_perm, dtype=DTYPE) * MAX_HASH]).min(axis=0)
    Hs = [bytes(hash_values[start:end].byteswap().data) for start, end in hashranges]
    return [(band_idx, H, idx) for band_idx, H in enumerate(Hs)]


# endregion

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


# endregion

# region: Quality Control
def process_cluster(cluster: List[Any]) -> List[Any]:
    return cluster[:1]
# endregion



@register_etl
def deduplication___minhash___lsh_jaccard(
    spark,
    data: Union[RDD, DataFrame],
    threshold=0.7,
    ngram_size=5,
    min_length=5,
    num_perm=250,
    band_n=None,
    row_per_band=None,
    subset="text",
    seed=42,
    *args,
    **kwargs,
):
    """
    fuzzy deduplication using minhash and lsh
    reference - ChenghaoMou/text-dedup

    args:
        threshold: similarity threshold
        ngram_size: n-gram size
        min_length: minimum token length of document to be considered
        num_perm: number of permutations
        band_n: number of bands
        row_per_band: number of rows per band
        subset: column to deduplicate on
    """
    if isinstance(data, RDD):
        data = data.toDF()

    RNG = np.random.RandomState(seed)

    if band_n is None or row_per_band is None:
        band_n, row_per_band = optimal_param(threshold, num_perm)

    HASH_RANGES: List[Tuple[int, int]] = [
        (i * row_per_band, (i + 1) * row_per_band) for i in range(band_n)
    ]
    PERMUTATIONS: Tuple[np.ndarray, np.ndarray] = (
        RNG.randint(1, MOD_PRIME, size=(num_perm,), dtype=DTYPE),
        RNG.randint(0, MOD_PRIME, size=(num_perm,), dtype=DTYPE),
    )

    # region: Data Loading
    data = data.withColumn("__id__", F.monotonically_increasing_id()).cache()
    # endregion

    # region: MinHash
    records: RDD = data.select("__id__", subset).rdd.cache()
    buckets: RDD = (
        records.flatMap(
            lambda x: generate_hash_values(
                content=x[1],  # subset
                idx=x[0],  # __id__
                num_perm=num_perm,
                ngram_size=ngram_size,
                min_length=min_length,
                hashranges=HASH_RANGES,
                permutations=PERMUTATIONS,
            )
        )  # (band_idx, band hash value, idx)
        .groupBy(lambda x: (x[0], x[1]))  # group by (band_idx, band hash value)
        .mapValues(lambda x: [ele[2] for ele in x])  # ((band_idx, hash value), [idx, ...])
    ).cache()
    records.unpersist()
    # endregion

    # region: Connected Components
    edges: RDD = buckets.flatMap(lambda x: generate_edges(x[1])).distinct().cache()
    buckets.unpersist()
    duplicate_edges, converged, iteration = alternating_algo(edges, max_iteration=20)
    duplicate_edges.cache()
    edges.unpersist()
    # endregion

    # region: Merge Results
    if not duplicate_edges.isEmpty():
        self_edges: pyspark.RDD = duplicate_edges.values().distinct().map(lambda x: (x, x)).cache()
        all_edges: pyspark.RDD = duplicate_edges.union(self_edges).cache()
        data = data.join(
            spark.createDataFrame(all_edges, schema=["__id__", "__component__"]),
            on="__id__",
            how="left",
        ).cache()
        duplicate_edges.unpersist()
        # endregion

        self_edges.unpersist()

        # region: Quality Control: This section is hard-coded for The Stack
        cliques: RDD = all_edges.groupBy(lambda x: x[1]).cache()
        all_edges.unpersist()

        data = data.join(
            spark.createDataFrame(
                cliques.mapValues(lambda x: process_cluster(cluster=list(x))).flatMap(
                    lambda x: [(ele[0], True) for ele in x[1]]
                ),
                schema=["__id__", "__keep__"],
            ),
            on="__id__",
            how="left",
        )
        cliques.unpersist()
        data = data.filter(F.col("__component__").isNull() | F.col("__keep__")).cache()
        data = data.drop("__component__", "__keep__").cache()
    # endregion

    # drop temporary columns
    data = data.drop("__id__").cache()

    return data