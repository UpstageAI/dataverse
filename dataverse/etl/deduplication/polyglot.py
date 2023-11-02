
"""
Code is from EleutherAI/dps
https://github.com/EleutherAI/dps/blob/master/dps/spark/jobs/dedup_job.py

This is a migration of the deduplication job from the DPS project to the Dataverse.
"""

import random
import binascii
import numpy as np
from itertools import combinations

from pyspark.rdd import RDD
from pyspark.sql import DataFrame

from dataverse.etl import register_etl

from typing import Union, List


MERSENNE_PRIME = (1 << 61) - 1
MAX_HASH = (1 << 32) - 1
HASH_RANGE = 1 << 32


def shingle_word(text: str, n_gram: int = 15, char_level: bool = False) -> List[str]:
    """
    example
    -------
    >>> shingle_word("hello world from ducky", n_gram=2)
    ['hello_world', 'world_from', 'from_ducky']

    >>> shingle_word("hello world from ducky", n_gram=2, char_level=True)
    ['h_e', 'e_l', 'l_l', 'l_o', 'o_w', 'w_o', 'o_r', 'r_l', 'l_d', 'd_f', 'f_r', 'r_o', 'o_m', 'm_d', 'd_u', 'u_c', 'c_k', 'k_y']
    """
    res = []
    text_words = text.split() if not char_level else text

    for i in range(len(text_words)):
        shingle = text_words[i : i + n_gram]

        if len(shingle) == n_gram:
            res.append("_".join(shingle).encode("utf-8"))

    return res


def generate_minhash(shingles: List, num_perm: int = 64, seed: int = 1) -> np.array:
    def hashfunc(b: bytes) -> bytes:
        return binascii.crc32(b) & MAX_HASH

    hashvalues = np.ones(num_perm, dtype=np.uint64) * MAX_HASH

    generator = np.random.RandomState(seed)
    permutations = np.array(
        [
            (
                generator.randint(1, MERSENNE_PRIME, dtype=np.uint64),
                generator.randint(0, MERSENNE_PRIME, dtype=np.uint64),
            )
            for _ in range(num_perm)
        ],
        dtype=np.uint64,
    ).T

    for shingle in shingles:
        hv = hashfunc(shingle)
        a, b = permutations
        phv = np.bitwise_and((a * hv + b) % MERSENNE_PRIME, np.uint64(MAX_HASH))
        hashvalues = np.minimum(phv, hashvalues)

    return hashvalues


def jaccard_by_hashvalues(src_hashvalues, tgt_hashvalues) -> float:
    if len(src_hashvalues) != len(tgt_hashvalues):
        raise ValueError()

    return np.float(np.count_nonzero(src_hashvalues == tgt_hashvalues)) / np.float(
        len(src_hashvalues)
    )


def expand_instances_by_minhash(
    data, expand_size: int, n_gram: int, seed: int = 1, char_level: bool = False
):
    shingles = shingle_word(data["text"], n_gram=n_gram, char_level=char_level)
    minhashes = generate_minhash(shingles, num_perm=expand_size, seed=seed)

    for mh in minhashes.tolist():
        yield (str(mh), [dict(**data, shingles=shingles, hashvalues=minhashes)])


def explore_dedup_instance(hash_groups, threshold: float = 0.8):
    if len(hash_groups) <= 1:
        return

    group_represent_text = hash_groups[0][
        "text"
    ]  # not to remove all text instances in group.
    pairs = combinations(hash_groups, 2)

    for d_1, d_2 in pairs:
        sim_score = jaccard_by_hashvalues(d_1["hashvalues"], d_2["hashvalues"])
        if sim_score >= threshold:
            dedup_text = [d_1["text"], d_2["text"]]
            if group_represent_text in dedup_text:
                yield dedup_text[0] if dedup_text[
                    0
                ] != group_represent_text else dedup_text[1]
            else:
                yield random.choice(dedup_text)


@register_etl
def deduplication___polyglot___minhash(
    spark,
    data: Union[RDD, DataFrame],
    expand_size: int = 64,
    n_gram: int = 15,
    seed: int = 1,
    char_level: bool = False,
    sim_threshold: float = 0.8,
    *args,
    **kwargs,
):
    """
    fuzzy deduplication

    for UFL
    """
    if isinstance(data, DataFrame):
        data = data.rdd
        data = data.map(lambda row: row.asDict())

    overlap_kv_rdd: RDD = (
        data.flatMap(
            lambda x: expand_instances_by_minhash(
                x,
                expand_size=expand_size,
                n_gram=n_gram,
                seed=seed,
                char_level=char_level,
            )
        )
        .reduceByKey(lambda x, y: x + y)
        .flatMap(
            lambda x: explore_dedup_instance(x[1], threshold=sim_threshold)
        )
        .distinct()
        .map(lambda x: (x, dict(text=x)))
        .cache()
    )

    data = data.map(lambda x: (x["text"], x)).subtractByKey(overlap_kv_rdd).map(
        lambda x: x[1]
    )
    return data
