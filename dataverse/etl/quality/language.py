
"""
language filtering from Common Crawl

This is a migration of the common crawl code to Dataverse.
some part of code is from facebookresearch/cc_net
https://github.com/facebookresearch/cc_net/blob/main/cc_net/split_by_lang.py
"""

from pyspark.rdd import RDD
from pyspark.sql import DataFrame


import os
import multiprocessing
import functools
import requests
import fasttext
from fasttext.FastText import _FastText
from typing import Union
from pathlib import Path

from dataverse.etl.registry import register_etl
from dataverse.utils.setting import SystemSetting


def load_fasttext(
    url = "https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin",
):
    """
    There is 2 issues found here
    - due to unserilizable fasttext problem, we need to load the model for every task
        - this is a problem, extremely slow
        - we need to load the model once and use it for all tasks
    - since this could lead to duplicated download, we need to check if the model is already downloaded
        - so far found no duplicated download, but if there is, hope to be fixed in the future
    """
    # FIXME: this is a manual check for duplicate download
    # rd_n = np.random.randint(0, 1000000)
    # print(rd_n, 'entered load_fasttext model!')

    # Get the lid.bin file for Fasttext
    cache_dir = SystemSetting().CACHE_DIR
    cache_dir = Path(f"{cache_dir}/.cache/dataverse/model")
    fasttext_path = cache_dir / "fasttext" / "bin" / "lid.bin"
    fasttext_path.parent.mkdir(parents=True, exist_ok=True) # Make directories if not existed

    if not fasttext_path.exists():
        # FIXME: this is a manual check for duplicate download
        # print(rd_n, 'downloading fasttext model!')
        response = requests.get(url, stream=True)

        # Raise exception if downloading is not successful
        response.raise_for_status() 
        with open(fasttext_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
    
    # FIXME: this is to suppress the warning message
    # return fasttext.load_model(str(fasttext_path))
    return _FastText(model_path=str(fasttext_path))

def language_predict_fasttext(row, model, top_k: int = 1, score_rounding: int = 2):
    text = row['text'].replace("\n", "")
    labels, scores = model.predict(text, k=top_k)
    labels = [l.replace("__label__", "") for l in labels]

    row['labels'] = labels
    row['scores'] = scores.round(score_rounding)

    return row

def language_predict_fasttext_by_partition(rows, top_k: int = 1, score_rounding: int = 2):
    # loaded for every partition
    model = load_fasttext()

    # FIXME: not possible to use multiprocessing here because of the model is not serializable
    # pool = multiprocessing.Pool(processes = os.cpu_count() or 0)
    # results = pool.imap(
    #     functools.partial(language_predict_fasttext, model=model, top_k=top_k),
    #     rows,
    # )
    for row in rows:
        yield language_predict_fasttext(row, model, top_k=top_k)


@register_etl
def quality___language___fasttext_filter(
    spark,
    data: Union[RDD, DataFrame],
    subset='text',
    top_k=1,
    score_rounding=2,
    threshold=0.0,
    whitelist=None,
    blacklist=None,
    *args,
    **kwargs
):
    """
    language filtering using fasttext

    - if language score is below threshold, it will be removed

    - rules for `whitelist` and `blacklist`:
        - if `whitelist` and `blacklist` are both None, all languages will be kept
            - default

        - if `whitelist` is not None, only languages in the `whitelist` will be kept
        - if `blacklist` is not None, languages in the `blacklist` will be removed
        - if `whitelist` and `blacklist` are both not None, error will be raised

    args:
        subset (str): subset or columns to consider filter 
        top_k (int): number of top languages to keep after classification
            - if fasttext classified 3 languages, top_k=1 will keep the top language
                - [en, fr, de] -> [en]
            - if fasttext classified 3 languages, top_k=2 will keep the top 2 languages
                - [en, fr, de] -> [en, fr]
        score_rounding (int): number of decimal places to round the scores
        threshold (float): minimum score to keep the language
        whitelist (list): list of languages to keep
        blacklist (list): list of languages to remove
    """
    if isinstance(data, DataFrame):
        data = data.rdd
        data = data.map(lambda row: row.asDict())

    # detect language using fasttext
    data = data.mapPartitions(functools.partial(
        language_predict_fasttext_by_partition,
        top_k=top_k,
        score_rounding=score_rounding,
    ))

    # filter by threshold
    data = data.filter(lambda x: any(s >= threshold for s in x['scores'][:top_k]))

    # filter by whitelist and blacklist
    if whitelist is not None and blacklist is not None:
        raise ValueError("whitelist and blacklist cannot be both not None")
    elif whitelist is not None:
        data = data.filter(lambda x: any(l in whitelist for l in x['labels'][:top_k]))
    elif blacklist is not None:
        data = data.filter(lambda x: all(l not in blacklist for l in x['labels'][:top_k]))
    else:
        # otherwise, keep all languages
        ...

    # remove labels and scores
    data = data.map(lambda x: {k: v for k, v in x.items() if k != 'labels' and k != 'scores'})

    return data