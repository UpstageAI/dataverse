
"""
Load Common Crawl data from dump-id & segment files

Code is from facebookresearch/cc_net with some modifications
https://github.com/facebookresearch/cc_net

This is a migration of the code to Dataverse.
"""

import io
import os
import json
import gzip
import glob
import time
import tempfile
import requests
import functools
import warnings
import numpy as np

from pathlib import Path
from urllib.parse import urlparse
from typing import Any, List, Text, Tuple, Optional, Union, Dict, Iterable, TextIO

from dataverse.etl import register_etl
from dataverse.utils.setting import SystemSetting
from dataverse.utils.format import get_uuidv1


def parse_doc(headers: List[str], doc: List[str]) -> Optional[dict]:
    """Headers format is:
    WARC/1.0
    WARC-Type: conversion
    WARC-Target-URI: [url]
    WARC-Date: [crawldate: 2019-02-15T19:15:59Z]
    WARC-Record-ID: <urn:uuid:8865156e-d5f1-4734-9c68-4b46eaf2bb7e>
    WARC-Refers-To: <urn:uuid:340152e2-65cf-4143-b522-8ce4e2d069d7>
    WARC-Block-Digest: sha1:S3DTWCONT2L6ORTGCY2KXEZ37LNBB7V2
    Content-Type: text/plain
    Content-Length: 7743
    """
    if not headers or not doc:
        return None
    try:
        url, date, digest, length = None, None, None, None
        for header in headers:
            if header.startswith("WARC-Target-URI:"):
                url = header.split()[1]
            elif header.startswith("WARC-Date:"):
                date = header.split()[1]
            elif header.startswith("WARC-Block-Digest:"):
                digest = header.split()[1]
            elif header.startswith("Content-Length:"):
                length = int(header.split()[1])

    except Exception as e:
        # logger.warning("Can't parse header:", e, headers, doc)
        return None

    # Docs are separated by two empty lines.
    last = None
    if not doc[-1] and not doc[-2]:
        last = -2
    title, doc = doc[0], doc[1:last]

    return {
        "url": url,
        "date_download": date,
        "digest": digest,
        "length": length,
        "nlines": len(doc),
        "source_domain": urlparse(url).netloc,
        "title": title,
        "raw_content": "\n".join(doc),
    }

def group_by_docs(warc_lines: Iterable[str]) -> Iterable[dict]:
    doc: List[str] = []
    headers, read_headers = [], True
    for warc in warc_lines:
        warc = warc.strip()
        if read_headers:
            headers.append(warc)
            read_headers = warc != ""
            continue

        if warc == "WARC/1.0":
            # We reached the beginning of the new doc.
            parsed = parse_doc(headers, doc)
            if parsed is not None:
                yield parsed
            headers, doc, read_headers = [warc], [], True
            continue

        doc.append(warc)

    # Return the last document
    if doc:
        parsed = parse_doc(headers, doc)
        if parsed is not None:
            yield parsed

def _close_when_exhausted(file) -> Iterable[str]:
    with file:
        yield from file

def open_segment_file(segment: str, verbose: bool = True) -> Iterable[str]:
    """
    overwrite the open_segment function to get the WET file from the folder

    args:
        segment: path to the WET file
    """
    filename = Path(segment)
    if filename.suffix == ".gz":
        file: TextIO = gzip.open(filename, "rt")  # type: ignore
    else:
        file = open(filename, "rt")
    return _close_when_exhausted(file)

def process_segment_file(segment: str, verbose: bool = True) -> Iterable[dict]:
    for doc in group_by_docs(open_segment_file(segment, verbose=verbose)):
        doc["cc_segment"] = segment
        yield doc

def find_wet_files(directory):
    """find *.wet, *wet.gz files recursively"""
    return glob.glob(os.path.join(directory, '**/*.wet'), recursive=True) + \
           glob.glob(os.path.join(directory, '**/*.wet.gz'), recursive=True)



WET_URL_ROOT = "https://data.commoncrawl.org"
FileDescriptor = Union[Path, List[Path], str]
ReadableFileLike = Union[Iterable[str], FileDescriptor, None]

def _tmp(prefix: str = None, suffix: str = None, dir: Path = None) -> Path:
    if isinstance(prefix, Path):
        prefix = str(prefix)
    if isinstance(suffix, Path):
        suffix = str(suffix)
    _, tmp_path = tempfile.mkstemp(prefix=prefix, suffix=suffix, dir=dir)
    return Path(tmp_path)

def _yield_from(files: list) -> Iterable[str]:
    for file in files:
        yield from open_read(file)

def open_read(filename: ReadableFileLike) -> Iterable[str]:
    """Open the given file, list of files or files matching the given glob and read lines.

    `filename` is None or "-" -> reads from stdin
    `filename` is a Path / str -> interprets filename as a glob and open files matching it
    `filename` is a list -> opens sequentially all files from the list using `open_read`
    `filename` is something else -> returns the object wrapped in a `nullcontext`
        This allows to pass already openened files or iterables.

    `open_read` will decompress gzip files, given they have ".gz" suffix.
    """
    if filename is None:
        return sys.stdin

    if isinstance(filename, list):
        assert isinstance(filename[0], Path)
        if len(filename) == 0:
            return []
        if len(filename) > 1:
            return _yield_from(filename)
        filename = tp.cast(Path, filename[0])
    if isinstance(filename, str):
        if filename.startswith("http://") or filename.startswith("https://"):
            return open_remote_file(filename)

        filename = Path(filename)
    if not isinstance(filename, Path):
        # we might have received an iterable, return it unmodified.
        return filename  # type: ignore

    # Expand glob patterns only when reading
    files = [Path(f) for f in sorted(glob.glob(str(filename)))]
    if len(files) > 1:
        return _yield_from(files)
    if len(files) == 1:
        filename = files[0]

    assert isinstance(filename, Path)

    if filename.suffix == ".gz":
        file: TextIO = gzip.open(filename, "rt")  # type: ignore
    else:
        file = open(filename, "rt")

    return _close_when_exhausted(file)


def request_get_content(url: str, n_retry: int = 3, verbose: bool = True) -> bytes:
    """Retrieve the binary content at url.

    Retry on connection errors.
    """
    t0 = time.time()

    if verbose:
        # TODO: Logging will be activated later
        # logging.info(f"Starting download of {url}")
        print(f"Starting download of {url}")

    for i in range(1, n_retry + 1):
        try:
            with requests.Session() as session:
                r = session.get(url)
                r.raise_for_status()
            break
        except requests.exceptions.RequestException as e:
            # Sleep and try again on error, unless it's a 404.
            message = e.args[0] if isinstance(e.args[0], str) else ""
            if i == n_retry or "Client Error" in message:
                raise e
            warnings.warn(
                f"Swallowed error {e} while downloading {url} ({i} out of {n_retry})"
            )
            time.sleep(10 * 2 ** i)


    if verbose:
        dl_time = time.time() - t0
        dl_speed = len(r.content) / dl_time / 1024
        # logging.info(
        #     f"Downloaded {url} [{r.status_code}] took {dl_time:.0f}s ({dl_speed:.1f}kB/s)"
        # )
        print(f"Downloaded {url} [{r.status_code}] took {dl_time:.0f}s ({dl_speed:.1f}kB/s)")

    return r.content


def open_remote_file(url: str, cache: Path, verbose: bool = True) -> Iterable[str]:
    """
    Download the files at the given url to memory and opens it as a file.
    Assumes that the file is small, and fetch it when this function is called.
    """
    if cache and cache.exists():
        return open_read(cache)

    # TODO: open the remote file in streaming mode.
    # The hard part is that we need to write the content on disk at the same time,
    # to implement disk caching.
    raw_bytes = request_get_content(url, verbose=verbose)
    content = io.BytesIO(raw_bytes)
    if url.endswith(".gz"):
        f: TextIO = gzip.open(content, mode="rt")  # type: ignore
    else:
        f = io.TextIOWrapper(content)

    try:
        # The file might have been created even not fully downloaded/written
        # so make sure tmp_cache is deleted when the program exits.
        # and only replace the cache file when the download is complete.
        if cache and not cache.exists():
            tmp_cache = _tmp(cache)
            tmp_cache.write_bytes(raw_bytes)
            if not cache.exists():
                tmp_cache.replace(cache)
    finally:
        if tmp_cache.exists():
            tmp_cache.unlink()

    return _close_when_exhausted(f)


def cc_wet_paths_url(dump_id: str) -> str:
    return "/".join([WET_URL_ROOT, "crawl-data", "CC-MAIN-" + dump_id, "wet.paths.gz"])

def segment_url(segment: str):
    return "/".join((WET_URL_ROOT, segment))

def cc_segment_urls(dump_id: str, cache_dir: Path, verbose: bool = True) -> List[str]:
    wet_paths = cc_wet_paths_url(dump_id)
    wet_paths_cache = cache_dir / f"wet_{dump_id}.paths.gz"
    f = open_remote_file(wet_paths, cache=wet_paths_cache, verbose=verbose)
    return [segment.strip() for segment in f]

def open_segment_url(segment: str, cache_dir: Path, verbose: bool = True) -> Iterable[str]:
    url = segment_url(segment)
    file: Optional[Path] = None
    if cache_dir:
        file = cache_dir / segment.split("/")[-1]

    return open_remote_file(url, cache=file, verbose=verbose)

def process_segment_url(segment: str, cache_dir: Path, verbose: bool = True) -> Iterable[str]:
    for doc in group_by_docs(open_segment_url(segment, cache_dir, verbose=verbose)):
        doc["cc_segment"] = segment
        yield doc




@register_etl
def data_ingestion___common_crawl___wet2raw(
    spark,
    wet_path: str,
    segment_n: int = -1,
    repartition=20,
    seed: int = 42,
    verbose=True,
    *args,
    **kwargs
):
    """
    load WET to raw format as dict

    [ what is WET? ]
    - WET files which store extracted plain text from the data stored in the WARC‍

    Args:
        spark (SparkSession): spark session
        wet_path (str): path to the WET folder includes WET format files
            - this search recursively, so you don't need to specify the path to each WET file
            - this search for all the *.wet, *.gz files in the folder
        segment_n (int): the number of segments to load, this is sampling
            - one segment is about 1GB
            - (default) set as -1 if you wanna load all the segments
        repartition (int): the number of partitions
        seed (int): random seed
        verbose (bool): whether to print the information of the dataset
    """
    wet_paths = find_wet_files(wet_path)
    if segment_n > 0 and segment_n < len(wet_paths):
        np.random.seed(seed)
        wet_paths = np.random.choice(wet_paths, size=segment_n, replace=False)

    rdd = spark.sparkContext.parallelize(wet_paths)
    rdd = rdd.flatMap(functools.partial(process_segment_file, verbose=verbose))
    rdd = rdd.repartition(repartition)

    return rdd


@register_etl
def data_ingestion___common_crawl___dump2raw(
    spark,
    dump: str,
    segment_n: int = -1,
    repartition=20,
    use_cache: bool = True,
    cache_dir: str = None,
    seed: int = 42,
    verbose=True,
    *args,
    **kwargs
):
    """
    Args:
        spark (SparkSession): spark session
        dump (str): dump id of the Common Crawl
            - ex) 2023-23
        segment_n (int): the number of segments to load, this is sampling
            - one segment is about 1GB
            - (default) set as -1 if you wanna load all the segments
        repartition (int): the number of partitions
        use_cache (bool): whether to use the cache
            - set as False if you wanna save disk space, because the cache is huge
            - one WET dump is about 10TB
        cache_dir (str): cache path to save the dataset
        seed (int): random seed
        verbose (bool): whether to print the information of the dataset
    """
    if use_cache:
        if cache_dir is None:
            # save the parquet at package root path
            cache_dir = SystemSetting().CACHE_DIR
            cache_dir = f"{cache_dir}/.cache/dataverse/dataset/common_crawl_{dump}"
        else:
            cache_dir = f"{cache_dir}/common_crawl_{dump}"
    else:
        cache_dir = None

    if not isinstance(cache_dir, Path):
        cache_dir = Path(cache_dir)

    # if cache dir exist creat one
    if cache_dir and not cache_dir.exists():
        cache_dir.mkdir(parents=True)

    wet_urls = cc_segment_urls(dump, cache_dir, verbose=verbose)

    if segment_n > 0 and segment_n < len(wet_urls):
        np.random.seed(seed)
        wet_urls = np.random.choice(wet_urls, size=segment_n, replace=False)

    rdd = spark.sparkContext.parallelize(wet_urls)
    rdd = rdd.flatMap(functools.partial(
        process_segment_url,
        cache_dir=cache_dir,
        verbose=verbose,
    ))
    rdd = rdd.repartition(repartition)

    return rdd


def convert_bytes(data):
    if isinstance(data, bytes):
        return data.decode()
    if isinstance(data, dict):
        return {convert_bytes(key): convert_bytes(value) for key, value in data.items()}
    if isinstance(data, list):
        return [convert_bytes(element) for element in data]
    return data

@register_etl
def data_ingestion___common_crawl___raw2ufl(spark, data, *args, **kwargs):
    """
    convert raw format to ufl with custom template
    """
    def templatev1(data):
        new_data = {}
        new_data['id'] = get_uuidv1()
        new_data['name'] = 'common_crawl'
        new_data['text'] = (
            f"{data.get('raw_content', None)}"
        )
        new_data['meta'] = json.dumps(
            convert_bytes({
                'title': data.get('title', None),
                'url': data.get('url', None),
                'date_download': data.get('date_download', None),
                'digest': data.get('digest', None),
                'length': data.get('length', None),
                'nlines': data.get('nlines', None),
                'source_domain': data.get('source_domain', None),
                'cc_segment': data.get('cc_segment', None),
            })
        )
        return new_data

    data = data.map(lambda x: templatev1(x))

    return data