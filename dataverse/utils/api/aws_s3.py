
"""
Usage:

```python
from dataverse.utils.api import aws_s3_list_buckets
from dataverse.utils.api import aws_s3_list

aws_s3_list_buckets()
aws_s3_list("bucket")
```
"""

import boto3


def aws_check_credentials():
    """
    simple check if aws credentials are valid
    if no error, then credentials are valid
    """
    sts = boto3.client('sts')
    sts.get_caller_identity()

def aws_s3_create_bucket(bucket, location='ap-northeast-2'):
    """
    create aws s3 bucket

    Args:
        bucket (str): bucket name (must be unique)
        location (str): aws region name
    """
    s3 = boto3.client('s3', region_name=location)
    s3.create_bucket(
        Bucket=bucket,
        CreateBucketConfiguration={'LocationConstraint': location}
    )


def aws_s3_read(bucket, key):
    """
    Args:
        bucket (str): bucket name
        key (str): key (aws s3 file path)

    Usage:
        aws_s3_read('tmp', 'this/is/path.json')
    """
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=key)
    text = obj['Body'].read().decode('utf-8')

    return text


def aws_s3_download(bucket, key, local_path):
    """
    Args:
        bucket (str): bucket name
        key (str): key (aws s3 file path)
        local_path (str): local path to save file

    Usage:
        aws_s3_download('tmp', 'this/is/path.json', 'path.json')
    """
    s3 = boto3.client('s3')
    s3.download_file(bucket, key, local_path)

def aws_s3_upload(bucket, key, local_path):
    """
    Args:
        bucket (str): bucket name
        key (str): key (aws s3 file path)
        local_path (str): local path to save file

    Usage:
        aws_s3_upload('tmp', 'this/is/path.json', 'path.json')
    """
    s3 = boto3.client('s3')
    s3.upload_file(local_path, bucket, key)

def aws_s3_write(bucket, key, obj):
    """
    Args:
        bucket (str): bucket name
        key (str): key (aws s3 file path)
        obj (str): object to write

    Usage:
        aws_s3_write('tmp', 'this/is/path.json', '{"hello": "world"}')
    """
    s3 = boto3.client('s3')
    s3.put_object(Bucket=bucket, Key=key, Body=obj)

def aws_s3_list_buckets():
    """
    get all buckets from aws s3
    """
    s3 = boto3.client("s3")
    buckets = s3.list_buckets()['Buckets']
    bucket_names = []
    for bucket in buckets:
        bucket_names.append(bucket['Name'])

    return bucket_names

def aws_s3_ls(query=None):
    """
    ls command for aws s3
    this is made to be similar to linux ls command
    and unified to only single args usage to make it simple

    Args:
        query (str): file search query
    Returns:
        list: list of files/folders
            - list ends with '/' if it is a folder

    Usage:

    ```python
    - bucket/
        - subfolder1/
            - duck_folder1/
            - duck_folder2/
            - duck_file.txt
        - subfolder2/
        - subfile1.json
    ```
    >>> aws_list()
    - bucket/

    >>> aws_list(bucket)
    - subfolder1/
    - subfolder2/
    - subfile1.json

    >>> aws_list(bucket/subfolder1")
    - ducky_folder1/
    - ducky_folder2/
    - ducky_file.txt
    """
    s3 = boto3.client("s3")
    if query is None or query == "":
        return aws_s3_list_buckets()
    elif len(query.split("/")) > 1:
        bucket, prefix = query.split("/", 1)
    else:
        bucket = query
        prefix = ""

    if prefix and not prefix.endswith("/"):
        prefix += "/"

    results = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix,
        Delimiter="/",
    )
    objects = []

    # TODO: no limit to 1,000 objects - use pagination
    ...

    # files
    if "Contents" in results:
        objects.extend(list(obj["Key"] for obj in results["Contents"]))

    # subfolders
    if "CommonPrefixes" in results:
        objects.extend(list(obj["Prefix"] for obj in results["CommonPrefixes"]))

    # set default
    remove_prefix = True
    if remove_prefix:
        # remove the prefix itself
        objects = list(obj.replace(prefix, "") for obj in objects)

        # remove ''
        objects = list(obj for obj in objects if obj)
    else:
        for obj in objects:
            if obj == prefix:
                objects.remove(obj)

    return objects