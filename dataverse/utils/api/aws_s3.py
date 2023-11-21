
"""
Usage:

```python
from dataverse.utils.api import aws_s3_list_buckets
from dataverse.utils.api import aws_s3_list

aws_s3_list_buckets()
aws_s3_list("bucket_name")
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

def aws_s3_create_bucket(bucket_name, location='ap-northeast-2'):
    """
    create aws s3 bucket

    Args:
        bucket_name (str): bucket name (must be unique)
        location (str): aws region name
    """
    s3 = boto3.client('s3', region_name=location)
    s3.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={'LocationConstraint': location}
    )


def aws_s3_read(bucket_name, key):
    """
    Args:
        bucket_name (str): bucket name
        key (str): key (aws s3 file path)

    Usage:
        aws_s3_read('tmp', 'this/is/path.json')
    """
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket_name, Key=key)
    text = obj['Body'].read().decode('utf-8')

    return text


def aws_s3_download(bucket_name, key, local_path):
    """
    Args:
        bucket_name (str): bucket name
        key (str): key (aws s3 file path)
        local_path (str): local path to save file

    Usage:
        aws_s3_download('tmp', 'this/is/path.json', 'path.json')
    """
    s3 = boto3.client('s3')
    s3.download_file(bucket_name, key, local_path)

def aws_s3_upload(bucket_name, key, local_path):
    """
    Args:
        bucket_name (str): bucket name
        key (str): key (aws s3 file path)
        local_path (str): local path to save file

    Usage:
        aws_s3_upload('tmp', 'this/is/path.json', 'path.json')
    """
    s3 = boto3.client('s3')
    s3.upload_file(local_path, bucket_name, key)

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

def aws_s3_list(bucket_name, prefix="", delimiter="/", remove_prefix=False):
    """
    list files/folders from specific path from aws s3
    aws s3 ls s3://bucket_name/prefix

    Args:
        bucket_name (str): bucket name
        prefix (str): prefix (check aws doc)
        delimiter (str): delimiter (check aws doc)
        remove_prefix (bool): remove prefix itself
            - default - False
    Returns:
        list: list of files/folders
            - list ends with '/' if it is a folder

    Usage:
    - **get bucket [ sub file/folder ] list**
        - aws_list(bucket_name)
            - subfolder1/
            - subfolder2/
            - subfile1
    - **get bucket `subfolder1`'s [ sub file/folder ] list**
        - aws_list(bucket_name, prefix="subfolder1")
            - case1: remove_prefix == True
                - ducky_folder1/
                - ducky_folder2/
                - ducky_file
            - case2: remove_prefix == False
                - subfolder1/ducky_folder1/
                - subfolder1/ducky_folder2/
                - subfolder1/ducky_file
    """
    s3 = boto3.client("s3")
    results = s3.list_objects_v2(
        Bucket=bucket_name,
        Prefix=prefix,
        Delimiter=delimiter,
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