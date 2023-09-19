

import boto3


def aws_check_credentials():
    sts = boto3.client('sts')
    sts.get_caller_identity()

def aws_list(bucket_name, prefix="", delimiter="/", remove_prefix=False):
    """
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