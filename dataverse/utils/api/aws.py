
"""
Usage:

```python
from dataverse.utils.api import aws_s3_list_buckets
from dataverse.utils.api import aws_s3_list

aws_s3_list_buckets()
aws_s3_list("bucket")
```
"""

import json
import boto3



def aws_check_credentials(verbose=True):
    """
    simple check if aws credentials are valid

    Returns:
        bool: True if valid, False if not valid
    """
    sts = boto3.client('sts')
    try:
        sts.get_caller_identity()
        return True
    except Exception as e:
        if verbose:
            print(e)
        return False

class AWSClient:
    """
    AWS Client Information
    """
    # Singleton
    _initialized = False

    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(AWSClient, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        if self._initialized:
            return
        self.region = boto3.session.Session().region_name
        if self.region is None:
            raise Exception("AWS Region is not set. Set the AWS Region with `aws configure`")

        self.iam = boto3.client('iam')
        self.s3 = boto3.client('s3')
        self.ec2 = boto3.client('ec2', region_name=self.region)
        self.sts = boto3.client('sts')
        self.user_id = self.sts.get_caller_identity()['UserId']
        self._initialized = True

    def __str__(self) -> str:
        self.__repr__()

    def __repr__(self) -> str:
        return f"AWSClient(region={self.region}, user_id={self.user_id})"

# --------------------------------------------------------------------------------
# AWS State
"""
[ What is State? ]
>>> state management of operating aws services for dataverse

state will be managed by python dictionary and saved as json file in aws s3.
This will be synced with running AWS services and it will be created for each user.

[ stored information ]
- cache, meta, config, codes, etc.
"""
def aws_get_state():
    # to avoid circular import
    from dataverse.utils.setting import SystemSetting

    aws_bucket = SystemSetting()['AWS_BUCKET']
    state_path = f'{AWSClient().user_id}/state.json'

    # get state from aws s3
    try:
        content = aws_s3_read(aws_bucket, state_path)
        state = json.loads(content)

    # FIXME: exception should distinguish between key not found and other errors
    except:
        state = {}
        aws_s3_write(aws_bucket, state_path, json.dumps(state))

    return state

def aws_set_state(state):
    # to avoid circular import
    from dataverse.utils.setting import SystemSetting

    aws_bucket = SystemSetting()['AWS_BUCKET']
    state_path = f'{AWSClient().user_id}/state.json'
    aws_s3_write(aws_bucket, state_path, json.dumps(state))

# --------------------------------------------------------------------------------

def aws_vpc_create(cidr_block=None):

    # load all vpcs ids to check if the cidr block is occupied
    vpcs = AWSClient().ec2.describe_vpcs()
    second_octets = []
    for vpc in vpcs['Vpcs']:
        second_octet = int(vpc['CidrBlock'].split('.')[1])
        second_octets.append(second_octet)

    # auto generate cidr block if not provided
    if cidr_block is None:
        is_network_available = False
        for octet in range(0, 255):
            if octet not in second_octets:
                is_network_available = True
                break

        if is_network_available:
            cidr_block = '10.' + str(octet) + '.0.0/16'
        else:
            raise Exception('Unable to find an available CIDR block for VPC.')

    # user provided cidr block
    elif cidr_block.split('.')[1] in second_octets:
        raise Exception('The CIDR block is already occupied.')

    # create vpc
    vpc = AWSClient().ec2.create_vpc(CidrBlock=cidr_block)
    AWSClient().ec2.create_tags(
        Resources=[vpc_id],
        Tags=[{'Key':'Name', 'Value':'Dataverse-Temporary-VPC'}]
    )
    vpc_id = vpc['Vpc']['VpcId']

    # update state
    state = aws_get_state()
    if 'vpc' not in state:
        state['vpc'] = {}

    state['vpc'][vpc_id] = {}
    aws_set_state(state)

    return vpc_id

def aws_vpc_delete(vpc_id):
    if isinstance(vpc_id, str):
        vpc_ids = [vpc_id]
    elif isinstance(vpc_id, list):
        vpc_ids = vpc_id

    for vpc_id in vpc_ids:
        state = aws_get_state()

        # when VPC has dependency, remove dependency first
        if state['vpc'][vpc_id]:
            if 'subnet' in state['vpc'][vpc_id]:
                aws_subnet_delete(vpc_id, state['vpc'][vpc_id]['subnet'])

        AWSClient().ec2.delete_vpc(VpcId=vpc_id)
        del state['vpc'][vpc_id]
        aws_set_state(state)

def aws_subnet_create(vpc_id, cird_block=None):
    if cird_block is None:
        # Get VPC information to determine CIDR block
        vpcs = AWSClient().ec2.describe_vpcs(VpcIds=[vpc_id])
        cird_block = vpcs['Vpcs'][0]['CidrBlock']

    # create subnet
    subnet = AWSClient().ec2.create_subnet(CidrBlock=str(cird_block), VpcId=vpc_id)
    AWSClient().ec2.create_tags(
        Resources=[subnet['Subnet']['SubnetId']],
        Tags=[{'Key':'Name', 'Value':'Dataverse-Temporary-Subnet'}]
    )
    subnet_id = subnet['Subnet']['SubnetId']

    # update state
    state = aws_get_state()
    if 'subnet' not in state['vpc'][vpc_id]:
        state['vpc'][vpc_id]['subnet'] = []

    state['vpc'][vpc_id]['subnet'].append(subnet_id)
    aws_set_state(state)

    return subnet_id

def aws_subnet_delete(vpc_id, subnet_id):
    if isinstance(subnet_id, str):
        subnet_ids = [subnet_id]
    elif isinstance(subnet_id, list):
        subnet_ids = subnet_id

    for subnet_id in subnet_ids:
        AWSClient().ec2.delete_subnet(SubnetId=subnet_id)
        state = aws_get_state()
        state['vpc'][vpc_id]['subnet'].remove(subnet_id)
        aws_set_state(state)



def aws_s3_create_bucket(bucket):
    """
    create aws s3 bucket

    Args:
        bucket (str): bucket name (must be unique)
        location (str): aws region name
    """
    AWSClient().s3.create_bucket(
        Bucket=bucket,
        CreateBucketConfiguration={'LocationConstraint': AWSClient().region}
    )


def aws_s3_read(bucket, key):
    """
    Args:
        bucket (str): bucket name
        key (str): key (aws s3 file path)

    Usage:
        aws_s3_read('tmp', 'this/is/path.json')
    """
    obj = AWSClient().s3.get_object(Bucket=bucket, Key=key)
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
    AWSClient().s3.download_file(bucket, key, local_path)

def aws_s3_upload(bucket, key, local_path):
    """
    Args:
        bucket (str): bucket name
        key (str): key (aws s3 file path)
        local_path (str): local path to save file

    Usage:
        aws_s3_upload('tmp', 'this/is/path.json', 'path.json')
    """
    AWSClient().s3.upload_file(local_path, bucket, key)

def aws_s3_write(bucket, key, obj):
    """
    Args:
        bucket (str): bucket name
        key (str): key (aws s3 file path)
        obj (str): object to write

    Usage:
        aws_s3_write('tmp', 'this/is/path.json', '{"hello": "world"}')
    """
    AWSClient().s3.put_object(Bucket=bucket, Key=key, Body=obj)

def aws_s3_list_buckets():
    """
    get all buckets from aws s3
    """
    buckets = AWSClient().s3.list_buckets()['Buckets']
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
    if query is None or query == "":
        return aws_s3_list_buckets()
    elif len(query.split("/")) > 1:
        bucket, prefix = query.split("/", 1)
    else:
        bucket = query
        prefix = ""

    if prefix and not prefix.endswith("/"):
        prefix += "/"

    results = AWSClient().s3.list_objects_v2(
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