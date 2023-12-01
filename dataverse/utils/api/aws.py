
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

def aws_vpc_create(cidr_block=None, tag_name='Dataverse-Temporary-VPC'):

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
    vpc_id = vpc['Vpc']['VpcId']
    AWSClient().ec2.create_tags(
        Resources=[vpc_id],
        Tags=[
            {'Key': 'Name', 'Value': tag_name},
        ]
    )

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
            if 'security_group' in state['vpc'][vpc_id]:
                aws_security_group_delete(vpc_id, state['vpc'][vpc_id]['security_group'])
            if 'gateway' in state['vpc'][vpc_id]:
                aws_gateway_delete(vpc_id, state['vpc'][vpc_id]['gateway'])
            if 'route_table' in state['vpc'][vpc_id]:
                aws_route_table_delete(vpc_id, state['vpc'][vpc_id]['route_table'])

        AWSClient().ec2.delete_vpc(VpcId=vpc_id)
        del state['vpc'][vpc_id]
        aws_set_state(state)

def aws_subnet_create(vpc_id, cird_block=None, tag_name='Dataverse-Temporary-Subnet'):
    if cird_block is None:
        # Get VPC information to determine CIDR block
        vpcs = AWSClient().ec2.describe_vpcs(VpcIds=[vpc_id])
        cird_block = vpcs['Vpcs'][0]['CidrBlock']

    # create subnet
    subnet = AWSClient().ec2.create_subnet(CidrBlock=str(cird_block), VpcId=vpc_id)
    subnet_id = subnet['Subnet']['SubnetId']
    AWSClient().ec2.create_tags(
        Resources=[subnet_id],
        Tags=[
            {'Key': 'Name', 'Value': tag_name},
        ]
    )

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

def aws_emr_security_group_create(
        vpc_id,
        port=4040,
        group_name='DataverseEMRSecurityGroup',
        description='Dataverse EMR security group',
        tag_name='Dataverse-Temporary-EMR-Security-Group'
    ):
    """
    Create a security group for EMR.
    # TODO: Create a new function for general purpose.
    ...

    args:
        vpc_id (str): The VPC ID.
        port (int): The port to open for pyspark UI
        group_name (str): The name of the security group.
        description (str): The description of the security group.
    """
    security_group = AWSClient().ec2.create_security_group(
        GroupName=group_name,
        Description=description,
        VpcId=vpc_id,
    )
    security_group_id = security_group['GroupId']
    AWSClient().ec2.authorize_security_group_ingress(
        GroupId=security_group_id,
        IpPermissions=[
            {
                'IpProtocol': 'tcp',
                'FromPort': port,
                'ToPort': port,
                'IpRanges': [{'CidrIp': '0.0.0.0/0'}]
            },
        ])
    AWSClient().ec2.create_tags(
        Resources=[security_group_id],
        Tags=[
            {'Key': 'Name', 'Value': tag_name},
        ]
    )

    # set state
    state = aws_get_state()
    if 'security_group' not in state['vpc'][vpc_id]:
        state['vpc'][vpc_id]['security_group'] = []

    state['vpc'][vpc_id]['security_group'].append(security_group_id)
    aws_set_state(state)

    return security_group_id

def aws_security_group_delete(vpc_id, security_group_id):
    if isinstance(security_group_id, str):
        security_group_ids = [security_group_id]
    elif isinstance(security_group_id, list):
        security_group_ids = security_group_id

    for security_group_id in security_group_ids:
        AWSClient().ec2.delete_security_group(GroupId=security_group_id)
        state = aws_get_state()
        state['vpc'][vpc_id]['security_group'].remove(security_group_id)
        aws_set_state(state)

def aws_gateway_create(vpc_id, tag_name='Dataverse-Gateway'):
    """
    Create a gateway for public subnet.
    """
    gateway = AWSClient().ec2.create_internet_gateway()
    gateway_id = gateway['InternetGateway']['InternetGatewayId']

    # attach gateway to vpc
    AWSClient().ec2.attach_internet_gateway(
        InternetGatewayId=gateway_id,
        VpcId=vpc_id
    )
    AWSClient().ec2.create_tags(
        Resources=[gateway_id],
        Tags=[
            {'Key': 'Name', 'Value': tag_name},
        ]
    )

    # set state
    state = aws_get_state()
    if 'gateway' not in state['vpc'][vpc_id]:
        state['vpc'][vpc_id]['gateway'] = []
     
    state['vpc'][vpc_id]['gateway'].append(gateway_id)
    aws_set_state(state)

    return gateway_id

def aws_gateway_delete(vpc_id, gateway_id):
    if isinstance(gateway_id, str):
        gateway_ids = [gateway_id]
    elif isinstance(gateway_id, list):
        gateway_ids = gateway_id

    for gateway_id in gateway_ids:
        # detach gateway from vpc
        AWSClient().ec2.detach_internet_gateway(
            InternetGatewayId=gateway_id,
            VpcId=vpc_id
        )
        AWSClient().ec2.delete_internet_gateway(InternetGatewayId=gateway_id)
        state = aws_get_state()
        state['vpc'][vpc_id]['gateway'].remove(gateway_id)
        aws_set_state(state)

def aws_route_table_create(vpc_id, gateway_id, tag_name='Dataverse-Route-Table'):
    """
    Create a route table for public subnet.
    """
    route_table = AWSClient().ec2.create_route_table(VpcId=vpc_id)
    route_table_id = route_table['RouteTable']['RouteTableId']
    AWSClient().ec2.create_route(
        DestinationCidrBlock='0.0.0.0/0',
        RouteTableId=route_table_id,
        GatewayId=gateway_id,
    )
    AWSClient().ec2.create_tags(
        Resources=[route_table_id],
        Tags=[
            {'Key': 'Name', 'Value': tag_name},
        ]
    )

    # set state
    state = aws_get_state()
    if 'route_table' not in state['vpc'][vpc_id]:
        state['vpc'][vpc_id]['route_table'] = []

    state['vpc'][vpc_id]['route_table'].append(route_table_id)
    aws_set_state(state)

    return route_table_id

def aws_route_table_delete(vpc_id, route_table_id):
    if isinstance(route_table_id, str):
        route_table_ids = [route_table_id]
    elif isinstance(route_table_id, list):
        route_table_ids = route_table_id

    for route_table_id in route_table_ids:
        AWSClient().ec2.delete_route_table(RouteTableId=route_table_id)
        state = aws_get_state()
        state['vpc'][vpc_id]['route_table'].remove(route_table_id)
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