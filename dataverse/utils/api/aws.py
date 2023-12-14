
"""
Usage:

```python
from dataverse.utils.api import aws_s3_list_buckets
from dataverse.utils.api import aws_s3_list

aws_s3_list_buckets()
aws_s3_list("bucket")
```
"""


import os
import glob
import re
import shutil
import tarfile
import tempfile
import json
import time
import boto3
import datetime
import ipaddress
import pkg_resources
from omegaconf import OmegaConf

from dataverse.utils.analyze import python_is_script_executable


# TODO: get the information from AWS when it's supported someday
# reference - https://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-emr-supported-instance-types.html
EMR_SUPPORTED_EC2_INSTANCES = [
    "m1.small", "m1.medium", "m1.large", "m1.xlarge", "m3.xlarge", "m3.2xlarge",
    "c1.medium", "c1.xlarge", "c3.xlarge", "c3.2xlarge", "c3.4xlarge", "c3.8xlarge",
    "cc1.4xlarge", "cc2.8xlarge",
    "c4.large", "c4.xlarge", "c4.2xlarge", "c4.4xlarge", "c4.8xlarge",
    "c5.xlarge", "c5.9xlarge", "c5.2xlarge", "c5.4xlarge", "c5.9xlarge", "c5.18xlarge",
    "c5d.xlarge", "c5d.2xlarge", "c5d.4xlarge", "c5d.9xlarge", "c5d.18xlarge",
    "m2.xlarge", "m2.2xlarge", "m2.4xlarge",
    "r3.xlarge", "r3.2xlarge", "r3.4xlarge", "r3.8xlarge",
    "cr1.8xlarge",
    "m4.large", "m4.xlarge", "m4.2xlarge", "m4.4xlarge", "m4.10xlarge", "m4.16large",
    "m5.xlarge", "m5.2xlarge", "m5.4xlarge", "m5.12xlarge", "m5.24xlarge",
    "m5d.xlarge", "m5d.2xlarge", "m5d.4xlarge", "m5d.12xlarge", "m5d.24xlarge",
    "r4.large", "r4.xlarge", "r4.2xlarge", "r4.4xlarge", "r4.8xlarge", "r4.16xlarge",
    "h1.4xlarge",
    "hs1.2xlarge", "hs1.4xlarge", "hs1.8xlarge",
    "i2.xlarge", "i2.2xlarge", "i2.4xlarge", "i2.8xlarge",
    "d2.xlarge", "d2.2xlarge", "d2.4xlarge", "d2.8xlarge",
    "g2.2xlarge",
    "cg1.4xlarge"
]

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

        self.sts = boto3.client('sts')
        self.iam = boto3.client('iam')
        self.s3 = boto3.client('s3')
        self.ec2 = boto3.client('ec2')
        self.emr = boto3.client('emr')
        self.ssm = boto3.client('ssm')
        self.user_id = self.sts.get_caller_identity()['UserId']
        self.account_id = self.sts.get_caller_identity()['Account']
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
# AWS EC2 Resource
def aws_ec2_instance_at_az(az):
    """
    get all instance info at the given AZ
    """
    response = AWSClient().ec2.describe_instance_type_offerings(
        LocationType='availability-zone',
        Filters=[
            {
                'Name': 'location',
                'Values': [
                    az,
                ]
            },
        ]
    )
    instances = [inst['InstanceType'] for inst in response['InstanceTypeOfferings']]

    return instances

def aws_ec2_instance_info(instance):
    """
    get instance info from aws
    """
    response = AWSClient().ec2.describe_instance_types(
        InstanceTypes=[instance],
    )

    return response

def aws_ec2_all_instance_info():
    """
    get all instance types information
    """
    instance_info = {}
    token = ''
    while True:
        if token == '':
            response = AWSClient().ec2.describe_instance_types()
        else:
            response = AWSClient().ec2.describe_instance_types(NextToken=token)

        for instance_type in response['InstanceTypes']:
            instance_info[instance_type['InstanceType']] = {
                'vcpu': instance_type['VCpuInfo']['DefaultVCpus'],
                'memory': instance_type['MemoryInfo']['SizeInMiB']
            }

        if 'NextToken' in response:
            token = response['NextToken']
        else:
            break

    return instance_info

def aws_ec2_get_price(instance_type):
    response = AWSClient().ec2.describe_spot_price_history(
        InstanceTypes=[instance_type],
        ProductDescriptions=['Linux/UNIX (Amazon VPC)'],
        StartTime=datetime.datetime.now(),
        MaxResults=1,
    )

    return response['SpotPriceHistory'][0]['SpotPrice']


# --------------------------------------------------------------------------------
# AWS SSM (Systems Manager)
def aws_ssm_run_commands(instance_ids, commands, verbose=True, return_output=False):
    """
    Run commands on a list of EC2 instances using AWS SSM.
    """
    if return_output:
        results = {}
    for command in commands:
        if verbose:
            print(f"Sending following command to all instances...")
            print("==========================================")
            print(command)
            print("==========================================")

        command_id = AWSClient().ssm.send_command(
            InstanceIds=instance_ids,
            DocumentName="AWS-RunShellScript",
            Parameters={"commands": [command]},
            TimeoutSeconds=3600,
        )["Command"]["CommandId"]

        while True:
            # verify the previous step succeeded before running the next step.
            cmd_result = AWSClient().ssm.list_commands(CommandId=command_id)["Commands"][0]
            if cmd_result["StatusDetails"] == "Success":
                if verbose or return_output:
                    command_invocation = AWSClient().ssm.get_command_invocation(
                        CommandId=command_id,
                        InstanceId=instance_ids[0], # assume all instances are the same
                    )
                if verbose:
                    print("=========== Standard output ============")
                    print(command_invocation["StandardOutputContent"])
                    print("==========================================")
                    print(f"Command succeeded.")
                if return_output:
                    results[command] = command_invocation["StandardOutputContent"]
                break
            elif cmd_result["StatusDetails"] in ["Pending", "InProgress"]:
                if verbose:
                    print(f"Command status is {cmd_result['StatusDetails']}, waiting...")
                time.sleep(10)
            else:
                if verbose:
                    print(f"Command status is {cmd_result['StatusDetails']}, quitting.")
                    # get more detailed information about the command failure
                    command_invocation = AWSClient().ssm.get_command_invocation(
                        CommandId=command_id,
                        InstanceId=instance_ids[0], # assume all instances are the same
                    )
                    print("============= Error output ==============")
                    print(command_invocation["StandardErrorContent"])
                    print("=========== Standard output ============")
                    print(command_invocation["StandardOutputContent"])
                    print("==========================================")
                raise RuntimeError(
                    f"Command failed to run. [ {cmd_result['StatusDetails']} ]"
                )
    if return_output:
        return results



# --------------------------------------------------------------------------------
# AWS EMR

class EMRManager:
    """
    one EMR manager per one EMR cluster
    """
    def launch(self, config):
        """
        auto setup environments and launch emr cluster

        Args:
            config (OmegaConf): config for the etl
        """
        # clean unused resources
        self._clean()

        if config.emr.id is not None:
            config.emr.auto_generated = False

            return config.emr.id

        # TODO: modify interface for custom policy
        # create role & instance profile
        self._role_setup(config)
        self._instance_profile_setup(config)

        # create vpc
        self._vpc_setup(config)

        # create emr cluster
        # XXX: wait until instance profile is ready
        #      otherwise, emr cluster creation will fail
        # FIXME: convert to smart solution (e.g. waiter)
        #        currently AWS doesn't support waiter available option for instance profile
        # NOTE: I've tried to make waiter using `describe_instance_profile` but it didn't work
        time.sleep(7)

        # set default instance type
        self._set_default_instance(config)

        emr_id = self._emr_cluster_create(config)
        config.emr.id = emr_id
        config.emr.auto_generated = True

        return emr_id

    def _role_setup(self, config):
        """
        TODO: modify interface for custom policy
        """

        # [ EC2 ] --------------------------------------------------
        ec2_trust_policy = {
            "Version": "2008-10-17",
            "Statement": [
                {
                    "Sid": "",
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "ec2.amazonaws.com"
                    },
                    "Action": "sts:AssumeRole"
                }
            ]
        }
        ec2_role = 'Dataverse_EMR_EC2_DefaultRole'
        ec2_policy = 'AmazonElasticMapReduceforEC2Role'
        ssm_policy = 'AmazonSSMManagedInstanceCore'

        # add timestamp to temporary role name
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        ec2_role = f"{ec2_role}_{timestamp}"
        ec2_policy_arns = [
            f"arn:aws:iam::aws:policy/service-role/{ec2_policy}",
            f"arn:aws:iam::aws:policy/{ssm_policy}"
        ]

        aws_iam_role_create(
            role_name=ec2_role,
            trust_policy=ec2_trust_policy,
            policy_arns=ec2_policy_arns,
            description='Role for Dataverse EMR EC2',
        )
        config.emr.role.ec2.name = ec2_role
        config.emr.role.ec2.policy_arns = ec2_policy_arns

        # [ EMR ] --------------------------------------------------
        emr_trust_policy = {
            "Version": "2008-10-17",
            "Statement": [
                {
                    "Sid": "",
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "elasticmapreduce.amazonaws.com"
                    },
                    "Action": "sts:AssumeRole",
                    "Condition": {
                        "StringEquals": {
                            "aws:SourceAccount": AWSClient().account_id
                        },
                        "ArnLike": {
                            "aws:SourceArn": f"arn:aws:elasticmapreduce:{AWSClient().region}:{AWSClient().account_id}:*"
                        }
                    }
                }
            ]
        }
        emr_role = 'Dataverse_EMR_DefaultRole'
        emr_policy = 'AmazonElasticMapReduceRole'

        # add timestamp to temporary role name
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        emr_role = f"{emr_role}_{timestamp}"
        emr_policy_arns = [f"arn:aws:iam::aws:policy/service-role/{emr_policy}"]

        aws_iam_role_create(
            role_name=emr_role,
            trust_policy=emr_trust_policy,
            policy_arns=emr_policy_arns,
            description='Role for Dataverse EMR',
        )
        config.emr.role.emr.name = emr_role
        config.emr.role.emr.policy_arns = emr_policy_arns

    def _instance_profile_setup(self, config):
        """
        TODO: modify interface for custom policy
        """
        ec2_role = config.emr.role.ec2.name
        instance_profile_name = 'Dataverse_EMR_EC2_DefaultRole_InstanceProfile'

        # add timestamp to temporary role name
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        instance_profile_name = f"{instance_profile_name}_{timestamp}"

        aws_iam_instance_profile_create(
            instance_profile_name=instance_profile_name,
            role_name=ec2_role,
        )
        config.emr.instance_profile.name = instance_profile_name
        config.emr.instance_profile.ec2_role = ec2_role

    def _vpc_setup(self, config):
        """
        config will be automatically updated
        """

        # VPC
        vpc_id = aws_vpc_create()
        config.emr.vpc.id = vpc_id

        # if private subnet is required
        subnet_args = {
            'vpc_id': vpc_id,
            'tag_name': 'Dataverse-Temporary-Subnet-Public',
        }
        if not config.emr.subnet.public:
            vpcs = AWSClient().ec2.describe_vpcs(VpcIds=[vpc_id])
            cidr_block = vpcs['Vpcs'][0]['CidrBlock']
            ip_net = ipaddress.ip_network(cidr_block)

            # split the network into two subnets
            public_subnet, private_subnet = list(ip_net.subnets())
            subnet_args['cird_block'] = str(public_subnet)

        # Subnet
        subnet_id = aws_subnet_create(**subnet_args)
        config.emr.subnet.id = subnet_id
        config.emr.subnet.public_id = subnet_id

        # Internet Gateway
        gateway_id = aws_gateway_create(vpc_id)
        config.emr.gateway.id = gateway_id

        # Route Table
        route_table_id = aws_route_table_create(
            vpc_id=vpc_id,
            gateway_id=gateway_id,
            destination_cidr_block='0.0.0.0/0',
            tag_name='Dataverse-Route-Table-Public',
        )
        aws_route_table_asscociate_subnet(subnet_id, route_table_id)
        config.emr.route_table.id = route_table_id

        if not config.emr.subnet.public:
            # add NAT Gateway to public subnet
            elastic_ip_id = aws_elastic_ip_allocate(vpc_id=vpc_id)
            config.emr.elastic_ip.id = elastic_ip_id

            nat_gateway_id = aws_nat_gateway_create(
                vpc_id=vpc_id,
                subnet_id=subnet_id,
                elastic_ip_id=elastic_ip_id,
            )
            config.emr.nat_gateway.id = nat_gateway_id

            # create private subnet
            private_subnet_id = aws_subnet_create(
                vpc_id=vpc_id,
                cird_block=str(private_subnet),
                tag_name='Dataverse-Temporary-Subnet-Private',
            )
            config.emr.subnet.id = private_subnet_id
            config.emr.subnet.private_id = private_subnet_id

            # add NAT Gateway to private subnet
            private_route_table_id = aws_route_table_create(
                vpc_id=vpc_id,
                nat_gateway_id=nat_gateway_id,
                destination_cidr_block='0.0.0.0/0',
                tag_name='Dataverse-Route-Table-Private',
            )
            aws_route_table_asscociate_subnet(
                subnet_id=private_subnet_id,
                route_table_id=private_route_table_id,
            )

        # set state
        state = aws_get_state()
        state['vpc'][vpc_id]['public_subnet'] = config.emr.subnet.public
        aws_set_state(state)

    def _set_default_instance(
        self,
        config,
        min_memory=2048,
        max_memory=8192,
    ):
        """
        choose default instance type by memory

        args:
            config (OmegaConf): config for the etl
            min_memory (int): minimum memory size (MiB)
            max_memory (int): maximum memory size (MiB)
        """
        subnet_id = config.emr.subnet.id
        az = aws_subnet_az(subnet_id)
        instances = aws_ec2_instance_at_az(az=az)

        # find memory size is bigger specified min/max memory
        candidate = None
        _min_candidate_memory = float('inf')
        for instance in instances:

            # check if instance is supported by EMR
            if instance not in EMR_SUPPORTED_EC2_INSTANCES:
                continue

            instance_info = aws_ec2_instance_info(instance)
            memory = instance_info['InstanceTypes'][0]['MemoryInfo']['SizeInMiB']
            if min_memory <= memory <= max_memory:
                if memory < _min_candidate_memory:
                    candidate = instance
                    _min_candidate_memory = memory

        if candidate is None:
            raise Exception(f"Unable to find instance type with memory between {min_memory} and {max_memory}")


        instance_info = aws_ec2_instance_info(candidate)
        vcpu = instance_info['InstanceTypes'][0]['VCpuInfo']['DefaultVCpus']
        memory = instance_info['InstanceTypes'][0]['MemoryInfo']['SizeInMiB']
        print(
            f"{'=' * 80}\n"
            f"Default instance type is [ {candidate} ]\n"
            f"{'=' * 80}\n"
            f" vCPU: {vcpu}\n"
            f" Memory: {memory}\n"
            f" Price: {aws_ec2_get_price(candidate)}\n"
            f"{'=' * 80}\n"
        )

        if config.emr.master_instance.type is None:
            config.emr.master_instance.type = candidate
        if config.emr.core_instance.type is None:
            config.emr.core_instance.type = candidate
        if config.emr.task_instance.type is None:
            config.emr.task_instance.type = candidate

    def _emr_cluster_create(self, config):
        """
        create aws emr cluster

        Args:
            config (OmegaConf): config for the etl
        """
        # to avoid circular import
        from dataverse.utils.setting import SystemSetting
        log_dir = f"s3://{SystemSetting().AWS_BUCKET}/{AWSClient().user_id}/emr/logs"

        # instance group setting
        instance_groups = [
            {
                'Name': 'master nodes',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': config.emr.master_instance.type,
                'InstanceCount': 1,
            },
            {
                'Name': 'core nodes',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'CORE',
                'InstanceType': config.emr.core_instance.type,
                'InstanceCount': config.emr.core_instance.count,
            },
        ]

        # task is optional
        if config.emr.task_instance.count > 0:
            instance_groups.append(
                {
                    'Name': 'task nodes',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'TASK',
                    'InstanceType': config.emr.task_instance.type,
                    'InstanceCount': config.emr.task_instance.count,
                }
            )

        # create emr cluster
        emr_id = AWSClient().emr.run_job_flow(
            Name=config.emr.name,
            ReleaseLabel=config.emr.release,
            AutoTerminationPolicy={
                "IdleTimeout": config.emr.idle_timeout,
            },
            Instances={
                'InstanceGroups': instance_groups,
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2SubnetId': config.emr.subnet.id,
            },
            Applications=[{'Name': 'Spark'}],
            VisibleToAllUsers=True,
            JobFlowRole=config.emr.instance_profile.name,
            ServiceRole=config.emr.role.emr.name,
            Tags=[
                {
                    'Key': 'Name',
                    'Value': config.emr.name,
                },
            ],
            LogUri=log_dir,
        )['JobFlowId']

        # wait until emr cluster is ready
        waiter = AWSClient().emr.get_waiter('cluster_running')
        waiter.wait(ClusterId=emr_id)

        # set state
        state = aws_get_state()
        if 'emr' not in state:
            state['emr'] = {}

        state['emr'][emr_id] = {
            'vpc_id': config.emr.vpc.id,
        }

        # instance profile
        if config.emr.instance_profile.name is not None:
            state['emr'][emr_id]['instance_profile'] = config.emr.instance_profile.name

        # role
        if 'role' not in state['emr'][emr_id]:
            state['emr'][emr_id]['role'] = {}

        if config.emr.role.emr.name is not None:
            state['emr'][emr_id]['role']['emr'] = config.emr.role.emr.name
        if config.emr.role.ec2.name is not None:
            state['emr'][emr_id]['role']['ec2'] = config.emr.role.ec2.name

        aws_set_state(state)

        config.emr.id = emr_id

        return emr_id

    def run(self, config, verbose=False):
        # setup environment
        self._setup(config, verbose=verbose)

        # run emr
        # get pip installed packages path
        location = self._get_pip_package_path(config, verbose=verbose)
        emr_main = os.path.join(location, 'dataverse', 'api', 'emr.py')

        response = AWSClient().emr.add_job_flow_steps(
            JobFlowId=config.emr.id,
            Steps=[
                {
                    'Name': 'Run Dataverse python script on Master node',
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': [
                            'python3',
                            emr_main,
                            '--config',
                            '/home/hadoop/dataverse/config.yaml',
                        ]
                    }
                },
            ]
        )
        step_id = response['StepIds'][0]

        return step_id

    def _setup(self, config, verbose=False):
        """
        [ upload to S3 ]
        - config for `dataverse`
        - dataverse site-packages source code
        - requirements.txt
        - dynamic etl files

        [ move s3 to ec2 ]
        - move uploaded files in S3 from local to EMR cluster

        [ setup environment on EMR cluster ]
        - set aws region
        - install pip dependencies for `dataverse`
        - set `dataverse` package at EMR cluster pip installed packages path
        """
        # generate working directory
        self._get_working_dir(config)

        # upload to necessary dataverse files to S3
        self._upload_config(config)
        self._upload_source_code(config)
        self._upload_dependencies(config)
        self._upload_dynamic_etl_files(config)

        # move uploaded files in S3 from local to EMR cluster
        self._move_s3_to_ec2(config, verbose=verbose)

        # setup environment on EMR cluster
        self._setup_aws(config, verbose=verbose)
        self._setup_dependencies(config, verbose=verbose)
        self._setup_source_code(config, verbose=verbose)

    def _get_working_dir(self, config):
        """
        get working directory path for the emr cluster
        if not provided, it will be automatically generated
        """
        # to avoid circular import
        from dataverse.utils.setting import SystemSetting

        if config.emr.working_dir is not None:
            working_dir = config.emr.working_dir
            if working_dir.startswith(('s3://', 's3a://', 's3n://')):
                aws_s3_matched = re.match(r's3[a,n]?://([^/]+)/(.*)', working_dir)
                if not aws_s3_matched:
                    raise ValueError(f"EMR working directory {working_dir} is not a valid s3 path")
        else:
            # [ emr versioning ] - emr_YYYY-MM-DD_HH:MM:SS_<emr_id>
            # datetime first for ascending order
            bucket = SystemSetting()['AWS_BUCKET']
            user_id = AWSClient().user_id
            working_dir_name = datetime.datetime.now().strftime(f"emr_%Y-%m-%d_%H:%M:%S_{config.emr.id}")

            working_dir = f"s3://{bucket}/{user_id}/emr/{working_dir_name}"
            config.emr.working_dir = working_dir

        return working_dir

    def _upload_config(self, config):
        """
        upload config for `dataverse` to S3
        """
        working_dir = self._get_working_dir(config)
        bucket, key = aws_s3_path_parse(working_dir)

        aws_s3_write(bucket, f"{key}/config.yaml", OmegaConf.to_yaml(config))

    def _upload_source_code(self, config):
        """
        upload pip site-packages source code to S3

        caveat:
            this doesn't include wheel files or meta data for pip packages
        """
        # to avoid circular import
        from dataverse.utils.setting import SystemSetting

        temp_dir = tempfile.mkdtemp()
        zip_file = os.path.join(temp_dir, 'dataverse.tar.gz')

        dataverse_home = SystemSetting().DATAVERSE_HOME
        with tarfile.open(zip_file, "w:gz") as tar:
            tar.add(dataverse_home, arcname=os.path.basename(dataverse_home))

        working_dir = self._get_working_dir(config)
        bucket, key = aws_s3_path_parse(working_dir)

        aws_s3_upload(bucket, f'{key}/dataverse.tar.gz', zip_file)

        shutil.rmtree(temp_dir)

    def _upload_dependencies(self, config, package_name="dataverse"):
        # get all dependencies
        requirements = []
        for r in pkg_resources.get_distribution(package_name).requires():
            requirements.append(str(r))

        # create requirements.txt
        temp_dir = tempfile.mkdtemp()
        dependency_file = os.path.join(temp_dir, 'requirements.txt')

        with open(dependency_file, 'w') as f:
            for requirement in requirements:
                f.write(f"{requirement}\n")

        # upload requirements.txt to S3
        working_dir = self._get_working_dir(config)
        bucket, key = aws_s3_path_parse(working_dir)

        aws_s3_upload(bucket, f'{key}/requirements.txt', dependency_file)

        shutil.rmtree(temp_dir)

    def _upload_dynamic_etl_files(self, config):
        # to avoid circular import
        from dataverse.etl import ETLRegistry

        # get all etl files
        dynamic_etl_file_paths = []
        for etl in ETLRegistry().get_all():
            # not part of the dataverse source but dynamically loaded by user
            if not etl.__etl_dir__:
                file_path = etl.__file_path__

                # jupyter notebook is not supported
                # TODO: allow jupyter notebook
                # NOTE: reason why jupyter notebook is not supported is because
                #       the filename point at the temporary file path not the `.ipynb` file
                if 'ipykernel' in file_path:
                    raise ValueError(
                        'Dynamic ETL from jupyter notebook not supported. Only from .py files\n'
                        f"[ {file_path} ] is given which is temporary jupyter cell execution file\n"
                    )

                # only declaration is allowed
                # TODO: analyze the code and only parse necessary dynamic etl code
                # NOTE: this is to prevent execution of the code
                if python_is_script_executable(file_path):
                    raise ValueError(
                        'Dynamic ETL file should only contain declaration (imports, functions, classes, etc.)'
                        f"[ {file_path} ] includes execution.\n"
                    )

                # check not from of jupyter notebook
                dynamic_etl_file_paths.append(file_path)

        # upload etl files to S3
        working_dir = self._get_working_dir(config)
        bucket, key = aws_s3_path_parse(working_dir)

        # if dynamic_etl dir exists, remove it
        # NOTE: this is to prevent old files from being uploaded
        #       in case that user is using setup multiple times with same working_dir
        try:
            aws_s3_delete(bucket, f'{key}/dynamic_etl')
        except:
            pass

        for file_path in dynamic_etl_file_paths:
            aws_s3_upload(
                bucket=bucket,
                key=f'{key}/dynamic_etl/{os.path.basename(file_path)}',
                local_path=file_path
            )

    def _move_s3_to_ec2(self, config, verbose=False):
        """
        move uploaded files in S3 from local to EMR cluster
        """
        nodes = AWSClient().emr.list_instances(
            ClusterId=config.emr.id
        )["Instances"]
        instance_ids = [node["Ec2InstanceId"] for node in nodes]

        # remove existing dataverse directory
        commands = [
            "rm -r /home/hadoop/dataverse",
        ]
        try:
            aws_ssm_run_commands(instance_ids, commands, verbose=verbose)
        except:
            pass

        commands = [
            f"aws s3 cp {config.emr.working_dir} /home/hadoop/dataverse --recursive",
        ]
        aws_ssm_run_commands(instance_ids, commands, verbose=verbose)

    def _get_pip_package_path(self, config, verbose=False):
        """
        get pip installed packages path
        """
        nodes = AWSClient().emr.list_instances(
            ClusterId=config.emr.id
        )["Instances"]
        instance_ids = [node["Ec2InstanceId"] for node in nodes]

        commands = ["pip3 show numpy"]
        result = aws_ssm_run_commands(
            instance_ids,
            commands,
            verbose=verbose,
            return_output=True,
        )
        location = re.findall(r'Location: (.*)\n', result['pip3 show numpy'])[0]

        return location

    def _setup_aws(self, config, verbose=False):
        """
        setup aws environment on EMR cluster
        """
        nodes = AWSClient().emr.list_instances(
            ClusterId=config.emr.id
        )["Instances"]
        instance_ids = [node["Ec2InstanceId"] for node in nodes]

        commands = [
            f"aws configure set region {AWSClient().region}",
        ]
        aws_ssm_run_commands(instance_ids, commands, verbose=verbose)

    def _setup_dependencies(self, config, verbose=False):
        nodes = AWSClient().emr.list_instances(
            ClusterId=config.emr.id
        )["Instances"]
        instance_ids = [node["Ec2InstanceId"] for node in nodes]

        commands = [
            "sudo yum install -y python3-devel",
            "pip3 install wheel setuptools pip --upgrade",
        ]
        aws_ssm_run_commands(instance_ids, commands, verbose=verbose)

        # NOTE: unknown unlimited loop caused by `pip3 install -r requirements.txt`
        #       so I split the following command separately
        commands = [
            "pip3 install -r /home/hadoop/dataverse/requirements.txt",
        ]
        aws_ssm_run_commands(instance_ids, commands, verbose=verbose)

    def _setup_source_code(self, config, verbose=False):
        """
        copy dataverse source code to pip installed packages path
        """
        nodes = AWSClient().emr.list_instances(
            ClusterId=config.emr.id
        )["Instances"]
        instance_ids = [node["Ec2InstanceId"] for node in nodes]

        # unzip dataverse.tar.gz and copy to pip installed packages path
        commands = [
            "tar -xzf /home/hadoop/dataverse/dataverse.tar.gz -C /home/hadoop/dataverse",
        ]
        aws_ssm_run_commands(instance_ids, commands, verbose=verbose)

        # get pip installed packages path
        location = self._get_pip_package_path(config, verbose=verbose)

        # copy dataverse source code to pip installed packages path
        commands = [
            f"cp -r /home/hadoop/dataverse/dataverse {location}",
        ]
        aws_ssm_run_commands(instance_ids, commands, verbose=verbose)

    def wait(self, config, step_id, verbose=True):
        """
        waiter for emr step
        """
        while True:
            response = AWSClient().emr.describe_step(
                ClusterId=config.emr.id,
                StepId=step_id,
            )
            state = response['Step']['Status']['State']
            if state == 'Pending':
                time.sleep(10)
                if verbose:
                    print("[ Dataverse ] step pending...")
                continue
            if state in ['COMPLETED', 'FAILED', 'CANCELLED']:
                if verbose:
                    print(f"[ Dataverse ] step status: {state}. Done.")
                break
            if verbose:
                if 'Message' in response['Step']['Status']['StateChangeReason']:
                    print(response['Step']['Status']['StateChangeReason']['Message'])
                time.sleep(10)

    def terminate(self, config):
        """
        terminate emr cluster

        Args:
            config (OmegaConf): config for the etl
        """
        # only terminate auto generated emr cluster
        if config.emr.auto_generated is False:
            print('EMR cluster is not auto generated. Not terminating.')
            return

        if config.emr.id is None:
            print('EMR cluster is not launched. Proceeding to clean resources.')
        else:
            AWSClient().emr.terminate_job_flows(JobFlowIds=[config.emr.id])

            # wait until emr cluster is terminated
            waiter = AWSClient().emr.get_waiter('cluster_terminated')
            waiter.wait(ClusterId=config.emr.id)

            # set state
            state = aws_get_state()
            if 'emr' in state and config.emr.id in state['emr']:
                del state['emr'][config.emr.id]
                aws_set_state(state)

        # clean unused resources
        self._clean()

    def _clean(self):
        """
        clean unused resources related to EMR
        """
        self._clean_stopped_emr()
        self._clean_unused_vpc()
        self._clean_unused_iam_instance_profile()
        self._clean_unused_iam_role()

    def _clean_stopped_emr(self):
        """
        check stopped EMR and update the state
        """
        state = aws_get_state()

        # get all emr ids
        emr_ids = []
        if 'emr' in state:
            for emr_id in state['emr']:
                emr_ids.append(emr_id)

        # remove stopped emr from state
        REMOVE_STATES = [
            'TERMINATED',
            'TERMINATED_WITH_ERRORS'
        ]
        for emr_id in emr_ids:
            emr_info = AWSClient().emr.describe_cluster(ClusterId=emr_id)
            if emr_info['Cluster']['Status']['State'] in REMOVE_STATES:
                del state['emr'][emr_id]
        aws_set_state(state)

    def _clean_unused_vpc(self):
        """
        check the AWS state and clean vpc that is not used by any emr cluster
        """
        state = aws_get_state()

        # get all vpc ids that are used by emr
        used_vpc_ids = []
        if 'emr' in state:
            for emr_id in state['emr']:
                used_vpc_ids.append(state['emr'][emr_id]['vpc_id'])

        # get all vpc ids that are created
        all_vpc_ids = []
        if 'vpc' in state:
            for vpc_id in state['vpc']:
                all_vpc_ids.append(vpc_id)

        # clean unused vpc
        unused_vpc_ids = list(set(all_vpc_ids) - set(used_vpc_ids))

        for vpc_id in unused_vpc_ids:
            aws_vpc_delete(vpc_id)

    def _clean_unused_iam_role(self):
        """
        check the AWS state and clean iam role that is not used by any emr cluster
        """
        state = aws_get_state()

        # get all iam role names that are used by emr
        used_iam_role_names = []
        if 'emr' in state:
            for emr_id in state['emr']:
                if 'ec2' in state['emr'][emr_id]['role']:
                    used_iam_role_names.append(state['emr'][emr_id]['role']['ec2'])
                if 'emr' in state['emr'][emr_id]['role']:
                    used_iam_role_names.append(state['emr'][emr_id]['role']['emr'])

        # get all iam role names that are created
        all_iam_role_names = []
        if 'iam' in state and 'role' in state['iam']:
            for role_name in state['iam']['role']:
                all_iam_role_names.append(role_name)

        # clean unused iam role
        unused_iam_role_names = list(set(all_iam_role_names) - set(used_iam_role_names))

        for role_name in unused_iam_role_names:
            aws_iam_role_delete(role_name)

    def _clean_unused_iam_instance_profile(self):
        """
        check the AWS state and clean iam instance profile that is not used by any emr cluster
        """
        state = aws_get_state()

        # get all iam instance profile names that are used by emr
        used_iam_instance_profile_names = []
        if 'emr' in state:
            for emr_id in state['emr']:
                used_iam_instance_profile_names.append(state['emr'][emr_id]['instance_profile'])

        # get all iam instance profile names that are created
        all_iam_instance_profile_names = []
        if 'iam' in state and 'instance_profile' in state['iam']:
            for instance_profile_name in state['iam']['instance_profile']:
                all_iam_instance_profile_names.append(instance_profile_name)

        # clean unused iam instance profile
        unused_iam_instance_profile_names = list(set(all_iam_instance_profile_names) - set(used_iam_instance_profile_names))

        for instance_profile_name in unused_iam_instance_profile_names:
            aws_iam_instance_profile_delete(instance_profile_name)

    def terminate_by_id(self, emr_id):
        """
        when you want to terminate emr cluster without config

        ```python
        from dataverse.utils.api import EMRManager
        EMRManager().terminate_by_id('j-3C05XDxxxxxxx')
        ```
        """
        # to avoid circular import
        from dataverse.config import Config

        config = Config.default(emr=True)
        config.emr.id = emr_id
        self.terminate(config)


# --------------------------------------------------------------------------------

def aws_iam_role_create(
    role_name,
    trust_policy,
    policy_arns,
    description='Role for Dataverse',
    max_session_duration=3600,
):

    # create role
    try:
        AWSClient().iam.create_role(
            RoleName=role_name,
            Description=description,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            MaxSessionDuration=max_session_duration,
        )

        # attach policy
        for policy_arn in policy_arns:
            AWSClient().iam.attach_role_policy(
                RoleName=role_name,
                PolicyArn=policy_arn,
            )

        # set state
        state = aws_get_state()
        if 'iam' not in state:
            state['iam'] = {}

        if 'role' not in state['iam']:
            state['iam']['role'] = {}

        state['iam']['role'][role_name] = {
            'policy_arns': policy_arns,
        }
        aws_set_state(state)
    except AWSClient().iam.exceptions.EntityAlreadyExistsException:
        print(f"{role_name} already exists.")
    except Exception as e:
        raise e

    # wait until role is ready
    waiter = AWSClient().iam.get_waiter('role_exists')
    waiter.wait(RoleName=role_name)

def aws_iam_role_delete(role_name):
    try:
        # detach policy
        response = AWSClient().iam.list_attached_role_policies(RoleName=role_name)
        for policy in response['AttachedPolicies']:
            AWSClient().iam.detach_role_policy(
                RoleName=role_name,
                PolicyArn=policy['PolicyArn'],
            )

        # delete role
        AWSClient().iam.delete_role(RoleName=role_name)
    except AWSClient().iam.exceptions.NoSuchEntityException:
        print(f"{role_name} does not exist.")
    except Exception as e:
        raise e

    # set state
    state = aws_get_state()
    if 'iam' in state and 'role' in state['iam']:
        if role_name in state['iam']['role']:
            del state['iam']['role'][role_name]
            aws_set_state(state)

def aws_iam_instance_profile_create(instance_profile_name, role_name):
    try:
        AWSClient().iam.create_instance_profile(
            InstanceProfileName=instance_profile_name
        )
        AWSClient().iam.add_role_to_instance_profile(
            InstanceProfileName=instance_profile_name,
            RoleName=role_name
        )

        # set state
        state = aws_get_state()
        if 'iam' not in state:
            state['iam'] = {}

        if 'instance_profile' not in state['iam']:
            state['iam']['instance_profile'] = {}

        state['iam']['instance_profile'][instance_profile_name] = {
            'role_name': role_name,
        }
        aws_set_state(state)
    except AWSClient().iam.exceptions.EntityAlreadyExistsException:
        print(f"{instance_profile_name} already exists.")
    except Exception as e:
        raise e

    # wait until instance profile is ready
    waiter = AWSClient().iam.get_waiter('instance_profile_exists')
    waiter.wait(InstanceProfileName=instance_profile_name)

    # FIXME: wait until instance profile is available
    ...

def aws_iam_instance_profile_delete(instance_profile_name):
    # remove role from instance profile
    response = AWSClient().iam.get_instance_profile(InstanceProfileName=instance_profile_name)
    role_name = response['InstanceProfile']['Roles'][0]['RoleName']
    AWSClient().iam.remove_role_from_instance_profile(
        InstanceProfileName=instance_profile_name,
        RoleName=role_name,
    )

    # delete instance profile
    AWSClient().iam.delete_instance_profile(InstanceProfileName=instance_profile_name)

    # set state
    state = aws_get_state()
    if 'iam' in state and 'instance_profile' in state['iam']:
        if instance_profile_name in state['iam']['instance_profile']:
            del state['iam']['instance_profile'][instance_profile_name]
            aws_set_state(state)

def aws_iam_remove_all_instance_profile():
    """
    WARNING: this will remove all instance profile that has Dataverse in it
             which means it might remove other instance profile that not from you
    """
    # get all instance profile
    instance_profiles = AWSClient().iam.list_instance_profiles()["InstanceProfiles"]
    # remove all the instance_profile that has Dataverse in it
    for profile in instance_profiles:
        if "Dataverse" in profile["InstanceProfileName"]:
            aws_iam_instance_profile_delete(profile["InstanceProfileName"])


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

    state['vpc'][vpc_id] = {'public_subnet': False}
    aws_set_state(state)

    # wait until vpc is ready
    waiter = AWSClient().ec2.get_waiter('vpc_available')
    waiter.wait(VpcIds=[vpc_id])

    return vpc_id

def aws_vpc_delete(vpc_id):
    if isinstance(vpc_id, str):
        vpc_ids = [vpc_id]
    elif isinstance(vpc_id, list):
        vpc_ids = vpc_id

    for vpc_id in vpc_ids:
        state = aws_get_state()

        # [ DEPENDENCY ] remove all dependencies
        # ------------------------------------------------------------
        # dataverse managed dependency
        if state['vpc'][vpc_id]:
            if 'nat_gateway' in state['vpc'][vpc_id]:
                aws_nat_gateway_delete(vpc_id, state['vpc'][vpc_id]['nat_gateway'])
            if 'elastic_ip' in state['vpc'][vpc_id]:
                aws_elastic_ip_release(vpc_id, state['vpc'][vpc_id]['elastic_ip'])
            if 'subnet' in state['vpc'][vpc_id]:
                # NOTE: set retry because terminated EMR cluster iterrupts subnet deletion
                #       by dependency problem for few seconds
                # HACK: this is a hacky solution and should be fixed in the future
                RETRY_SUBNET_DELETION = 5
                for _ in range(RETRY_SUBNET_DELETION):
                    try:
                        aws_subnet_delete(vpc_id, state['vpc'][vpc_id]['subnet'])
                        break
                    except AWSClient().ec2.exceptions.ClientError as e:
                        if e.response['Error']['Code'] == 'DependencyViolation':
                            time.sleep(5)
                            continue
                        else:
                            raise e
                    except Exception as e:
                        raise e
            if 'security_group' in state['vpc'][vpc_id]:
                aws_security_group_delete(vpc_id, state['vpc'][vpc_id]['security_group'])
            if 'gateway' in state['vpc'][vpc_id]:
                aws_gateway_delete(vpc_id, state['vpc'][vpc_id]['gateway'])
            if 'route_table' in state['vpc'][vpc_id]:
                aws_route_table_delete(vpc_id, state['vpc'][vpc_id]['route_table'])

        # EMR managed dependency
        vpc = boto3.resource('ec2').Vpc(vpc_id)

        # NOTE: remove dependency between security groups
        for security_group in vpc.security_groups.all():
            aws_security_group_remove_dependency(security_group.id)

        for security_group in vpc.security_groups.all():
            if security_group.group_name == "default":
                continue
            aws_security_group_delete(vpc_id, security_group.id)
        # ------------------------------------------------------------

        try:
            AWSClient().ec2.delete_vpc(VpcId=vpc_id)
        # when vpc doesn't exist
        except AWSClient().ec2.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'InvalidVpcID.NotFound':
                print(f"VPC {vpc_id} doesn't exist.")
        # re-thrown other exceptions
        except Exception as e:
            raise e

        if 'vpc' in state and vpc_id in state['vpc']:
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

    # wait until subnet is ready
    waiter = AWSClient().ec2.get_waiter('subnet_available')
    waiter.wait(SubnetIds=[subnet_id])

    return subnet_id

def aws_subnet_delete(vpc_id, subnet_id):
    if isinstance(subnet_id, str):
        subnet_ids = [subnet_id]
    elif isinstance(subnet_id, list):
        subnet_ids = subnet_id

    for subnet_id in subnet_ids:
        AWSClient().ec2.delete_subnet(SubnetId=subnet_id)
        state = aws_get_state()

        if 'vpc' in state and vpc_id in state['vpc']:
            if 'subnet' in state['vpc'][vpc_id] and subnet_id in state['vpc'][vpc_id]['subnet']:
                state['vpc'][vpc_id]['subnet'].remove(subnet_id)
                aws_set_state(state)

def aws_subnet_az(subnet_id):
    """
    when subnet id is give find the AZ
    """
    response = AWSClient().ec2.describe_subnets(SubnetIds=[subnet_id])
    az = response['Subnets'][0]['AvailabilityZone']

    return az

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

        if 'vpc' in state and vpc_id in state['vpc']:
            if 'security_group' in state['vpc'][vpc_id] and security_group_id in state['vpc'][vpc_id]['security_group']:
                state['vpc'][vpc_id]['security_group'].remove(security_group_id)
                aws_set_state(state)

def aws_security_group_remove_dependency(security_group_id):
    """
    """
    response = AWSClient().ec2.describe_security_groups(
        GroupIds=[security_group_id]
    )

    # Removing inbound rules
    inbound_rules = response['SecurityGroups'][0]['IpPermissions']
    if inbound_rules:
        AWSClient().ec2.revoke_security_group_ingress(
            GroupId=security_group_id,
            IpPermissions=inbound_rules
        )

    # Removing outbound rules
    outbound_rules = response['SecurityGroups'][0]['IpPermissionsEgress']
    if outbound_rules:
        AWSClient().ec2.revoke_security_group_egress(
            GroupId=security_group_id,
            IpPermissions=outbound_rules
        )

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

    # wait until gateway is ready
    waiter = AWSClient().ec2.get_waiter('internet_gateway_exists')
    waiter.wait(InternetGatewayIds=[gateway_id])

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
        if 'vpc' in state and vpc_id in state['vpc']:
            if 'gateway' in state['vpc'][vpc_id] and gateway_id in state['vpc'][vpc_id]['gateway']:
                state['vpc'][vpc_id]['gateway'].remove(gateway_id)
                aws_set_state(state)

def aws_elastic_ip_allocate(vpc_id, tag_name='Dataverse-Elastic-IP'):
    """
    Allocate an elastic ip.
    """
    elastic_ip = AWSClient().ec2.allocate_address(Domain='vpc')
    elastic_ip_id = elastic_ip['AllocationId']
    AWSClient().ec2.create_tags(
        Resources=[elastic_ip_id],
        Tags=[
            {'Key': 'Name', 'Value': tag_name},
        ]
    )

    # set state
    state = aws_get_state()
    if 'vpc' not in state:
        state['vpc'] = {}
    if vpc_id not in state['vpc']:
        state['vpc'][vpc_id] = {}
    if 'elastic_ip' not in state['vpc'][vpc_id]:
        state['vpc'][vpc_id]['elastic_ip'] = []

    state['vpc'][vpc_id]['elastic_ip'].append(elastic_ip_id)
    aws_set_state(state)

    # TODO: wait until elastic ip is ready
    ...

    return elastic_ip_id

def aws_elastic_ip_release(vpc_id, elastic_ip_id):
    if isinstance(elastic_ip_id, str):
        elastic_ip_ids = [elastic_ip_id]
    elif isinstance(elastic_ip_id, list):
        elastic_ip_ids = elastic_ip_id

    for elastic_ip_id in elastic_ip_ids:
        try:
            AWSClient().ec2.release_address(AllocationId=elastic_ip_id)
            state = aws_get_state()
            if 'vpc' in state and vpc_id in state['vpc']:
                if 'elastic_ip' in state['vpc'][vpc_id] and elastic_ip_id in state['vpc'][vpc_id]['elastic_ip']:
                    state['vpc'][vpc_id]['elastic_ip'].remove(elastic_ip_id)
                    aws_set_state(state)
        except AWSClient().ec2.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'InvalidAllocationID.NotFound':
                print(f"Elastic IP id {elastic_ip_id} doesn't exist.")
            else:
                raise e
        except Exception as e:
            raise e

def aws_nat_gateway_create(
    vpc_id,
    subnet_id,
    elastic_ip_id,
    tag_name='Dataverse-NAT-Gateway'
):
    """
    Create a NAT gateway for private subnet.
    """
    # create NAT gateway
    nat_gateway = AWSClient().ec2.create_nat_gateway(
        AllocationId=elastic_ip_id,
        SubnetId=subnet_id,
    )
    nat_gateway_id = nat_gateway['NatGateway']['NatGatewayId']

    # set tag
    AWSClient().ec2.create_tags(
        Resources=[nat_gateway_id],
        Tags=[
            {'Key': 'Name', 'Value': tag_name},
        ]
    )

    # set state
    state = aws_get_state()
    if 'vpc' not in state:
        state['vpc'] = {}
    if vpc_id not in state['vpc']:
        state['vpc'][vpc_id] = {}
    if 'nat_gateway' not in state['vpc'][vpc_id]:
        state['vpc'][vpc_id]['nat_gateway'] = []

    state['vpc'][vpc_id]['nat_gateway'].append(nat_gateway_id)
    aws_set_state(state)

    # wait until NAT gateway is ready
    waiter = AWSClient().ec2.get_waiter('nat_gateway_available')
    waiter.wait(NatGatewayIds=[nat_gateway_id])

    return nat_gateway_id

def aws_nat_gateway_delete(vpc_id, nat_gateway_id):
    if isinstance(nat_gateway_id, str):
        nat_gateway_ids = [nat_gateway_id]
    elif isinstance(nat_gateway_id, list):
        nat_gateway_ids = nat_gateway_id

    for nat_gateway_id in nat_gateway_ids:
        # delete NAT gateway
        AWSClient().ec2.delete_nat_gateway(NatGatewayId=nat_gateway_id)

        # set state
        state = aws_get_state()
        if 'vpc' in state and vpc_id in state['vpc']:
            if 'nat_gateway' in state['vpc'][vpc_id] and nat_gateway_id in state['vpc'][vpc_id]['nat_gateway']:
                state['vpc'][vpc_id]['nat_gateway'].remove(nat_gateway_id)
                aws_set_state(state)

        # wait until NAT gateway is deleted
        waiter = AWSClient().ec2.get_waiter('nat_gateway_deleted')
        waiter.wait(NatGatewayIds=[nat_gateway_id])

def aws_route_table_create(
    vpc_id,
    gateway_id=None,
    nat_gateway_id=None,
    tag_name='Dataverse-Route-Table',
    destination_cidr_block='0.0.0.0/0',
):
    """
    Create a route table for subnet.
    """
    route_table = AWSClient().ec2.create_route_table(VpcId=vpc_id)
    route_table_id = route_table['RouteTable']['RouteTableId']
    args = {
        'DestinationCidrBlock': destination_cidr_block,
        'RouteTableId': route_table_id,
    }
    if gateway_id is not None:
        args['GatewayId'] = gateway_id
    if nat_gateway_id is not None:
        args['NatGatewayId'] = nat_gateway_id

    AWSClient().ec2.create_route(**args)
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

    # TODO: wait until route table is ready
    #       didn't found waiter for route table
    ...

    return route_table_id

def aws_route_table_delete(vpc_id, route_table_id):
    if isinstance(route_table_id, str):
        route_table_ids = [route_table_id]
    elif isinstance(route_table_id, list):
        route_table_ids = route_table_id

    for route_table_id in route_table_ids:
        AWSClient().ec2.delete_route_table(RouteTableId=route_table_id)
        state = aws_get_state()
        if 'vpc' in state and vpc_id in state['vpc']:
            if 'route_table' in state['vpc'][vpc_id] and route_table_id in state['vpc'][vpc_id]['route_table']:
                state['vpc'][vpc_id]['route_table'].remove(route_table_id)
                aws_set_state(state)

def aws_route_table_asscociate_subnet(subnet_id, route_table_id):
    route_table = boto3.resource('ec2').RouteTable(route_table_id)
    route_table.associate_with_subnet(SubnetId=subnet_id)

def aws_s3_path_parse(path):
    """
    parse aws s3 path to bucket and key
    """
    aws_s3_matched = re.match(r's3[a,n]?://([^/]+)/(.*)', path)
    if aws_s3_matched:
        bucket = aws_s3_matched.group(1)
        path = aws_s3_matched.group(2)
    else:
        raise Exception(f"Invalid S3 path: {path}")

    return bucket, path

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

def aws_s3_delete_bucket(bucket):
    """
    delete aws s3 bucket

    Args:
        bucket (str): bucket name
    """
    AWSClient().s3.delete_bucket(Bucket=bucket)

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
    obj_type = aws_s3_get_object_type(bucket, key)
    if obj_type == 'folder':
        paginator = AWSClient().s3.get_paginator('list_objects')
        page_iterator = paginator.paginate(Bucket=bucket)
        for page in page_iterator:
            for obj in page['Contents']:
                bucket_key = obj['Key']

                if not bucket_key.startswith(key):
                    continue

                rel_bucket_path = bucket_key.replace(key, '')
                if rel_bucket_path.startswith('/'):
                    rel_bucket_path = rel_bucket_path[1:]

                local_file_path = os.path.join(local_path, rel_bucket_path)
                os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
                AWSClient().s3.download_file(bucket, bucket_key, local_file_path)
    elif obj_type == 'file':
        AWSClient().s3.download_file(bucket, key, local_path)
    elif obj_type == 'no_obj':
        raise Exception(f"Object doesn't exist: {key}")

def aws_s3_upload(bucket, key, local_path):
    """
    Args:
        bucket (str): bucket name
        key (str): key (aws s3 file path)
        local_path (str): local path to save file

    Usage:
        aws_s3_upload('tmp', 'this/is/path.json', 'path.json')
    """
    if os.path.isdir(local_path):
        files = glob.glob(os.path.join(local_path, "*/*"))
        for file in files:
            rel_path = os.path.relpath(file, local_path)
            _key = os.path.join(key, rel_path)

            # NOTE: no need to upload folder
            if os.path.isdir(file):
                continue

            AWSClient().s3.upload_file(file, bucket, _key)
    else:
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

def aws_s3_delete(bucket, key):
    """
    Args:
        bucket (str): bucket name
        key (str): key (aws s3 file path)

    Usage:
        aws_s3_delete('tmp', 'this/is/path.json')
    """
    obj_type = aws_s3_get_object_type(bucket, key)

    if obj_type == 'folder':
        paginator = AWSClient().s3.get_paginator('list_objects')
        page_iterator = paginator.paginate(Bucket=bucket)
        for page in page_iterator:
            for obj in page['Contents']:
                bucket_key = obj['Key']

                if not bucket_key.startswith(key):
                    continue

                AWSClient().s3.delete_object(Bucket=bucket, Key=bucket_key)
    elif obj_type == 'file':
        AWSClient().s3.delete_object(Bucket=bucket, Key=key)
    elif obj_type == 'no_obj':
        raise Exception(f"Object doesn't exist: {key}")

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

def aws_s3_get_object_type(bucket, key):
    """
    get object type from s3

    NOTE:
        S3 don't have a concept of folder
        so this is a hardcoded solution to check key is file/folder or doesn't exist

    TODO:
        if there is edge case that this function doesn't cover
        please add it to the test case
    """
    results = AWSClient().s3.list_objects_v2(
        Bucket=bucket,
        Prefix=key,
        Delimiter="/",
    )
    if 'CommonPrefixes' in results:
        prefix_folders = results['CommonPrefixes'][0]['Prefix'].split('/')
        key_folders = key.split('/')

        # remove ''
        prefix_folders = [x for x in prefix_folders if x != '']
        key_folders = [x for x in key_folders if x != '']

        # check key exacly match prefix
        for key_folder in key_folders:
            if key_folder not in prefix_folders:
                return 'no_obj'
        return 'folder'
    elif 'Contents' in results:
        content = results['Contents'][0]['Key']
        if content == key:
            if content.endswith('/'):
                return 'folder'
            else:
                return 'file'
        else:
            return 'folder'
    else:
        return 'no_obj'