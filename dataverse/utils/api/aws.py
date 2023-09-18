

import boto3


def aws_check_credentials():
    sts = boto3.client('sts')
    sts.get_caller_identity()