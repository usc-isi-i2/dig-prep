import boto
from boto.regioninfo import RegionInfo

"""
Add a section like this in your boto config file::

    [Credentials]
    govcloud_access_key = <access key>
    govcloud_secret_key = <secret key>

And then you should be able to do this::

    >>> import govcloud
    >>> ec2 = govcloud.connect_ec2()

"""

def get_govcloud_creds(access_key=None, secret_key=None):
    """
    If you place something like this in your boto config file:

    [Credentials]
    govcloud_access_key = <access key>
    govcloud_secret_key = <secret key>

    This function will find them and return them.
    """
    if not access_key:
        access_key = boto.config.get('Credentials',
                                     'govcloud_access_key',
                                     None)
    if not secret_key:
        secret_key = boto.config.get('Credentials',
                                     'govcloud_secret_key',
                                     None)
    return (access_key, secret_key)


def connect_ec2(aws_access_key_id=None, aws_secret_access_key=None, **kwargs):
    """
    :type aws_access_key_id: string
    :param aws_access_key_id: Your AWS Access Key ID

    :type aws_secret_access_key: string
    :param aws_secret_access_key: Your AWS Secret Access Key

    :rtype: :class:`boto.ec2.connection.EC2Connection`
    :return: A connection to Amazon's EC2
    """
    access_key, secret_key = get_govcloud_creds(aws_access_key_id,
                                                aws_secret_access_key)
    region = RegionInfo(name='govcloud',
                        endpoint='ec2.us-gov-west-1.amazonaws.com')
    from boto.ec2.connection import EC2Connection
    return EC2Connection(access_key, secret_key,
                         region=region, **kwargs)


def connect_vpc(aws_access_key_id=None, aws_secret_access_key=None, **kwargs):
    """
    :type aws_access_key_id: string
    :param aws_access_key_id: Your AWS Access Key ID

    :type aws_secret_access_key: string
    :param aws_secret_access_key: Your AWS Secret Access Key

    :rtype: :class:`boto.vpc.VPCConnection`
    :return: A connection to VPC
    """
    access_key, secret_key = get_govcloud_creds(aws_access_key_id,
                                                aws_secret_access_key)
    region = RegionInfo(name='govcloud',
                        endpoint='ec2.us-gov-west-1.amazonaws.com')
    from boto.vpc import VPCConnection
    return VPCConnection(access_key, secret_key,
                         region=region, **kwargs)


def connect_cloudwatch(aws_access_key_id=None, aws_secret_access_key=None,
                       **kwargs):
    """
    :type aws_access_key_id: string
    :param aws_access_key_id: Your AWS Access Key ID

    :type aws_secret_access_key: string
    :param aws_secret_access_key: Your AWS Secret Access Key

    :rtype: :class:`boto.ec2.cloudwatch.CloudWatchConnection`
    :return: A connection to Amazon's EC2 Monitoring service
    """
    access_key, secret_key = get_govcloud_creds(aws_access_key_id,
                                                aws_secret_access_key)
    from boto.ec2.cloudwatch import CloudWatchConnection
    region = RegionInfo(name='govcloud',
                        endpoint='monitoring.us-gov-west-1.amazonaws.com')
    return CloudWatchConnection(access_key, secret_key,
                                region=region, **kwargs)


def connect_iam(aws_access_key_id=None, aws_secret_access_key=None, **kwargs):
    """
    :type aws_access_key_id: string
    :param aws_access_key_id: Your AWS Access Key ID

    :type aws_secret_access_key: string
    :param aws_secret_access_key: Your AWS Secret Access Key

    :rtype: :class:`boto.iam.IAMConnection`
    :return: A connection to Amazon's IAM
    """
    access_key, secret_key = get_govcloud_creds(aws_access_key_id,
                                                aws_secret_access_key)
    from boto.iam import IAMConnection
    return IAMConnection(access_key, secret_key,
                         host='iam.us-gov.amazonaws.com', **kwargs)


def connect_s3(aws_access_key_id=None, aws_secret_access_key=None, **kwargs):
    """
    :type aws_access_key_id: string
    :param aws_access_key_id: Your AWS Access Key ID

    :type aws_secret_access_key: string
    :param aws_secret_access_key: Your AWS Secret Access Key

    :rtype: :class:`boto.s3.connection.S3Connection`
    :return: A connection to Amazon's S3
    """
    access_key, secret_key = get_govcloud_creds(aws_access_key_id,
                                                aws_secret_access_key)
    from boto.s3.connection import S3Connection
    return S3Connection(access_key, secret_key,
                        host='s3-us-gov-west-1.amazonaws.com', **kwargs)


def connect_s3fips(aws_access_key_id=None, aws_secret_access_key=None,
                   **kwargs):
    """
    :type aws_access_key_id: string
    :param aws_access_key_id: Your AWS Access Key ID

    :type aws_secret_access_key: string
    :param aws_secret_access_key: Your AWS Secret Access Key

    :rtype: :class:`boto.s3.connection.S3Connection`
    :return: A connection to Amazon's S3
    """
    access_key, secret_key = get_govcloud_creds(aws_access_key_id,
                                                aws_secret_access_key)
    from boto.s3.connection import S3Connection
    return S3Connection(access_key, secret_key,
                        host='s3-fips-us-gov-west-1.amazonaws.com', **kwargs)


def connect_sts(aws_access_key_id=None, aws_secret_access_key=None, **kwargs):
    """
    :type aws_access_key_id: string
    :param aws_access_key_id: Your AWS Access Key ID

    :type aws_secret_access_key: string
    :param aws_secret_access_key: Your AWS Secret Access Key

    :rtype: :class:`boto.sts.STSConnection`
    :return: A connection to Amazon's STS
    """
    access_key, secret_key = get_govcloud_creds(aws_access_key_id,
                                                aws_secret_access_key)
    from boto.sts import STSConnection
    region = RegionInfo(name='govcloud',
                        endpoint='ec2.us-gov-west-1.amazonaws.com')
    return STSConnection(access_key, secret_key,
                         region=region, **kwargs)

def connect_sqs(aws_access_key_id=None, aws_secret_access_key=None, **kwargs):
    """
    :type aws_access_key_id: string
    :param aws_access_key_id: Your AWS Access Key ID

    :type aws_secret_access_key: string
    :param aws_secret_access_key: Your AWS Secret Access Key

    :rtype: :class:`boto.sqs.SQSConnection`
    :return: A connection to Amazon's SQS
    """
    access_key, secret_key = get_govcloud_creds(aws_access_key_id,
                                                aws_secret_access_key)
    from boto.sqs import SQSConnection, SQSRegionInfo
    region = SQSRegionInfo(name='us-gov-west-1',
                           endpoint='sqs.us-gov-west-1.amazonaws.com')
    return SQSConnection(access_key, secret_key,
                         region=region, **kwargs)
