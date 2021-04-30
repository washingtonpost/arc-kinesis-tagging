import boto3
import os
from dotenv import load_dotenv
load_dotenv()

def assume_role(): 
    access_key, access_secret = None, None
    if os.environ.get('access_key') and os.environ.get('access_secret'):
        access_key = os.environ.get('access_key')
        access_secret = os.environ.get('access_secret')
    else:
        # reads credentials from your ~/.aws/credentials file
        # credentials should belong to your target AWS account owned by your organization; these should not belong to an Arc account 
        # to learn more about setting up your own credentials, see https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html
        # to ensure you're using the correct AWS credential profile, run export AWS_PROFILE=your_profile_name_here
        try:
            credentials = boto3.Session().get_credentials()
            access_key = credentials.access_key 
            access_secret = credentials.secret_key
        except:
            raise Exception("Could not find AWS Profile. Did you export your AWS_PROFILE?")

    if access_key is None or access_secret is None:
        raise Exception("No Access Key or Access Secret provided. Please either set these values in .env or in an AWS Profile. See README for more information.")

    # creates an STS client that will be used to assume a limited role within the Arc AWS Account
    try:
        sts_client = boto3.client('sts', os.environ.get('region'), 
                        aws_access_key_id=credentials.access_key, 
                        aws_secret_access_key=credentials.secret_key
                    )

        sts_response = sts_client.assume_role(
                        RoleArn=os.environ.get('role_arn'),
                        RoleSessionName='kinesis-customer-sample',
                        PolicyArns=[
                            {
                                'arn': os.environ.get('policy_arn')
                            }
                        ])
    except:
        raise Exception("Unable to assume Arc IAM Role. First, validate that your AWS Credentials are correct. If the issue persists, validate that your ARNs & region are configured in your .env file & that the values match those given to you by Arc.")

    return sts_response


def create_kinesis_client(sts_response): 
    try: 
        kinesis_client = boto3.client('kinesis', os.environ.get('region'), 
                        aws_access_key_id=sts_response.get('Credentials').get('AccessKeyId'), 
                        aws_secret_access_key=sts_response.get('Credentials').get('SecretAccessKey'), 
                        aws_session_token=sts_response.get('Credentials').get('SessionToken'))
    except:
        raise Exception("Unable to create a Kinesis Client. Please validate that your ARNs & region are configured in your .env file & that the values match those given to you by Arc.")

    return kinesis_client

def describe_stream(kinesis_client, stream_name):
    try: 
        stream_info = kinesis_client.describe_stream(StreamName=stream_name)
        shard_id = stream_info['StreamDescription']['Shards'][0]['ShardId']
        all_shards = kinesis_client.list_shards(StreamName=stream_name)
    except:
        raise Exception("Unable to connect to Kinesis Stream. Please validate that your stream_name is configured in your .env file & that the value matches the stream name given to you by Arc.")
    return all_shards.get('Shards')