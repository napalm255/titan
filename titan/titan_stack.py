from aws_cdk import (
    Duration,
    Stack,
    aws_sqs as sqs,
    aws_s3 as s3,
    aws_secretsmanager as secretsmanager,
)
from constructs import Construct
import json

class TitanStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        conf_bucket = s3.Bucket(self, 'confBucket')

        git_credentials = {
            'url': 'https://github.com/napalm255/titan-conf',
            'username': 'blah',
            'password': 'ugh'
        }

        git_secret = secretsmanager.CfnSecret(self, 'gitSecret',
            description="Git Repository and Credentials for Titan Configuration.",
            kms_key_id='aws/secretsmanager',
            secret_string=json.dumps(git_credentials)
        )

        # lambda for syncing github to s3
