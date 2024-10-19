"""
Agent
"""

import logging
import sys
import os
import stat
import uuid
import json
import watchtower
import sh
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError


def wait_for_message(queue_url, logger):
    """Wait for message."""
    sqs = boto3.client('sqs')

    while True:
        response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=5,
                VisibilityTimeout=5,
                MessageAttributeNames=['All'],
                MessageSystemAttributeNames=['MessageGroupId',
                                             'MessageDeduplicationId']
        )

        if 'Messages' not in response:
            logger.debug({'status': 'query returned no messages'})
            continue
        logger.info(json.dumps({**{'status': 'query', 'response': {**response}}}))

        try:
            message = response['Messages'][0]
            message_id = message['MessageId']
            receipt_handle = message['ReceiptHandle']
            attributes = message['MessageAttributes']
            tags = message['Attributes']['MessageGroupId']
            job_id = attributes['job_id']['StringValue']
            execution_id = message['Attributes']['MessageDeduplicationId']
            job = json.loads(message['Body'])
        except KeyError as ex:
            logger.critical(json.dumps({'status': 'failed query', 'reason': ex}))
            return {}

        return {
            'message_id': message_id,
            'receipt_handle': receipt_handle,
            'tags': tags,
            'job': job,
            'job_id': job_id,
            'execution_id': execution_id,
        }


def delete_message(queue_url, message, logger):
    """Delete queue message."""
    sqs = boto3.client('sqs')
    try:
        receipt_handle = message['receipt_handle']
        logger.info(json.dumps({**{'status': 'deleting message'}, **message}))
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        logger.info(json.dumps({**{'status': 'deleted message'}, **message}))
    except (KeyError, ClientError):
        logger.critical(json.dumps({**{'status': 'failed to delete message'}, **message}))
        return False
    return True


def process_message(command, logger):
    """Run a shell command and log its output in near real-time to cloudwatch."""
    if not os.path.isfile(command):
        logger.error('file does not exist')
        return

    try:
        for line in sh.Command(command)(_iter=True, _err_to_out=True):
            logger.info(line.strip())
        logger.info('exit code: 0')
    except sh.ErrorReturnCode as e:
        logger.error('exit code: %s', e.exit_code)


def register_agent(table, agent_id, tags, logger, count=0):
    """Register agent in dynamodb."""
    ddb = boto3.resource('dynamodb')
    table = ddb.Table(table)
    try:
        logger.info(json.dumps({'status': 'registering agent', 'agent_id': agent_id}))
        response = table.put_item(
            Item={
                'agent_id': agent_id,
                'tags': tags,
                'status': 'online',
                'jobs_executed': count
            }
        )
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            logger.info(json.dumps({'status': 'registered agent', 'agent_id': agent_id}))
            return True
    except ClientError:
        logger.info(json.dumps({'status': 'agent registration failed', 'agent_id': agent_id}))
        return False
    return False


def deregister_agent(table, agent_id, logger):
    """Deregister agent in dynamodb."""
    ddb = boto3.resource('dynamodb')
    table = ddb.Table(table)
    try:
        logger.info(json.dumps({'status': 'deregistering agent', 'agent_id': agent_id}))
        response = table.delete_item(
            Key={
                'agent_id': agent_id
            }
        )
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            logger.info(json.dumps({'status': 'deregistered agent', 'agent_id': agent_id}))
            return True
    except ClientError:
        logger.info(json.dumps({'status': 'agent deregistration failed', 'agent_id': agent_id}))
        return False
    return False


def logging_handler(log_group_name, log_stream_name):
    """Create logging handler."""
    return watchtower.CloudWatchLogHandler(
        log_group=log_group_name,
        stream_name=log_stream_name,
        create_log_group=True,
        create_log_stream=True,
        use_queues=True,
        send_interval=5,
        max_batch_count=5
    )

def logging_config(log_group_name, log_stream_name):
    """Return logging configuration."""
    return {
        'log_group_name': log_group_name,
        'log_stream_name': log_stream_name
    }


def flush_loggers(loggers):
    """Flush and close handlers for loggers."""
    for logger in loggers:
        for handler in logger.handlers:
            handler.flush()
            handler.close()


def job_setup(job_dir):
    """Setup job directory."""
    try:
        os.makedirs(job_dir, exist_ok=True)
        return True
    except OSError:
        return False


def job_download(bucket, key, local_dir):
    """Download script from s3."""
    s3 = boto3.client('s3')
    local_file = f'{local_dir}/{key}'
    try:
        # Download file from s3
        s3.download_file(bucket, key, local_file)
        # Set script to executable
        current_permissions = os.stat(local_file).st_mode
        os.chmod(local_file, current_permissions | stat.S_IXUSR)
        return True
    except NoCredentialsError:
        print("Credentials not available.")
    except PartialCredentialsError:
        print("Incomplete credentials provided.")
    except ClientError as e:
        print(f"An error occurred: {e}")
    return True


def generate_agent_id():
    """Generate and return agent id."""
    return str(uuid.uuid4())


def get_account_id():
    """Get and return account id."""
    sts = boto3.client('sts')
    return sts.get_caller_identity()['Account']


def load_configuration():
    """Get and return configuration."""
    # Account Id
    account_id = get_account_id()

    # Region
    region = 'us-east-1'
    region = os.getenv('AWS_REGION', region)
    if os.getenv('AWS_DEFAULT_REGION', region):
        region = os.getenv('AWS_DEFAULT_REGION')

    # Queue
    queue_name = 'jobs.fifo'
    queue_name = os.getenv('AGENT_QUEUE_NAME', queue_name)
    queue_url = f'https://sqs.{region}.amazonaws.com/{account_id}/{queue_name}'
    queue_url = os.getenv('AGENT_QUEUE_URL', queue_url)

    # Tags
    tags = 'unknown'
    tags = os.getenv('AGENT_TAGS', tags)

    return {
        'agent_id': generate_agent_id(),
        'account_id': account_id,
        'region': region,
        'queue_url': queue_url,
        'tags': tags
    }


def main():
    """Entrypoint."""
    logging.basicConfig(level=logging.INFO)

    # Agent configuration
    conf = load_configuration()

    # Configure agent logging
    agent_logger = logging.getLogger('agent')
    agent_logger.addHandler(
        logging_handler(**logging_config(
            'agents', f'agent/{conf["agent_id"]}'
        ))
    )

    # Register agent
    if not register_agent(table='agents', agent_id=conf['agent_id'],
                          tags=conf['tags'], logger=agent_logger, count=0):
        sys.exit(1)

    # Wait for messages and process
    try:
        job_execution_count = 0
        while True:
            # Wait for a message
            message = wait_for_message(conf['queue_url'], agent_logger)
            if not message:
                continue
            agent_logger.info({**{'status': 'received'}, **message})

            job_id = message['job_id']
            execution_id = message['execution_id']

            # Job Directory
            home_dir = os.path.expanduser("~")
            job_dir = f'{home_dir}/tmp/agent/{execution_id}'

            # Configure job logging
            job_logger = logging.getLogger('job')
            job_logger.addHandler(
                logging_handler(**logging_config(
                    'jobs', f'job/{job_id}/{execution_id}'
                ))
            )

            # Delete message
            if not delete_message(conf['queue_url'], message, agent_logger):
                continue

            # Reregister with updated count
            job_execution_count += 1
            register_agent(
                table='agents',
                agent_id=conf['agent_id'],
                tags=conf['tags'],
                logger=agent_logger,
                count=job_execution_count
            )

            # Process message
            try:
                bucket = message['job']['bucket']
                key = message['job']['key']
            except KeyError:
                logging.critical(json.dumps({**{'status': 'error'}, **message}))
                continue

            agent_logger.info(json.dumps({**{'status': 'processing'}, **message}))
            if not job_setup(job_dir):
                agent_logger.critical(json.dumps(
                    {**{'status': 'failed to create tmp directory'}, **message}
                ))
            if not job_download(bucket, key, job_dir):
                agent_logger.critical(json.dumps(
                    {**{'status': 'no job script'}, **message}
                ))
            job_script = f'{job_dir}/{key}'
            process_message(job_script, job_logger)
            agent_logger.info(json.dumps({**{'status': 'processed'}, **message}))

            # Flush loggers
            flush_loggers([job_logger, agent_logger])
    except KeyboardInterrupt:
        # Deregister agent
        deregister_agent(
            table='agents',
            agent_id=conf['agent_id'],
            logger=agent_logger
        )
        sys.exit(0)

if __name__ == '__main__':
    main()
