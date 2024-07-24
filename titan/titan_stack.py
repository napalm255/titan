from aws_cdk import (
    Duration,
    Stack,
    aws_sqs as sqs,
    aws_lambda as _lambda,
)
from constructs import Construct

class TitanStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        tasks_queue = sqs.Queue(
            self, 'cfnTasksQueue',
            queue_name='tasks.fifo',
            fifo=True,
            enforce_ssl=True,
            visibility_timeout=Duration.minutes(5),
            content_based_deduplication=False,
            delivery_delay=Duration.seconds(0),
            retention_period=Duration.days(4),
            encryption=sqs.QueueEncryption.SQS_MANAGED,
            deduplication_scope=sqs.DeduplicationScope.MESSAGE_GROUP,
            fifo_throughput_limit=sqs.FifoThroughputLimit.PER_MESSAGE_GROUP_ID
        )

