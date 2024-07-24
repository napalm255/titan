import aws_cdk as core
import aws_cdk.assertions as assertions

from titan.titan_stack import TitanStack

def test_sqs_queue_created():
    app = core.App()
    stack = TitanStack(app, "titan")
    template = assertions.Template.from_stack(stack)

    template.has_resource_properties("AWS::SQS::Queue", {
        "VisibilityTimeout": 300
    })
