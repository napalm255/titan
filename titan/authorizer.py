"""
Titan Agent WebSocket Authorizer.
"""

import json
import logging
from utils.event import Event, EventBridgeHelper

logger = logging.getLogger()
logger.setLevel("INFO")


def generate_policy(principal_id, effect, resource):
    """
    Generate policy.

    :param principal_id: Principal ID.
    :param effect: Effect.
    :param resource: Resource.
    :return: Policy.
    """
    auth_response = {}
    auth_response['principalId'] = principal_id
    auth_response['context'] = {
      "principalId": principal_id
    }

    if (effect and resource):
        policy_document = {}
        policy_document['Version'] = '2012-10-17'
        policy_document['Statement'] = []
        statement = {}
        statement['Action'] = 'execute-api:Invoke'
        statement['Effect'] = effect
        statement['Resource'] = resource
        policy_document['Statement'] = [statement]
        auth_response['policyDocument'] = policy_document

    auth_response_json = json.dumps(auth_response)
    return auth_response_json


def generate_allow(principal_id, resource):
    """
    Generate allow policy.

    :param principal_id: Principal ID.
    :param resource: Resource.
    :return: Allow policy.
    """
    return generate_policy(principal_id, 'Allow', resource)


def generate_deny(principal_id, resource):
    """
    Generate deny policy.

    :param principal_id: Principal ID.
    :param resource: Resource.
    :return: Deny policy.
    """
    return generate_policy(principal_id, 'Deny', resource)


def check_token(token):
    """
    Check if token is valid.

    :param token: Token to check.
    :return: True if token is valid, False otherwise.
    """
    tokens = [
        "5236e26b-f221-4984-985c-793525ddf82b"
    ]
    if token in tokens:
        return True
    return False


def check_query_string_parameters(query_string_parameters):
    """
    Check if query string parameters are valid.

    :param query_string_parameters: Query string parameters.
    :return: True if query string parameters are valid, False otherwise.
    """
    required_parameters = [
        'host',
        'os',
        'labels'
    ]
    for param in required_parameters:
        if param not in query_string_parameters:
            return False
    return True


def lambda_handler(event, context):
    """
    Lambda handler.

    :param event: Event data.
    :param context: Context data.
    :return: Authorizer response.
    """
    # pylint: disable=unused-argument
    logger.info(event)

    token_valid = check_token(event['headers']['Auth'])
    params_valid= check_query_string_parameters(event['queryStringParameters'])

    if token_valid and params_valid:
        response = generate_allow('titan-agent', event['methodArn'])
        logger.info(response)
        logger.info('authorized')
        event = EventBridgeHelper().send(
                  Event(
                    source='titan-authorizer',
                    detail_type='Authorization',
                    detail={
                        'status': 'authorized',
                    }
                  )
                )
    else:
        response = generate_deny('titan-agent', event['methodArn'])
        logger.info(response)
        logger.error('unauthorized')

    return json.loads(response)
