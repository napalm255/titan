"""
Titan Websocker Authorizer.
"""

import json
import logging

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


def lambda_handler(event, context):
    """
    Lambda handler.

    :param event: Event data.
    :param context: Context data.
    :return: Authorizer response.
    """
    # pylint: disable=unused-argument
    logger.info(event)

    if check_token(event['headers']['Auth']):
        response = generate_allow('titan-agent', event['methodArn'])
        logger.info(response)
        logger.info('authorized')
        return json.loads(response)

    response = generate_deny('titan-agent', event['methodArn'])
    logger.info(response)
    logger.error('unauthorized')
    return json.loads(response)
