"""
Titan Websocker Authorizer.
"""

import json
import logging
import os

logger = logging.getLogger()
logger.setLevel("INFO")


def generate_policy(principal_id, effect, resource):
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
    return generate_policy(principal_id, 'Allow', resource)


def generate_deny(principal_id, resource):
    return generate_policy(principal_id, 'Deny', resource)


def lambda_handler(event, context):
    logger.info(event)
    token = "password123"

    if event['headers']['Auth'] == token:
        response = generate_allow('titan-agent', event['methodArn'])
        logger.info(response)
        logger.info('authorized')
        return json.loads(response)
    else:
        response = generate_deny('titan-agent', event['methodArn'])
        logger.info(response)
        logger.error('unauthorized')
        return json.loads(response)
