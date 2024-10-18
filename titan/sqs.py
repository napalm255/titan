"""
Titan SQS to WebSocket.

Handles messages from SQS and sends them to a WebSocket connection.
"""

import os
import json
import logging
import boto3

logger = logging.getLogger()
logger.setLevel("INFO")


def send_message_to_websocket(message, connection_id):
    """
    Send a message to a WebSocket agent using the API Gateway Management API.

    param message: The message to send.
    param connection_id: The connection ID of the WebSocket client.
    :return: None
    """

    wsconn_url = os.environ.get('WEBSOCKET_CONNECTIONS_URL')
    client = boto3.client('apigatewaymanagementapi', endpoint_url=wsconn_url)

    try:
        response = client.post_to_connection(
            ConnectionId=connection_id,
            Data=message
        )
        logger.info("Message sent to WebSocket: %s", response)
        return response
    except client.exceptions.GoneException as e:
        logger.error("Connection %s is gone: %s", connection_id, e)
        return None
    except Exception as e:  # pylint: disable=broad-except
        logger.error("Error sending message to WebSocket: %s", e)
        return None



def lambda_handler(event, context):
    """
    Process messages triggered by SQS.

    param event: The event that triggered the function.
    param context: The context of the function.
    :return: None
    """
    # pylint: disable=unused-argument
    logger.info(event)

    for record in event['Records']:
        message = json.loads(record['body'])
        connection_id = message['connection_id']
        message = message['message']

        send_message_to_websocket(message, connection_id)

    return {
        'statusCode': 200,
        'body': json.dumps('SQS event processed successfully!')
    }
