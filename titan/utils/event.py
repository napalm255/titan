"""
Event Bridge Utility
"""
import os
from typing import Any, Dict
import json
import logging
from pydantic import BaseModel,	Field, ValidationError
import boto3
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel("INFO")


class Event(BaseModel):
    """
    EventBridge event.
    """

    source: str = Field(title="Source", min_length=1)
    detail_type: str = Field(title="DetailType", min_length=1)
    detail: Dict[str, Any] = Field(title="Detail")
    event_bus_name: str = Field(title="EventBusName",
                                default=os.environ.get('EVENT_BUS_NAME', 'default'))
    resources: list = Field(title="Resources", default=[])

    def entry(self) -> Dict[str, Any]:
        """
        Return the event as an EventBridge entry.
        """
        event_dict = {}
        for field_name, field_info in self.__fields__.items():
            # Get the title from the field metadata
            title = field_info.title
            # Get the value of the field
            value = getattr(self, field_name)

            # Convert 'detail' field to JSON string as required by EventBridge
            if field_name == "detail":
                value = json.dumps(value)

            event_dict[title] = value

        return event_dict


class EventBridgeHelper:
    """
    Helper class to send events to AWS EventBridge.
    """
    # pylint: disable=too-few-public-methods
    def __init__(self):
        self.client = boto3.client('events')
        self.event_bridge_name = os.environ.get('EVENT_BUS_NAME', 'default')

    def send(self, event: Event) -> Dict[str, Any]:
        """
        Send an event to AWS EventBridge.

        :param event: Event to send.
        :return: Response from AWS EventBridge.
        """
        try:
            response = self.client.put_events(
                Entries=[event.entry()]
            )

            logger.info("Event sent successfully: %s", response)
            return response

        except ValidationError as ve:
            logger.error("Validation error: %s", ve)
            raise
        except ClientError as ce:
            logger.error("Failed to send event: %s", ce)
            raise
