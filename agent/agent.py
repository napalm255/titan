"""
Pipeline Execution Agent

The agent is responsible for the following actions:
    - Dynamically generating an agent ID based on system hostname and initial timestamp
    - Register the agent via an AWS Event Bridge message
    - Deregister the agent on exit via an AWS Event Bridge message
    - Listen for events on AWS SQS sent via Event Bridge
    - Execute the defined task (in BASH) in an isolated shell
    - Capture the task output in realtime to cloudwatch logs
"""
import os
import sys
import time
import json
import logging
import subprocess
import threading
import traceback
from datetime import datetime
from typing import Dict, Any
from botocore.exceptions import ClientError
from botocore.config import Config
import boto3
import boto3.session
import botocore
import botocore.exceptions

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
AGENT_ID = f"{os.uname().nodename}-{int(time.time())}"
SQS_QUEUE_URL = os.environ.get("SQS_QUEUE_URL")
REGION = os.environ.get("REGION")
TASK_TIMEOUT = int(os.environ.get("TASK_TIMEOUT", 60))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", 3))
AWS_CONFIG = Config(region_name=REGION)
AWS_SESSION = boto3.session.Session(region_name=REGION)
SQS_CLIENT = AWS_SESSION.client("sqs", config=AWS_CONFIG)
EVENTS_CLIENT = AWS_SESSION.client("events", config=AWS_CONFIG)

def register_agent(agent_id: str) -> None:
    """
    Register the agent with the event bridge
    """
    try:
        response = EVENTS_CLIENT.put_events(
            Entries=[
                {
                    "Detail": json.dumps({"agent_id": agent_id}),
                    "DetailType": "AgentRegistration",
                    "EventBusName": "default",
                    "Source": "agent"
                }
            ]
        )
        logger.info(f"Agent {agent_id} registered")
    except ClientError as e:
        logger.error(f"Error registering agent {agent_id}: {e}")
        sys.exit(1)

def deregister_agent(agent_id: str) -> None:
    """
    Deregister the agent with the event bridge
    """
    try:
        response = EVENTS_CLIENT.put_events(
            Entries=[
                {
                    "Detail": json.dumps({"agent_id": agent_id}),
                    "DetailType": "AgentDeregistration",
                    "EventBusName": "default",
                    "Source": "agent"
                }
            ]
        )
        logger.info(f"Agent {agent_id} deregistered")
    except ClientError as e:
        logger.error(f"Error deregistering agent {agent_id}: {e}")
        sys.exit(1)

def execute_task(task: Dict[str, Any]) -> None:
    """
    Execute the task in an isolated shell
    """
    task_id = task.get("task_id")
    command = task.get("command")
    logger.info(f"Executing task {task_id} with command: {command}")
    try:
        process = subprocess.Popen(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        start_time = time.time()
        while process.poll() is None:
            output = process.stdout.readline().decode("utf-8").strip()
            if output:
                logger.info(f"Task {task_id} output: {output}")
            if time.time() - start_time > TASK_TIMEOUT:
                logger.error(f"Task {task_id} timed out")
                process.kill()
                break
        process.wait()
        logger.info(f"Task {task_id} completed with exit code {process.returncode}")
    except Exception as e:
        logger.error(f"Error executing task {task_id}: {e}")
        sys.exit(1)
