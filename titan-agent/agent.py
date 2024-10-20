"""
Titan Agent.
"""

import logging
import os
import sys
import json
import uuid
import platform
import asyncio
import threading
import subprocess
import urllib.parse
from typing import Optional
import websockets
import watchtower
import sh
from botocore.exceptions import ClientError


# Setup logger for CloudWatch
# pylint: disable=logging-fstring-interpolation
logging.basicConfig(level=logging.INFO)
try:
    cloudwatch_handler = watchtower.CloudWatchLogHandler(
        log_group="/titan/agents",
        stream_name=str(uuid.uuid4()),
        create_log_group=True,
        create_log_stream=True,
        use_queues=True,
        send_interval=1,
        max_batch_count=3
    )
except ClientError as error:
    logging.error(f"Error configuring agent logging: {error}")
    sys.exit(1)
logger = logging.getLogger('titan-agent')
logger.addHandler(cloudwatch_handler)


def flush_logger(_logger: logging.Logger):
    """
    Flush the logger handlers.
    """
    _logger.info("Flushing logger handlers...")
    for _handler in _logger.handlers:
        _handler.flush()
        _handler.close()


class AgentDetails:
    """
    Titan Agent Details.

    This class provides details about the agent such as the operating system, version, and
    hostname.
    """
    def __init__(self):
        self.host: str = platform.node()
        self.system: str = platform.system()
        self.os: str = os.name
        if 'ID' in platform.freedesktop_os_release():
            self.os: str = platform.freedesktop_os_release()['ID']
        self.version: str = platform.version()
        if 'VERSION_ID' in platform.freedesktop_os_release():
            self.version: str = platform.freedesktop_os_release()['VERSION_ID']
        self.labels: str = ','.join(self.get_labels())

    def get_labels(self) -> list:
        """
        Get the agent labels.
        """
        return [
            'default'
        ]

    @property
    def details(self) -> dict:
        """
        Get the agent details.

        :return: Agent details.
        """
        return self.__dict__

    def __str__(self):
        """
        Convert the agent details to a JSON string.

        :return: JSON string.
        """
        return json.dumps(self.__dict__)


class JobAgent:
    """
    Titan Job Agent.

    This agent connects to the Titan WebSocket server and listens for job messages.
    It can execute shell commands and terminate the current job.
    """

    def __init__(self, websocket_url: str, token: str, agent_details: AgentDetails):
        """
        Initialize the Job Agent.

        :param websocket_url: WebSocket URL to connect to.
        :param token: Authorization token.
        :param agent_details: Agent details.
        """
        self.agent_details: AgentDetails = agent_details
        self.websocket_url: str = websocket_url
        self.current_process: Optional[subprocess.Popen] = None
        self.job_lock = threading.Lock()
        self.headers: dict = {
            "Auth": token
        }
        self.query_params: dict = self.agent_details.details

    @property
    def url(self):
        """
        Construct the WebSocket URL with query parameters.
        """
        return f"{self.websocket_url}?{urllib.parse.urlencode(self.query_params)}"

    async def connect(self):
        """
        Connect to the WebSocket server.
        """
        async with websockets.connect(self.url, extra_headers=self.headers) as ws:
            logger.info("Connected to WebSocket")
            await self.listen(ws)

    async def listen(self, ws):
        """
        Listen for incoming messages from the WebSocket server.

        :param ws: WebSocket connection.
        """
        while True:
            try:
                message = await ws.recv()
                job_message = json.loads(message)
                logger.info(f"Received message: {job_message}")

                if job_message.get("action") == "execute":
                    await self.handle_execute(job_message["shell_command"])

                elif job_message.get("action") == "terminate":
                    await self.handle_terminate()

            except websockets.ConnectionClosedError:
                logger.error("WebSocket connection closed, attempting to reconnect...")
                await asyncio.sleep(5)
                await self.connect()

    async def handle_execute(self, shell_command: str):
        """
        Execute a shell command.

        :param shell_command: Shell command to execute.
        """
        if self.job_lock.locked():
            logger.warning("A job is already running, skipping execution")
            return

        def job_logging():
            """
            Configure logging for the agent.
            """
            try:
                cw_handler = watchtower.CloudWatchLogHandler(
                    log_group="/titan/jobs",
                    stream_name=str(uuid.uuid4()),
                    create_log_group=True,
                    create_log_stream=True,
                    use_queues=True,
                    send_interval=1,
                    max_batch_count=3
                )
            except ClientError as error:
                logging.error(f"Error configuring job logging: {error}")
                sys.exit(1)
            job_logger = logging.getLogger('titan-job')
            job_logger.addHandler(cw_handler)
            logging.info("Configured job logging")
            return job_logger

        def execute():
            job_logger = job_logging()
            with self.job_lock:
                try:
                    self.current_process = sh.Command(shell_command)(
                        _iter=True,
                        _err_to_out=True
                    )
                    for line in self.current_process:
                        job_logger.info(line.strip())
                    job_logger.info("Job completed successfully with return code: 0")
                except sh.ErrorReturnCode as error:
                    job_logger.error(f"Job failed with return code: {error.exit_code}")

            flush_logger(job_logger)

        threading.Thread(target=execute).start()

    async def handle_terminate(self):
        """
        Terminate the current job.
        """
        logger.info("Received terminate message")
        logger.info(self.job_lock.locked())
        logger.info(self.current_process.process)
        if self.job_lock.locked() and self.current_process.process:
            logger.info("Terminating the current job")
            self.current_process.process.terminate()

    def start(self):
        """
        Start the agent.
        """
        logger.info("Starting the agent...")
        asyncio.run(self.connect())


if __name__ == "__main__":
    try:
        details = AgentDetails()
        logger.info(f"Agent details: {details}")

        agent = JobAgent(
            websocket_url = os.environ.get("WEBSOCKET_URL"),
            token = os.environ.get("WEBSOCKET_TOKEN"),
            agent_details = details
        )
        agent.start()
    except KeyboardInterrupt:
        logger.info("Shutting down the agent...")
        sys.exit(0)
    finally:
        flush_logger(logger)
