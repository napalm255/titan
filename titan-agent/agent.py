"""
Titan Agent.
"""

import os
import sh
import sys
import json
import logging
import asyncio
import threading
import subprocess
import urllib.parse
from typing import Optional
import websockets
import watchtower

# Setup logger for CloudWatch
logging.basicConfig(level=logging.INFO)
cloudwatch_handler = watchtower.CloudWatchLogHandler(
    log_group="/titan/agents",
    create_log_group=True,
    create_log_stream=True,
    use_queues=True,
    send_interval=5,
    max_batch_count=5
)
logger = logging.getLogger('titan-agent')
logger.addHandler(cloudwatch_handler)


class JobAgent:
    """
    Titan Job Agent.

    This agent connects to the Titan WebSocket server and listens for job messages.
    It can execute shell commands and terminate the current job.
    """
    # pylint: disable=logging-fstring-interpolation

    def __init__(self, websocket_url: str, token: str):
        """
        Initialize the Job Agent.

        :param websocket_url: WebSocket URL to connect to.
        :param token: Authorization token.
        """
        self.websocket_url = websocket_url
        self.current_process: Optional[subprocess.Popen] = None
        self.job_lock = threading.Lock()
        self.headers = {
            "Auth": token
        }
        self.query_params = {
            "os": os.name,
            "host": os.uname().nodename,
            "labels": "default"
        }

    def url(self):
        """
        Construct the WebSocket URL with query parameters.
        """
        return f"{self.websocket_url}?{urllib.parse.urlencode(self.query_params)}"

    async def connect(self):
        """
        Connect to the WebSocket server.
        """
        async with websockets.connect(self.url(), extra_headers=self.headers) as ws:
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

        def execute():
            try:
                for line in sh.Command(shell_command)(_iter=True, _err_to_out=True):
                    logger.info(line.strip())
                logger.info("Job completed successfully with return code: 0")
            except sh.ErrorReturnCode as error:
                logger.error(f"Job failed with return code: {error.exit_code}")

        def execute_job():
            logger.info(f"Executing shell command: {shell_command}")
            with self.job_lock:
                self.current_process = subprocess.Popen(
                    shell_command,
                    shell=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
                for line in self.current_process.stdout:
                    logger.info(line.decode().strip())

                self.current_process.wait()
                logger.info(f"Job completed with return code: {self.current_process.returncode}")
                for handler in logger.handlers:
                    handler.flush()
                    handler.close()

        # threading.Thread(target=execute_job).start()
        threading.Thread(target=execute).start()

    async def handle_terminate(self):
        """
        Terminate the current job.
        """
        if self.job_lock.locked() and self.current_process:
            logger.info("Terminating the current job")
            self.current_process.terminate()

    def start(self):
        """
        Start the agent.
        """
        logger.info("Starting the agent...")
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.connect())

# Example usage
if __name__ == "__main__":
    try:
        agent = JobAgent(
            websocket_url = os.environ.get("WEBSOCKET_URL"),
            token = os.environ.get("WEBSOCKET_TOKEN")
        )
        agent.start()
    except KeyboardInterrupt:
        logger.info("Shutting down the agent...")
        sys.exit(0)
