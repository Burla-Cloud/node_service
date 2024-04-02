import os
import sys
import socket
import traceback
import json
from typing import Optional

from collections import deque

from google.cloud import logging
from fastapi import Request
from node_service import __version__
from node_service.endpoints import SELF


PRIVATE_PORT_QUEUE = deque(range(32768, 60999))  # <- these ports should be mostly free.


def startup_error_msg(container_logs, image):
    return {
        "severity": "ERROR",
        "message": "subjob_executor timed out.",
        "exception": container_logs,
        "job_id": SELF["current_job_id"],
        "image": image,
    }


def next_free_port():
    """
    pops ports from `PRIVATE_PORT_QUEUE` until free one is found.
    The "correct" way to do this is to bind to port 0 which tells the os to return a random free
    port. This was attempted first, but it kept returning already-in-use ports.
    """
    port = PRIVATE_PORT_QUEUE.pop()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        if s.connect_ex(("localhost", port)) != 0:
            return port
        else:
            return next_free_port()


class Logger:
    def __init__(
        self, request: Optional[Request] = None, gcl_client: Optional[logging.Client] = None
    ):
        self.loggable_request = self._loggable_request(request) if request else None
        self.gcl_client = gcl_client if gcl_client else logging.Client()

    def _loggable_request(self, request):
        return json.dumps(vars(request), skipkeys=True, default=lambda o: "<not serializable>")

    def log(self, msg: str, **kw):
        struct = {
            "message": msg,
            "request": self.loggable_request,
            "version": __version__,
            **kw,
        }
        self.gcl_client.log_struct(struct)

    def log_exception(self, e: Exception):
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
        traceback_str = "".join(traceback_details)

        if os.environ.get("IN_DEV"):
            print(traceback_str, sys.stderr)

        struct = {
            "severity": "ERROR",
            "message": str(e),
            "traceback": traceback_str,
            "version": __version__,
            "request": self.loggable_request,
        }
        self.gcl_client.log_struct(struct)
