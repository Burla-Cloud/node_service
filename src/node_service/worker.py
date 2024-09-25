import os
import sys
import json
import requests
import traceback
from uuid import uuid4
from time import sleep
from typing import Optional

from google.cloud import logging
import docker

from node_service import PROJECT_ID, IN_DEV
from node_service.helpers import next_free_port

LOGGER = logging.Client().logger("node_service")
DEVELOPMENT_VOLUMES = {
    f"{os.environ.get('HOME')}/.config/gcloud": {
        "bind": "/root/.config/gcloud",
        "mode": "ro",
    },
    f"{os.environ.get('HOME')}/Documents/burla/container_service": {
        "bind": "/burla",
        "mode": "ro",
    },
}


class Worker:
    """An instance of this = a running container with a running `container_service` instance."""

    def __init__(self, python_version: str, python_executable: str, image: str, docker_client):
        self.container = None
        attempt = 0
        docker_client.images.pull(image)

        while self.container is None:
            port = next_free_port()
            gunicorn_command = f"gunicorn -t 60 -b 0.0.0.0:{port} container_service:app"
            short_image_name = image.split("/")[-1].split(":")[0]
            try:
                self.container = docker_client.containers.run(
                    name=f"{short_image_name}_{str(uuid4())[:8]}",
                    image=image,
                    command=["/bin/sh", "-c", f"{python_executable} -m {gunicorn_command}"],
                    ports={port: port},
                    volumes=DEVELOPMENT_VOLUMES if IN_DEV else {},
                    environment={
                        "GOOGLE_CLOUD_PROJECT": PROJECT_ID,
                        "PROJECT_ID": PROJECT_ID,
                        "IN_DEV": IN_DEV,
                    },
                    detach=True,
                )
            except docker.errors.APIError as e:
                if ("address already in use" in str(e)) or ("port is already allocated" in str(e)):
                    # This leaves an extra container in the "Created" state.
                    containers_status = [c.status for c in docker_client.containers.list(all=True)]
                    LOGGER.log_struct(
                        {
                            "severity": "WARNING",
                            "message": f"PORT ALREADY IN USE, TRYING AGAIN.",
                            "containers_status": containers_status,
                        }
                    )
                else:
                    raise e
            except requests.exceptions.ConnectionError as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
                traceback_str = "".join(traceback_details)
                msg = "error thrown on `docker run` after returning."
                log = {"message": msg, "exception": str(e), "traceback": traceback_str}
                LOGGER.log_struct(dict(severity="WARNING").update(log))
                pass  # Thrown by containers.run long after it has already returned ??
            else:
                # Sometimes the container doesn't start and also doesn't throw an error ??
                # This is the case when calling containers.run() and container.start()
                attempt = 0
                sleep(1)
                self.container.reload()
                while self.container.status == "created":
                    self.container.start()
                    attempt += 1
                    if attempt == 10:
                        raise Exception("Unable to start node.")
                    sleep(1)
                    self.container.reload()

                if attempt > 1:
                    LOGGER.log_struct(
                        {
                            "severity": "INFO",
                            "message": f"CONTAINER STARTED! after {attempt+1} attempt(s)",
                            "state": self.container.status,
                            "name": self.container.name,
                        }
                    )

            attempt += 1
            if attempt == 10:
                raise Exception("Unable to start container.")

        self.subjob_id = None
        self.docker_client = docker_client
        self.python_version = python_version
        self.host = f"http://127.0.0.1:{port}"

        if self.status() != "READY":
            raise Exception("Worker failed to start.")

    def exists(self):
        try:
            self.container.reload()
            return True
        except docker.errors.NotFound:
            return False

    def logs(self):
        if self.exists():
            return self.container.logs().decode("utf-8")
        raise Exception("This worker no longer exists.")

    def remove(self):
        if self.exists():
            try:
                self.container.remove(force=True)  # The "force" arg kills it if it's not stopped
            except docker.errors.APIError as e:
                if not "409 Client Error" in str(e):
                    raise e

    def execute(self, job_id: str, function_pkl: Optional[bytes] = None):
        container_is_running = self.exists()
        url = f"{self.host}/jobs/{job_id}"

        if container_is_running and function_pkl:
            response = requests.post(url, files=dict(function_pkl=function_pkl))
        elif container_is_running:
            response = requests.post(url)
        else:
            raise Exception("This worker no longer exists.")
        response.raise_for_status()

    def log_debug_info(self):
        container_logs = self.logs() if self.exists() else "Unable to retrieve container logs."
        container_logs = f"\nERROR INSIDE CONTAINER:\n{container_logs}\n"
        containers_info = [vars(c) for c in self.docker_client.containers.list(all=True)]
        containers_info = json.loads(json.dumps(containers_info, default=lambda thing: str(thing)))
        logger = logging.Client().logger("node_service")
        logger.log_struct(
            {
                "severity": "ERROR",
                "LOGS_FROM_FAILED_CONTAINER": container_logs,
                "CONTAINERS INFO": containers_info,
            }
        )

        if os.environ.get("IN_DEV"):  # <- to make debugging easier
            print(container_logs, file=sys.stderr)

    def status(self, attempt: int = 0):
        try:
            response = requests.get(f"{self.host}/")
            response.raise_for_status()
            status = response.json()["status"]  # will be one of: READY, RUNNING, FAILED, DONE
        except requests.exceptions.ConnectionError:
            if attempt <= 30:
                sleep(3)
                return self.status(attempt + 1)
            else:
                status = "FAILED"

        if status == "FAILED":
            self.log_debug_info()
            self.remove()

        return status
