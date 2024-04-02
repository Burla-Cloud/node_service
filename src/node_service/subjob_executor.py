import os
import sys
import requests
import traceback
from uuid import uuid4
from time import sleep

from google.cloud import logging
import docker

from node_service import IN_PRODUCTION, PROJECT_ID
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


class SubJobExecutor:
    """An instance of this = a running container with a running `container_service` instance."""

    def __init__(self, python_version: str, python_executable: str, image: str, docker_client):
        self.container = None
        attempt = 0

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
                    volumes=DEVELOPMENT_VOLUMES if os.environ.get("IN_DEV") else {},
                    environment={
                        "GOOGLE_CLOUD_PROJECT": PROJECT_ID,
                        "IN_PRODUCTION": IN_PRODUCTION,
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
            raise Exception("Executor failed to start.")

    def exists(self):
        try:
            self.container.reload()
            return True
        except docker.errors.NotFound:
            return False

    def logs(self):
        if self.exists():
            return self.container.logs().decode("utf-8")
        raise Exception("This executor no longer exists.")

    def remove(self):
        if self.exists():
            self.container.remove(force=True)  # The "force" arg kills it if it's not stopped

    def execute(self, job_id: str):
        if self.exists():
            response = requests.post(f"{self.host}/jobs/{job_id}", json={})
            response.raise_for_status()
        else:
            raise Exception("This executor no longer exists.")

    def log_debug_info(self):
        logs = self.logs() if self.exists() else "Unable to retrieve container logs."
        logger = logging.Client().logger("node_service")
        logger.log_struct({"severity": "ERROR", "container_logs": logs})

        debug_info = [vars(c) for c in self.docker_client.containers.list()]
        logger.log_struct({"severity": "ERROR", "DEBUG INFO": debug_info})

        if os.environ.get("IN_DEV"):  # <- to make debugging easier
            print(logs, file=sys.stderr)

    def status(self, attempt: int = 0):
        try:
            response = requests.get(f"{self.host}/")
            response.raise_for_status()
            status = response.json()["status"]  # will be one of: READY, RUNNING, FAILED, DONE
        except requests.exceptions.ConnectionError:
            #
            logger = logging.Client().logger("node_service")
            error = f"ConnectionError: Unable to connect to container_service at host: {self.host}"
            logger.log_struct({"severity": "DEBUG", "error": error})
            #
            if attempt <= 30:
                sleep(3)
                return self.status(attempt + 1)
            else:
                status = "FAILED"

        if status == "FAILED":
            self.log_debug_info()
            self.remove()

        return status
