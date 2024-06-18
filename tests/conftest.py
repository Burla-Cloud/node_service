import requests
import threading
import docker
import uvicorn

from time import sleep

import pytest

PORT = 5000
HOSTNAME = f"http://127.0.0.1:{PORT}"

CONTAINERS = [
    {
        "image": "us-docker.pkg.dev/burla-test/burla-job-containers/default/image-nogpu:latest",
        "python_executable": "/.pyenv/versions/3.9.*/bin/python3.9",
        "python_version": "3.9",
    },
    {
        "image": "us-docker.pkg.dev/burla-test/burla-job-containers/default/image-nogpu:latest",
        "python_executable": "/.pyenv/versions/3.10.*/bin/python3.10",
        "python_version": "3.10",
    },
    {
        "image": "us-docker.pkg.dev/burla-test/burla-job-containers/default/image-nogpu:latest",
        "python_executable": "/.pyenv/versions/3.11.*/bin/python3.11",
        "python_version": "3.11",
    },
    {
        "image": "us-docker.pkg.dev/burla-test/burla-job-containers/default/image-nogpu:latest",
        "python_executable": "/.pyenv/versions/3.12.*/bin/python3.12",
        "python_version": "3.12",
    },
]


def delete_containers():
    client = docker.from_env()
    containers = client.containers.list(all=True)
    for container in containers:
        if container.name.startswith("image-nogpu"):
            print(f"REMOVING: {container.name}")
            container.remove(force=True)


def start_server(app):
    uvicorn.run(app, host="0.0.0.0", port=PORT)


@pytest.fixture(scope="module")
def hostname():
    print("\n")
    delete_containers()

    from node_service import app  # <- standby containers are started when this is loaded.

    server_thread = threading.Thread(target=start_server, args=(app,), daemon=True)
    server_thread.start()
    sleep(3)

    # Wait until node service has started all subjob_executors
    attempt = 0
    while True:
        try:
            response = requests.get(f"{HOSTNAME}/")
            response.raise_for_status()
            status = response.json()["status"]
        except requests.exceptions.ConnectionError:
            status = None

        if status == "FAILED":
            raise Exception("Node service entered state: FAILED")
        if status == "PLEASE_REBOOT":
            response = requests.post(f"{HOSTNAME}/reboot", json=CONTAINERS)
            response.raise_for_status()
        if status == "READY":
            break

        sleep(2)
        attempt += 1
        if attempt > 10:
            raise Exception("TIMEOUT! Node Service not ready after 20 seconds?")

    print("\nNODE SERVICE STARTED\n")
    yield HOSTNAME

    # because we're using a daemon thread this will also kill the thread dies safely when
    # the program ends.
