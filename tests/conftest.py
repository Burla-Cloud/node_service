import requests
import threading
import docker
import uvicorn

from time import sleep

import pytest

PORT = 5000
HOSTNAME = f"http://127.0.0.1:{PORT}"


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

    print("STARTING NODE SERVICE (starting all standby containers)")
    from node_service import app  # <- standby containers are started when this is loaded.

    server_thread = threading.Thread(target=start_server, args=(app,), daemon=True)
    server_thread.start()
    sleep(3)

    # Wait until node service has started all subjob_executors
    attempt = 0
    while True:
        sleep(2)
        try:
            response = requests.get(f"{HOSTNAME}/")
            response.raise_for_status()
            assert response.json()["status"] != "FAILED"
            break
        except requests.exceptions.ConnectionError:
            pass

        attempt += 1
        if attempt > 10:
            raise Exception("TIMEOUT! Node Service not ready after 20 seconds?")

    print("\nNODE SERVICE STARTED\n")
    yield HOSTNAME

    # because we're using a daemon thread this will also kill the thread dies safely when
    # the program ends.
