import os
from threading import Thread

import docker
from fastapi import FastAPI, Depends, Request
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
from google.cloud import logging
from google.cloud import firestore

__version__ = "v0.1.37"

IN_PRODUCTION = os.environ.get("IN_PRODUCTION") == "True"
PROJECT_ID = "burla-prod" if IN_PRODUCTION else "burla-test"
JOBS_BUCKET = "burla-jobs-prod" if PROJECT_ID == "burla-prod" else "burla-jobs"
# max num containers is 1024 due to some kind of network/port related limit
N_CPUS = 1 if os.environ.get("IN_DEV") == "True" else os.cpu_count()  # set IN_DEV in your bashrc

SELF = {
    "subjob_executors": [],
    "job_id": None,
    "BOOTING": True,
    "REBOOTING": False,
    "RUNNING": False,
    "FAILED": False,
}


async def get_request_json(request: Request):
    return await request.json()


def get_db():
    return firestore.Client(project=PROJECT_ID)


def get_gcl_client():
    return logging.Client().logger("node_service")


def get_logger(
    request: Request,
    gcl_client: logging.Client = Depends(get_gcl_client),
):
    return Logger(request=request, gcl_client=gcl_client)


from node_service.endpoints import router as endpoints_router
from node_service.subjob_executor import SubJobExecutor
from node_service.helpers import Logger


def _reboot_containers():
    """
    Reboots all "subjob_executors" by removing all containers.
    Then starting all containers as defined in the `cluster_config` firebase document.
    """
    SELF["RUNNING"] = False
    SELF["REBOOTING"] = True

    db = firestore.Client()
    config = db.collection("cluster_config").document("cluster_config").get().to_dict()
    docker_client = docker.from_env(timeout=240)

    # remove all containers
    [container.remove(force=True) for container in docker_client.containers.list(all=True)]
    SELF["subjob_executors"] = []

    # get list of containers to start per cpu from config
    machine_type = "n1-standard-96"
    for node in config["Nodes"]:
        if node["machine_type"] == machine_type:
            standby_containers = node["containers"]

    def create_subjob_executor(*a, **kw):
        # Log error inside thread because sometimes it isn't sent to the main thread, not sure why.
        try:
            subjob_executor = SubJobExecutor(*a, **kw)
            SELF["subjob_executors"].append(subjob_executor)
        except Exception as e:
            Logger().log_exception(e)

    # start instance of every container for every cpu
    threads = []
    for container in standby_containers:
        docker_client.images.pull(container["image"])
        for _ in range(N_CPUS):
            args = (
                container["python_version"],
                container["python_executable"],
                container["image"],
                docker_client,
            )
            thread = Thread(target=create_subjob_executor, args=args)
            threads.append(thread)
            thread.start()

    for thread in threads:
        thread.join()

    # Sometimes on larger machines, some containers don't start, or get stuck in the "CREATED" state
    # This has not been diagnosed, this check is performed to ensure all containers started.
    containers_status = [c.status for c in docker_client.containers.list(all=True)]
    num_running_containers = sum([status == "running" for status in containers_status])
    some_containers_missing = num_running_containers != (N_CPUS * len(standby_containers))

    if some_containers_missing:
        SELF["FAILED"] = True
    else:
        SELF["BOOTING"] = False
        SELF["REBOOTING"] = False
        SELF["job_id"] = None


try:
    Logger().log("Hi this is a test.")
    _reboot_containers()
except Exception as e:
    Logger().log_exception(e)
    SELF["FAILED"] = True


app = FastAPI(docs_url=None, redoc_url=None)
app.include_router(endpoints_router)


@app.get("/")
def get_status():
    if SELF["FAILED"]:
        return {"status": "FAILED"}
    elif SELF["BOOTING"]:
        return {"status": "BOOTING"}
    elif SELF["REBOOTING"]:
        return {"status": "REBOOTING"}
    elif SELF["RUNNING"]:
        return {"status": "RUNNING"}
    else:
        return {"status": "READY"}


@app.post("/reboot")
def reboot_containers():
    if SELF["RUNNING"]:
        return f"Node in state `RUNNING`, unable to satisfy request", 409
    else:
        _reboot_containers()
        return "Success"


@app.get("/version")
def version_endpoint():
    return {"version": __version__}


@app.exception_handler(Exception)
async def log_exception_before_returning(request: Request, exc: Exception):
    # I can't figure out how to get the already-created logger instance into this function,
    # So instead, reinstantiate the logger:
    get_logger(request=request, gcl_client=get_gcl_client()).log_exception(exc)
    if isinstance(exc, StarletteHTTPException):
        return JSONResponse(status_code=exc.status_code, content=f'{{"error": {str(exc)}}}')
    else:
        return JSONResponse(status_code=500, content='{"error": "Internal Server Error."}')
