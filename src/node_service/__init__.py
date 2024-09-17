import os
import sys
import json
import asyncio
import traceback
import subprocess
from uuid import uuid4
from time import time
from typing import Callable
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, BackgroundTasks, Depends
from fastapi.responses import Response
from starlette.datastructures import UploadFile
from google.cloud import logging
from google.cloud.compute_v1 import InstancesClient

IDLE_SHUTDOWN_TIMEOUT_SECONDS = 60 * 10
CURRENT_TIME_UNTIL_SHOTDOWN = IDLE_SHUTDOWN_TIMEOUT_SECONDS

INSTANCE_NAME = os.environ.get("INSTANCE_NAME")
IN_DEV = os.environ.get("IN_DEV") == "True"

if IN_DEV:
    cmd = ["gcloud", "config", "get-value", "project"]
    PROJECT_ID = subprocess.run(cmd, capture_output=True, text=True).stdout.strip()
else:
    PROJECT_ID = os.environ["PROJECT_ID"]
JOBS_BUCKET = f"burla-jobs--{PROJECT_ID}"

# max num containers is 1024 due to some network/port related limit
N_CPUS = 1 if IN_DEV else os.cpu_count()
GCL_CLIENT = logging.Client().logger("node_service", labels=dict(INSTANCE_NAME=INSTANCE_NAME))
SELF = {
    "instance_name": None,
    "most_recent_container_config": [],
    "subjob_executors": [],
    "job_id": None,
    "PLEASE_REBOOT": True,
    "BOOTING": False,
    "RUNNING": False,
    "FAILED": False,
}

from node_service.helpers import Logger


async def get_request_json(request: Request):
    try:
        return await request.json()
    except:
        form_data = await request.form()
        return json.loads(form_data["request_json"])


async def get_request_files(request: Request):
    """
    If request is multipart/form data load all files and returns as dict of {filename: bytes}
    """
    form_data = await request.form()
    files = {}
    for key, value in form_data.items():
        if isinstance(value, UploadFile):
            files.update({key: await value.read()})

    if files:
        return files


def get_logger(request: Request):
    return Logger(request=request)


def get_add_background_task_function(
    background_tasks: BackgroundTasks, logger: Logger = Depends(get_logger)
):
    def add_logged_background_task(func: Callable, *a, **kw):
        tb_details = traceback.format_list(traceback.extract_stack()[:-1])
        parent_traceback = "Traceback (most recent call last):\n" + format_traceback(tb_details)

        def func_logged(*a, **kw):
            try:
                return func(*a, **kw)
            except Exception as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                tb_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
                local_traceback_no_title = "\n".join(format_traceback(tb_details).split("\n")[1:])
                traceback_str = parent_traceback + local_traceback_no_title
                logger.log(message=str(e), severity="ERROR", traceback=traceback_str)

        background_tasks.add_task(func_logged, *a, **kw)

    return add_logged_background_task


from node_service.helpers import Logger, format_traceback
from node_service.endpoints import router as endpoints_router


async def shutdown_if_idle_for_too_long():
    """FYI: Errors/stdout from this function will be completely hidden!"""

    # this is in a for loop so the wait time can be extended while waiting
    for _ in range(CURRENT_TIME_UNTIL_SHOTDOWN):
        await asyncio.sleep(1)

    if not IN_DEV:
        struct = dict(message=f"SHUTTING DOWN NODE DUE TO INACTIVITY: {INSTANCE_NAME}")
        await GCL_CLIENT.log_struct(struct, severity="WARNING")

        instance_client = InstancesClient()
        silly_response = await instance_client.aggregated_list(project=PROJECT_ID)
        vms_per_zone = [getattr(vms_in_zone, "instances", []) for _, vms_in_zone in silly_response]
        vms = [vm for vms_in_zone in vms_per_zone for vm in vms_in_zone]
        vm = next((vm for vm in vms if vm.name == INSTANCE_NAME), None)

        if vm is None:
            struct = dict(message=f"INSTANCE NOT FOUND?? UNABLE TO DELETE: {INSTANCE_NAME}")
            await GCL_CLIENT.log_struct(struct, severity="ERROR")
        else:
            zone = vm.zone.split("/")[-1]
            instance_client.delete(project=PROJECT_ID, zone=zone, instance=INSTANCE_NAME)


@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(shutdown_if_idle_for_too_long())
    yield


app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None)
app.include_router(endpoints_router)


@app.get("/")
def get_status():
    if SELF["FAILED"]:
        return {"status": "FAILED"}
    elif SELF["BOOTING"]:
        return {"status": "BOOTING"}
    elif SELF["PLEASE_REBOOT"]:
        return {"status": "PLEASE_REBOOT"}
    elif SELF["RUNNING"]:
        return {"status": "RUNNING"}
    else:
        return {"status": "READY"}


@app.middleware("http")
async def log_and_time_requests__log_errors(request: Request, call_next):
    """
    Fastapi `@app.exception_handler` will completely hide errors if middleware is used.
    Catching errors in a `Depends` function will not distinguish
        http errors originating here vs other services.
    """
    start = time()
    request.state.uuid = uuid4().hex

    # If `get_logger` was a dependency this will be the second time a Logger is created.
    # This is fine because creating this object only attaches the `request` to a function.
    logger = Logger(request)

    # Important to note that HTTP exceptions do not raise errors here!
    try:
        response = await call_next(request)
    except Exception as e:
        # create new response object to return gracefully.
        response = Response(status_code=500, content="Internal server error.")
        response.background = BackgroundTasks()
        add_background_task = get_add_background_task_function(response.background, logger=logger)

        exc_type, exc_value, exc_traceback = sys.exc_info()
        tb_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
        traceback_str = format_traceback(tb_details)
        add_background_task(logger.log, str(e), "ERROR", traceback=traceback_str)

    response_contains_background_tasks = getattr(response, "background") is not None
    if not response_contains_background_tasks:
        response.background = BackgroundTasks()

    if not IN_DEV:
        add_background_task = get_add_background_task_function(response.background, logger=logger)
        msg = f"Received {request.method} at {request.url}"
        add_background_task(logger.log, msg)

        status = response.status_code
        latency = time() - start
        msg = f"{request.method} to {request.url} returned {status} after {latency} seconds."
        add_background_task(logger.log, msg, latency=latency)

    global CURRENT_TIME_UNTIL_SHOTDOWN
    CURRENT_TIME_UNTIL_SHOTDOWN = IDLE_SHUTDOWN_TIMEOUT_SECONDS
    return response
