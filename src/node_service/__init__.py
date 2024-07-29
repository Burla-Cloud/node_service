import os
import sys
import json
import traceback
from uuid import uuid4
from time import time

from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import Response
from starlette.datastructures import UploadFile
from google.cloud import logging


__version__ = "v0.1.37"

IN_PRODUCTION = os.environ.get("IN_PRODUCTION") == "True"
IN_DEV = os.environ.get("IN_DEV") == "True"
PROJECT_ID = "burla-prod" if IN_PRODUCTION else "burla-test"
JOBS_BUCKET = "burla-jobs-prod" if PROJECT_ID == "burla-prod" else "burla-jobs"
# max num containers is 1024 due to some kind of network/port related limit
N_CPUS = 1 if IN_DEV else os.cpu_count()  # set IN_DEV in your bashrc

GCL_CLIENT = logging.Client().logger("node_service")
SELF = {
    "most_recent_container_config": [],
    "subjob_executors": [],
    "job_id": None,
    "PLEASE_REBOOT": True,
    "BOOTING": False,
    "RUNNING": False,
    "FAILED": False,
}


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


from node_service.endpoints import router as endpoints_router
from node_service.helpers import Logger, add_logged_background_task, format_traceback


app = FastAPI(docs_url=None, redoc_url=None)
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


@app.get("/version")
def version_endpoint():
    return {"version": __version__}


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

        exc_type, exc_value, exc_traceback = sys.exc_info()
        tb_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
        traceback_str = format_traceback(tb_details)
        add_logged_background_task(
            response.background, logger, logger.log, str(e), "ERROR", traceback=traceback_str
        )

    response_contains_background_tasks = getattr(response, "background") is not None
    if not response_contains_background_tasks:
        response.background = BackgroundTasks()

    if not IN_DEV:
        msg = f"Received {request.method} at {request.url}"
        add_logged_background_task(response.background, logger, logger.log, msg)

        status = response.status_code
        latency = time() - start
        msg = f"{request.method} to {request.url} returned {status} after {latency} seconds."
        add_logged_background_task(response.background, logger, logger.log, msg, latency=latency)

    return response
