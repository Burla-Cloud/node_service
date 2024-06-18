import os

from fastapi import FastAPI, Depends, Request
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
from google.cloud import logging


__version__ = "v0.1.37"

IN_PRODUCTION = os.environ.get("IN_PRODUCTION") == "True"
PROJECT_ID = "burla-prod" if IN_PRODUCTION else "burla-test"
JOBS_BUCKET = "burla-jobs-prod" if PROJECT_ID == "burla-prod" else "burla-jobs"
# max num containers is 1024 due to some kind of network/port related limit
N_CPUS = 1 if os.environ.get("IN_DEV") == "True" else os.cpu_count()  # set IN_DEV in your bashrc

SELF = {
    "most_recent_container_config": [],
    "subjob_executors": [],
    "job_id": None,
    "PLEASE_REBOOT": True,
    "REBOOTING": False,
    "RUNNING": False,
    "FAILED": False,
}


async def get_request_json(request: Request):
    return await request.json()


def get_gcl_client():
    return logging.Client().logger("node_service")


def get_logger(
    request: Request,
    gcl_client: logging.Client = Depends(get_gcl_client),
):
    return Logger(request=request, gcl_client=gcl_client)


from node_service.endpoints import router as endpoints_router
from node_service.helpers import Logger


app = FastAPI(docs_url=None, redoc_url=None)
app.include_router(endpoints_router)


@app.get("/")
def get_status():
    if SELF["FAILED"]:
        return {"status": "FAILED"}
    elif SELF["PLEASE_REBOOT"]:
        return {"status": "PLEASE_REBOOT"}
    elif SELF["REBOOTING"]:
        return {"status": "REBOOTING"}
    elif SELF["RUNNING"]:
        return {"status": "RUNNING"}
    else:
        return {"status": "READY"}


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
