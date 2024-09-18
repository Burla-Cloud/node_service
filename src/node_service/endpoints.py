import sys
import requests
import datetime
from typing import List
from threading import Thread
from typing import Optional, Callable
import traceback
import asyncio
import aiohttp

import pytz
import docker
from docker.errors import APIError, NotFound
from fastapi import APIRouter, Path, HTTPException, Depends, Response
from pydantic import BaseModel
from google.cloud import firestore

from node_service import (
    PROJECT_ID,
    SELF,
    N_CPUS,
    INSTANCE_NAME,
    get_request_json,
    get_logger,
    get_request_files,
    get_add_background_task_function,
)
from node_service.helpers import Logger, format_traceback
from node_service.subjob_executor import SubJobExecutor

router = APIRouter()


class Container(BaseModel):
    image: str
    python_executable: str
    python_version: str


@router.get("/jobs/{job_id}")
def get_job_status(
    job_id: str = Path(...),
    add_background_task: Callable = Depends(get_add_background_task_function),
):
    if not job_id == SELF["job_id"]:
        raise HTTPException(404)

    executors_status = [executor.status() for executor in SELF["subjob_executors"]]
    any_failed = any([status == "FAILED" for status in executors_status])
    all_done = all([status == "DONE" for status in executors_status])

    if all_done or any_failed:
        previous_containers = SELF["most_recent_container_config"]
        SELF["RUNNING"] = False
        if not SELF["BOOTING"]:
            add_background_task(reboot_containers, previous_containers)

    return {"all_subjobs_done": all_done, "any_subjobs_failed": any_failed}


@router.post("/jobs/{job_id}")
def execute(
    job_id: str = Path(...),
    request_json: dict = Depends(get_request_json),
    request_files: Optional[dict] = Depends(get_request_files),
    logger: Logger = Depends(get_logger),
    add_background_task: Callable = Depends(get_add_background_task_function),
):
    if SELF["RUNNING"]:
        raise HTTPException(409, detail=f"Node in state `RUNNING`, unable to satisfy request")

    SELF["job_id"] = job_id
    SELF["RUNNING"] = True
    function_pkl = (request_files or {}).get("function_pkl")
    db = firestore.Client(project=PROJECT_ID)
    node_doc = db.collection("nodes").document(INSTANCE_NAME)
    node_doc.update({"status": "RUNNING", "current_job": job_id})

    job_ref = db.collection("jobs").document(job_id)
    job = job_ref.get().to_dict()

    # determine which executors to call and which to remove
    executors_to_remove = []
    executors_to_keep = []
    future_parallelism = 0
    user_python_version = job["env"]["python_version"]
    for subjob_executor in SELF["subjob_executors"]:
        correct_python_version = subjob_executor.python_version == user_python_version
        need_more_parallelism = future_parallelism < request_json["parallelism"]

        if correct_python_version and need_more_parallelism:
            executors_to_keep.append(subjob_executor)
            future_parallelism += 1
        else:
            executors_to_remove.append(subjob_executor)

    if not executors_to_keep:
        msg = "No compatible containers.\n"
        msg += f"User is running python version {user_python_version}, "
        cluster_python_versions = [e.python_version for e in SELF["subjob_executors"]]
        cluster_python_versions_msg = ", ".join(cluster_python_versions[:-1])
        cluster_python_versions_msg += f", and {cluster_python_versions[-1]}"
        msg += f"containers in the cluster are running: {cluster_python_versions_msg}.\n"
        msg += "To fix this you can either:\n"
        msg += f" - update the cluster to run containers with python v{user_python_version}\n"
        msg += f" - update your local python version to be one of {cluster_python_versions}"
        return Response(status_code=500, content=msg)

    # call executors concurrently
    async def request_execution(session, url):
        async with session.post(url, data={"function_pkl": function_pkl}) as response:
            response.raise_for_status()

    async def request_executions(executors):
        async with aiohttp.ClientSession() as session:
            tasks = [request_execution(session, f"{e.host}/jobs/{job_id}") for e in executors]
            await asyncio.gather(*tasks)

    asyncio.run(request_executions(executors_to_keep))

    SELF["subjob_executors"] = executors_to_keep
    remove_executors = lambda executors: [executor.remove() for executor in executors]
    add_background_task(remove_executors, executors_to_remove)
    add_background_task(logger.log, f"Started executing job at parallelism: {future_parallelism}")


# TODO: should take in num container sets to start.
@router.post("/reboot")
def reboot_containers(containers: List[Container], logger: Logger = Depends(get_logger)):
    """Kill all containers then start provided containers."""
    started_booting_at = datetime.now(pytz.timezone("America/New_York"))

    # TODO: seems to have like a 1/5 chance (only after running a job) of throwing a:
    # `Unable to reboot, not all containers started!`
    # Error, prececed by many `PORT ALREADY IN USE, TRYING AGAIN.`'s
    # reconcile does a good job of cleaning these nodes up!

    if SELF["BOOTING"]:
        raise HTTPException(409, detail="Node already BOOTING, unable to satisfy request.")

    try:
        SELF["RUNNING"] = False
        SELF["BOOTING"] = True
        SELF["subjob_executors"] = []

        docker_client = docker.from_env(timeout=240)
        node_doc = firestore.Client(project=PROJECT_ID).collection("nodes").document(INSTANCE_NAME)
        node_doc.update(
            {
                "status": "BOOTING",
                "current_job": None,
                "parallelism": None,
                "target_parallelism": None,
                "started_booting_at": started_booting_at,
            }
        )

        # ignore `main_service` container so that in local testing I can use the `main_service`
        # container while I am running the `node_service` tests.
        current_containers = docker_client.containers.list(all=True)
        current_containers = [c for c in current_containers if c.name != "main_service"]

        # remove all current containers
        for container in current_containers:
            try:
                container.remove(force=True)
            except (APIError, NotFound, requests.exceptions.HTTPError) as e:
                # re-raise any errors that aren't an "already-in-progress" error
                if not (("409" in str(e)) or ("404" in str(e))):
                    raise e

        def create_subjob_executor(*a, **kw):
            # Log error inside thread because sometimes it isn't sent to the main thread, idk why.
            try:
                subjob_executor = SubJobExecutor(*a, **kw)
                SELF["subjob_executors"].append(subjob_executor)
            except Exception as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                tb_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
                traceback_str = format_traceback(tb_details)
                logger.log(str(e), "ERROR", traceback=traceback_str)

        # start instance of every container for every cpu
        threads = []
        for container in containers:
            for _ in range(N_CPUS):
                args = (
                    container.python_version,
                    container.python_executable,
                    container.image,
                    docker_client,
                )
                thread = Thread(target=create_subjob_executor, args=args)
                threads.append(thread)
                thread.start()

        for thread in threads:
            thread.join()

        # ignore `main_service` container so that in local testing I can use the `main_service`
        # container while I am running the `node_service` tests.
        current_containers = docker_client.containers.list(all=True)
        current_containers = [c for c in current_containers if c.name != "main_service"]

        # Sometimes on larger machines, some containers don't start, or get stuck in "CREATED" state
        # This has not been diagnosed, this check is performed to ensure all containers started.
        containers_status = [c.status for c in current_containers]
        num_running_containers = sum([status == "running" for status in containers_status])
        some_containers_missing = num_running_containers != (N_CPUS * len(containers))

        if some_containers_missing:
            SELF["FAILED"] = True
            raise Exception("Unable to reboot, not all containers started!")
        else:
            SELF["PLEASE_REBOOT"] = False
            SELF["BOOTING"] = False
            SELF["job_id"] = None
            node_doc.update({"status": "READY"})

    except Exception as e:
        SELF["FAILED"] = True
        raise e

    SELF["most_recent_container_config"] = containers
    return "Success"
