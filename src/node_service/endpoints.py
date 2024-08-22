import sys
import requests
from time import time
from typing import List
from threading import Thread
from typing import Optional
import traceback
import asyncio
import aiohttp

import docker
from docker.errors import APIError, NotFound
from fastapi import APIRouter, Path, HTTPException, Depends, BackgroundTasks
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
)
from node_service.helpers import Logger, add_logged_background_task, format_traceback
from node_service.subjob_executor import SubJobExecutor

router = APIRouter()


class Container(BaseModel):
    image: str
    python_executable: str
    python_version: str


def _remove_subjob_executors_async(
    executors_to_remove: List[SubJobExecutor], background_tasks: BackgroundTasks, logger: Logger
):
    remove_executors = lambda executors: [executor.remove() for executor in executors]
    add_logged_background_task(
        background_tasks, logger, remove_executors, executors=executors_to_remove
    )


@router.get("/jobs/{job_id}")
def get_job_status(
    background_tasks: BackgroundTasks, job_id: str = Path(...), logger: Logger = Depends(get_logger)
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
            add_logged_background_task(
                background_tasks, logger, reboot_containers, previous_containers
            )

    return {"all_subjobs_done": all_done, "any_subjobs_failed": any_failed}


@router.post("/jobs/{job_id}")
def execute(
    background_tasks: BackgroundTasks,
    job_id: str = Path(...),
    request_json: dict = Depends(get_request_json),
    logger: Logger = Depends(get_logger),
    request_files: Optional[dict] = Depends(get_request_files),
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

    job_ref.update({"benchmark.sorting_executors": time()})

    # determine which executors to call and which to remove
    executors_to_remove = []
    executors_to_keep = []
    future_parallelism = 0
    print(f"THIS IS A TEST \n\n\n\nHERE\n\n\n")
    logger.log(SELF["subjob_executors"])
    for subjob_executor in SELF["subjob_executors"]:
        correct_python_version = subjob_executor.python_version == job["env"]["python_version"]
        need_more_parallelism = future_parallelism < request_json["parallelism"]
        logger.log(f"need_more_parallelism: {need_more_parallelism}")
        logger.log(f"future_parallelism: {future_parallelism}")
        logger.log(f"correct_python_version: {correct_python_version}")
        logger.log(
            f"correct_python_version and need_more_parallelism: {correct_python_version and need_more_parallelism}"
        )
        logger.log(f"subjob_executor.python_version: {subjob_executor.python_version}")
        logger.log(f"subjob_executor.python_version: {subjob_executor.python_version}")
        logger.log(f"job env python: {job['env']['python_version']}")
        logger.log(f"request json parallelism: {request_json['parallelism']}")

        if correct_python_version and need_more_parallelism:
            logger.log("ADDING EXECUTOR")
            executors_to_keep.append(subjob_executor)
            future_parallelism += 1
        else:
            logger.log("REMOVING EXECUTOR")
            executors_to_remove.append(subjob_executor)

    # call executors concurrently
    async def request_execution(session, url):
        async with session.post(url, data={"function_pkl": function_pkl}) as response:
            response.raise_for_status()

    async def request_executions(executors):
        async with aiohttp.ClientSession() as session:
            tasks = [request_execution(session, f"{e.host}/jobs/{job_id}") for e in executors]
            await asyncio.gather(*tasks)

    begin_executing = time()
    asyncio.run(request_executions(executors_to_keep))
    job_ref.update(
        {"benchmark.done_begin_executing": time(), "benchmark.begin_executing": begin_executing}
    )

    if not executors_to_keep:
        raise Exception("No qualified subjob executors?")

    SELF["subjob_executors"] = executors_to_keep
    _remove_subjob_executors_async(executors_to_remove, background_tasks, logger)

    return "Success"


# TODO: should take in num container sets to start.
@router.post("/reboot")
def reboot_containers(containers: List[Container], logger: Logger = Depends(get_logger)):
    """Kill all containers then start provided containers."""

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

        node_doc = firestore.Client(project=PROJECT_ID).collection("nodes").document(INSTANCE_NAME)
        node_doc.update({"status": "BOOTING"})

        docker_client = docker.from_env(timeout=240)

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
