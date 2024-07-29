import requests
from typing import List
from threading import Thread
from typing import Optional

import docker
from docker.errors import APIError, NotFound
from fastapi import APIRouter, Path, HTTPException, Depends, BackgroundTasks
from pydantic import BaseModel
from google.cloud import firestore

from node_service import PROJECT_ID, SELF, N_CPUS, get_request_json, get_logger, get_request_files
from node_service.helpers import Logger, add_logged_background_task
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
        add_logged_background_task(background_tasks, logger, reboot_containers, previous_containers)

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
    job = firestore.Client(project=PROJECT_ID).collection("jobs").document(job_id).get().to_dict()

    # start executing immediately
    subjob_executors_to_remove = []
    subjob_executors_to_keep = []
    current_parallelism = 0
    for subjob_executor in SELF["subjob_executors"]:
        correct_python_version = subjob_executor.python_version == job["env"]["python_version"]
        need_more_parallelism = current_parallelism < request_json["parallelism"]

        if correct_python_version and need_more_parallelism:
            function_pkl = (request_files or {}).get("function_pkl")
            subjob_executor.execute(job_id=job_id, function_pkl=function_pkl)
            subjob_executors_to_keep.append(subjob_executor)
            current_parallelism += 1
            logger.log(f"Assigned {job_id} to executor, current_parallelism={current_parallelism}")
        else:
            subjob_executors_to_remove.append(subjob_executor)

    SELF["subjob_executors"] = subjob_executors_to_keep
    _remove_subjob_executors_async(subjob_executors_to_remove, background_tasks, logger)

    return "Success"


# TODO: should take in num container sets to start.
@router.post("/reboot")
def reboot_containers(containers: List[Container]):
    """Kill all containers then start provided containers."""

    if SELF["BOOTING"]:
        raise HTTPException(409, detail=f"Node already BOOTING, unable to satisfy request.")
    try:
        SELF["RUNNING"] = False
        SELF["BOOTING"] = True
        SELF["subjob_executors"] = []
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
                Logger().log_exception(e)

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

    except Exception as e:
        SELF["FAILED"] = True
        raise e

    SELF["most_recent_container_config"] = containers
    return "Success"
