from typing import List

from fastapi import APIRouter, Path, HTTPException, Depends, BackgroundTasks
from google.cloud import firestore

from node_service import PROJECT_ID, SELF, get_request_json, get_logger, _reboot_containers
from node_service.helpers import Logger
from node_service.subjob_executor import SubJobExecutor

router = APIRouter()


def _remove_subjob_executors_async(
    executors_to_remove: List[SubJobExecutor], background_tasks: BackgroundTasks
):
    remove_executors = lambda executors: [executor.remove() for executor in executors]
    background_tasks.add_task(remove_executors, executors=executors_to_remove)


@router.get("/jobs/{job_id}")
def get_job_status(background_tasks: BackgroundTasks, job_id: str = Path(...)):
    if not job_id == SELF["job_id"]:
        raise HTTPException(404)

    executors_status = [executor.status() for executor in SELF["subjob_executors"]]
    any_failed = any([status == "FAILED" for status in executors_status])
    all_done = all([(status == "DONE") or (status == "FAILED") for status in executors_status])

    if all_done or any_failed:
        background_tasks.add_task(_reboot_containers)

    return {"all_subjobs_done": all_done, "any_subjobs_failed": any_failed}


@router.post("/jobs/{job_id}")
def execute(
    background_tasks: BackgroundTasks,
    job_id: str = Path(...),
    request_json: dict = Depends(get_request_json),
    logger: Logger = Depends(get_logger),
):
    if SELF["RUNNING"]:
        return f"Node in state `RUNNING`, unable to satisfy request", 409

    SELF["job_id"] = job_id
    SELF["RUNNING"] = True  # <- only way to set to false is to call `_reboot_containers()`
    job = firestore.Client(project=PROJECT_ID).collection("jobs").document(job_id).get().to_dict()

    # start executing immediately
    subjob_executors_to_remove = []
    subjob_executors_to_keep = []
    current_parallelism = 0
    for subjob_executor in SELF["subjob_executors"]:
        correct_python_version = subjob_executor.python_version == job["python_version"]
        need_more_parallelism = current_parallelism < request_json["parallelism"]

        if correct_python_version and need_more_parallelism:
            subjob_executor.execute(job_id)
            subjob_executors_to_keep.append(subjob_executor)
            current_parallelism += 1
            logger.log(f"Assigned job to executor, current_parallelism={current_parallelism}")
        else:
            subjob_executors_to_remove.append(subjob_executor)

    SELF["subjob_executors"] = subjob_executors_to_keep
    _remove_subjob_executors_async(subjob_executors_to_remove, background_tasks)

    return "Success"
