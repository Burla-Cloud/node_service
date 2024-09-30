import os
import sys
import json
import pickle
import pytest
import requests
import subprocess
from uuid import uuid4
from time import sleep
from six import reraise
from queue import Queue
from typing import Optional
from threading import Thread, Event
from concurrent.futures import ThreadPoolExecutor

import cloudpickle
import docker
from tblib import Traceback
from google.cloud import firestore
from google.cloud.firestore import DocumentReference
from google.cloud.storage import Client, Blob

"""
If node_service is imported anywhere here the containers will be started 
then deleted by `delete_containers` in conftest before testing starts!!
DO NOT import node_service here.
"""

GCS_CLIENT = Client()
cmd = ["gcloud", "config", "get-value", "project"]
PROJECT_ID = subprocess.run(cmd, capture_output=True, text=True).stdout.strip()


def _upload_function_to_gcs(job_id, _function):
    pickled_function = cloudpickle.dumps(_function)
    function_uri = f"gs://burla-jobs/12345/{job_id}/function.pkl"
    blob = Blob.from_string(function_uri, GCS_CLIENT)
    blob.upload_from_string(data=pickled_function, content_type="application/octet-stream")


def _upload_inputs_to_gcs(job_id, _inputs):
    subjob_ids = list(range(len(_inputs)))
    for subjob_id, mock_input in zip(subjob_ids, _inputs):
        pickled_input = cloudpickle.dumps(mock_input)
        input_uri = f"gs://burla-jobs/{job_id}/inputs/{subjob_id}.pkl"
        blob = Blob.from_string(input_uri, GCS_CLIENT)
        blob.upload_from_string(data=pickled_input, content_type="application/octet-stream")


def print_logs_from_db(job_doc_ref: DocumentReference, stop_event: Event):

    def on_snapshot(collection_snapshot, changes, read_time):
        for change in changes:
            if change.type.name == "ADDED":
                print(change.document.to_dict()["msg"])

    collection_ref = job_doc_ref.collection("logs")
    query_watch = collection_ref.on_snapshot(on_snapshot)

    while not stop_event.is_set():
        stop_event.wait(0.5)  # this does not block the processing of new documents
    query_watch.unsubscribe()


def enqueue_outputs_from_db(job_doc_ref: DocumentReference, stop_event: Event, output_queue: Queue):

    def on_snapshot(collection_snapshot, changes, read_time):
        for change in changes:
            if change.type.name == "ADDED":
                output_pkl = change.document.to_dict()["output"]
                output_queue.put(cloudpickle.loads(output_pkl))

    collection_ref = job_doc_ref.collection("outputs")
    query_watch = collection_ref.on_snapshot(on_snapshot)

    while not stop_event.is_set():
        stop_event.wait(0.5)  # this does not block the processing of new documents
    query_watch.unsubscribe()


def _upload_input(inputs_collection, input_index, input_):
    input_pkl = cloudpickle.dumps(input_)
    input_too_big = len(input_pkl) > 1_048_576

    if input_too_big:
        msg = f"Input at index {input_index} is greater than 1MB in size.\n"
        msg += "Inputs greater than 1MB are unfortunately not yet supported."
        raise Exception(msg)
    else:
        doc = {"input": input_pkl, "claimed": False}
        inputs_collection.document(str(input_index)).set(doc)


def upload_inputs(DB: firestore.Client, inputs_id: str, inputs: list):
    """
    Uploads inputs into a separate collection not connected to the job
    so that uploading can start before the job document is created.
    """
    inputs_collection = DB.collection("inputs").document(inputs_id).collection("inputs")

    futures = []
    with ThreadPoolExecutor(max_workers=32) as executor:
        for input_index, input_ in enumerate(inputs):
            future = executor.submit(_upload_input, inputs_collection, input_index, input_)
            futures.append(future)

        for future in futures:
            future.result()  # This will raise exceptions if any occurred in the threads

    print("All inputs uploaded.")


def _create_job_document_in_database(
    job_id, inputs_id, image, dependencies, n_inputs, faux_python_version: Optional[str] = None
):
    python_version = faux_python_version if faux_python_version else f"3.{sys.version_info.minor}"
    db = firestore.Client(project=PROJECT_ID)
    job_ref = db.collection("jobs").document(job_id)
    job_ref.set(
        {
            "test": True,
            "inputs_id": inputs_id,
            "n_inputs": n_inputs,
            "target_parallelism": 1,
            "function_uri": f"gs://burla-jobs/12345/{job_id}/function.pkl",
            "user_python_version": python_version,
        }
    )


def _retrieve_and_raise_errors(job_id):
    db = firestore.Client(project=PROJECT_ID)
    job_ref = db.collection("jobs").document(job_id)
    job = job_ref.get().to_dict()

    env_install_error = job["env"].get("install_error")
    if env_install_error:
        raise Exception(env_install_error)

    if job.get("udf_errors"):
        # input_index = job["udf_errors"][0]["input_index"]
        udf_error = job["udf_errors"][0]["udf_error"]
        exception_info = pickle.loads(bytes.fromhex(udf_error))
        reraise(
            tp=exception_info["exception_type"],
            value=exception_info["exception"],
            tb=Traceback.from_dict(exception_info["traceback_dict"]).as_traceback(),
        )


def _wait_until_node_svc_not_busy(node_svc_hostname, attempt=0):
    response = requests.get(node_svc_hostname, timeout=60)
    response.raise_for_status()

    if response.json()["status"] != "READY":
        sleep(5)
        print(f"Waiting for not to be READY, current status={response.json()['status']}")
        return _wait_until_node_svc_not_busy(node_svc_hostname, attempt=attempt + 1)
    elif attempt == 30:
        raise Exception("node should have rebooted by now ?")


def _assert_node_service_left_proper_containers_running():
    from node_service import INSTANCE_N_CPUS  # <- see note near import statements at top.

    db = firestore.Client(project=PROJECT_ID)
    config = db.collection("cluster_config").document("cluster_config").get().to_dict()

    client = docker.from_env()
    attempts = 0
    in_standby = False
    while not in_standby:
        # ignore `main_service` container so that in local testing I can use the `main_service`
        # container while I am running the `node_service` tests.
        containers = [c for c in client.containers.list(all=True) if c.name != "main_service"]

        # all container svc running ?
        for container in containers:
            port = int(list(container.attrs["NetworkSettings"]["Ports"].values())[0][0]["HostPort"])
            response = requests.get(f"http://127.0.0.1:{port}")
            response.raise_for_status()
            assert response.json()["status"] == "READY"

        # correct num containers ?
        machine_type = "n4-standard-2"
        for node in config["Nodes"]:
            if node["machine_type"] == machine_type:
                break
        in_standby = len(containers) == len(node["containers"]) * INSTANCE_N_CPUS

        sleep(2)
        if attempts == 10:
            raise Exception("standby containers not started ??")
        attempts += 1


def _execute_job(
    node_svc_hostname,
    my_function,
    my_inputs,
    my_packages,
    my_image,
    send_inputs_through_gcs=False,
    faux_python_version=None,
):
    db = firestore.Client()
    JOB_ID = str(uuid4()) + "-test"
    INPUTS_ID = str(uuid4()) + "-test"
    DEFAULT_IMAGE = (
        f"us-docker.pkg.dev/{PROJECT_ID}/burla-job-containers/default/image-nogpu:latest"
    )
    image = my_image if my_image else DEFAULT_IMAGE

    # in separate thread start uploading inputs:
    input_uploader_thread = Thread(
        target=upload_inputs,
        args=(db, INPUTS_ID, my_inputs),
        daemon=True,
    )
    input_uploader_thread.start()

    if send_inputs_through_gcs:
        _upload_function_to_gcs(JOB_ID, my_function)
        _upload_inputs_to_gcs(JOB_ID, my_inputs)

    _create_job_document_in_database(
        JOB_ID, INPUTS_ID, image, my_packages, len(my_inputs), faux_python_version
    )

    # request job execution
    payload = {"parallelism": 1, "starting_index": 0}
    if send_inputs_through_gcs:
        response = requests.post(f"{node_svc_hostname}/jobs/{JOB_ID}", json=payload)
    else:
        function_pkl = cloudpickle.dumps(my_function)
        files = dict(function_pkl=function_pkl)
        data = dict(request_json=json.dumps(payload))
        response = requests.post(f"{node_svc_hostname}/jobs/{JOB_ID}", files=files, data=data)

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        if ("500" in str(e)) and response.text:
            print(response.text)
        raise e

    stop_event = Event()
    job_doc_ref = db.collection("jobs").document(JOB_ID)

    # Start collecting logs generated by this job using a separate thread.
    args = (job_doc_ref, stop_event)
    log_thread = Thread(target=print_logs_from_db, args=args, daemon=True)
    log_thread.start()

    # Start collecting outputs generated by this job using a separate thread.
    output_queue = Queue()
    args = (job_doc_ref, stop_event, output_queue)
    output_thread = Thread(target=enqueue_outputs_from_db, args=args, daemon=True)
    output_thread.start()

    # loop until job is done
    attempts = 0
    outputs = []
    while len(outputs) < len(my_inputs):
        sleep(1)

        while not output_queue.empty():
            outputs.append(output_queue.get())

        _retrieve_and_raise_errors(JOB_ID)

        # try:
        #     response = requests.get(f"{node_svc_hostname}/jobs/{JOB_ID}")
        #     response.raise_for_status()
        #     response_json = response.json()
        # except requests.exceptions.HTTPError as e:
        #     if "404" in str(e):
        #         msg = "Node service returning 404 when attempting to get info about job.\n"
        #         msg += "This should only happen if the job was never started or has recently ended."
        #         print(msg)
        #     else:
        #         raise e

        # if response_json["any_subjobs_failed"] == True:
        #     # exception in container_service,
        #     # if this happens tell user their job failed and it was not their fault.
        #     raise Exception("WORKER FAILED")

        attempts += 1
        if attempts >= 60 * 3:
            raise Exception("TIMEOUT: Job took > 3 minutes to finish?")

    stop_event.set()
    log_thread.join()
    output_thread.join()
    input_uploader_thread.join()
    return outputs


def test_healthcheck(hostname):
    response = requests.get(f"{hostname}/")
    response.raise_for_status()
    assert response.json() == {"status": "READY"}


def test_everything_simple(hostname):
    my_image = None
    my_inputs = ["hi", "hi"]
    my_packages = []

    def my_function(my_input):
        print(f"Processing input: {my_input}")
        return my_input * 2

    return_values = _execute_job(hostname, my_function, my_inputs, my_packages, my_image)

    assert return_values == [my_function(input_) for input_ in my_inputs]

    # _wait_until_node_svc_not_busy(hostname)
    # _assert_node_service_left_proper_containers_running()


def test_UDF_error(hostname):
    my_image = None
    my_inputs = ["hi", "hi"]
    my_packages = []

    def my_function(my_input):
        print(1 / 0)
        return my_input * 2

    with pytest.raises(ZeroDivisionError):
        _execute_job(hostname, my_function, my_inputs, my_packages, my_image)

    # _wait_until_node_svc_not_busy(hostname)
    # _assert_node_service_left_proper_containers_running()


def test_incompatible_containers_error(hostname):
    my_image = None
    my_inputs = ["hi", "hi"]
    my_packages = []

    def my_function(my_input):
        return my_input * 2

    with pytest.raises(requests.exceptions.HTTPError):
        _execute_job(
            hostname, my_function, my_inputs, my_packages, my_image, faux_python_version="3.9"
        )
