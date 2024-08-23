import sys
import json
import pickle
import pytest
import requests
from uuid import uuid4
from time import sleep
from six import reraise
from queue import Queue
from threading import Thread, Event

import cloudpickle
import docker
from tblib import Traceback
from google.cloud import firestore
from google.cloud.storage import Client, Blob
from google.cloud import pubsub
from google.cloud.pubsub_v1.types import BatchSettings

from env_builder import start_building_environment
from conftest import CONTAINERS

"""
If node_service is imported anywhere here the containers will be started 
then deleted by `delete_containers` in conftest before testing starts!!
DO NOT import node_service here.
"""

GCS_CLIENT = Client()
INPUTS_TOPIC_PATH = f"projects/burla-test/topics/burla_job_inputs"
OUTPUTS_SUBSCRIPTION_PATH = f"projects/burla-test/subscriptions/burla_job_outputs"
LOGS_SUBSCRIPTION_PATH = f"projects/burla-test/subscriptions/burla_job_logs"


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


def print_logs_from_stream(subscriber: pubsub.SubscriberClient, stop_event: Event):

    def callback(message):
        message.ack()
        try:
            print(message.data.decode())
        except:
            # ignore messages that cannot be unpickled (are not pickled)
            # ack these messages anyway so they don't loop through this subsctiption
            print(f"ERROR: data instance: {type(message.data)}, data: {message.data}")
            pass

    future = subscriber.subscribe(LOGS_SUBSCRIPTION_PATH, callback=callback)
    while not stop_event.is_set():
        try:
            future.result(timeout=0.1)
        except TimeoutError:
            continue


def enqueue_outputs_from_stream(
    subscriber: pubsub.SubscriberClient, stop_event: Event, output_queue: Queue
):

    def callback(message):
        message.ack()
        try:
            print(f"received output message: {message.data}")
            output_queue.put(cloudpickle.loads(message.data))
        except:
            # ignore messages that cannot be unpickled (are not pickled)
            # ack these messages anyway so they don't loop through this subsctiption
            pass

    future = subscriber.subscribe(OUTPUTS_SUBSCRIPTION_PATH, callback=callback)
    while not stop_event.is_set():
        try:
            future.result(timeout=0.1)
        except TimeoutError:
            continue


def upload_inputs(inputs: list):
    batch_settings = BatchSettings(max_bytes=10000000, max_latency=0.01, max_messages=1000)
    publisher = pubsub.PublisherClient(batch_settings=batch_settings)

    if not (0 <= len(inputs) <= 4294967295):
        raise ValueError("too many inputs: ID does not fit in 4 bytes.")

    for input_index, input_ in enumerate(inputs):
        packed_data = input_index.to_bytes(length=4, byteorder="big") + cloudpickle.dumps(input_)
        publisher.publish(topic=INPUTS_TOPIC_PATH, data=packed_data)

    print("All inputs uploaded.")


def _create_job_document_in_database(job_id, image, dependencies):
    db = firestore.Client(project="burla-test")
    job_ref = db.collection("jobs").document(job_id)
    job_ref.set(
        {
            "test": True,
            "function_uri": f"gs://burla-jobs/12345/{job_id}/function.pkl",
            "env": {
                "is_copied_from_client": bool(dependencies),
                "image": image,
                "packages": dependencies,
                "python_version": f"3.{sys.version_info.minor}",
            },
        }
    )


def _retrieve_and_raise_errors(job_id):
    db = firestore.Client(project="burla-test")
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
    from node_service import N_CPUS  # <- see note near import statements at top.

    db = firestore.Client(project="burla-test")
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
        in_standby = len(containers) == len(node["containers"]) * N_CPUS

        sleep(2)
        if attempts == 10:
            raise Exception("standby containers not started ??")
        attempts += 1


def _execute_job(
    node_svc_hostname, my_function, my_inputs, my_packages, my_image, send_inputs_through_gcs=False
):
    JOB_ID = str(uuid4()) + "-test"
    DEFAULT_IMAGE = "us-docker.pkg.dev/burla-test/burla-job-containers/default/image-nogpu:latest"
    image = my_image if my_image else DEFAULT_IMAGE

    if send_inputs_through_gcs:
        _upload_function_to_gcs(JOB_ID, my_function)
        _upload_inputs_to_gcs(JOB_ID, my_inputs)
        _create_job_document_in_database(JOB_ID, image, my_packages)
    else:
        _create_job_document_in_database(JOB_ID, image, my_packages)

    if my_packages:
        start_building_environment(JOB_ID, image=my_image if my_image else DEFAULT_IMAGE)

    # in separate thread start uploading inputs:
    input_uploader_thread = Thread(target=upload_inputs, args=(my_inputs,), daemon=True)
    input_uploader_thread.start()

    # request job execution
    if send_inputs_through_gcs:
        response = requests.post(f"{node_svc_hostname}/jobs/{JOB_ID}", json={"parallelism": 1})
    else:
        function_pkl = cloudpickle.dumps(my_function)
        files = dict(function_pkl=function_pkl)
        data = dict(request_json=json.dumps({"parallelism": 1}))
        response = requests.post(f"{node_svc_hostname}/jobs/{JOB_ID}", files=files, data=data)
    response.raise_for_status()

    stop_event = Event()
    subscriber = pubsub.SubscriberClient()

    # Start collecting logs generated by this job using a separate thread.
    args = (subscriber, stop_event)
    log_thread = Thread(target=print_logs_from_stream, args=args, daemon=True)
    log_thread.start()

    # Start collecting outputs generated by this job using a separate thread.
    output_queue = Queue()
    args = (subscriber, stop_event, output_queue)
    output_thread = Thread(target=enqueue_outputs_from_stream, args=args, daemon=True)
    output_thread.start()

    # loop until job is done
    attempts = 0
    outputs = []
    sleep_duration = 2
    while len(outputs) < len(my_inputs):
        sleep(sleep_duration)

        while not output_queue.empty():
            outputs.append(output_queue.get())

        _retrieve_and_raise_errors(JOB_ID)

        response = requests.get(f"{node_svc_hostname}/jobs/{JOB_ID}")
        response.raise_for_status()
        response_json = response.json()

        if response_json["any_subjobs_failed"] == True:
            # exception in container_service,
            # if this happens tell user their job failed and it was not their fault.
            raise Exception("EXECUTOR FAILED")

        attempts += 1
        if attempts * sleep_duration >= 60 * 3:
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


def test_version(hostname):
    response = requests.get(f"{hostname}/version")
    response.raise_for_status()
    print(response.json())


def test_everything_simple(hostname):
    """this is an e2e integration test that relies on live infrastructure"""
    my_image = None
    my_inputs = ["hi", "hi"]
    my_packages = []

    def my_function(my_input):
        return my_input * 2

    return_values = _execute_job(hostname, my_function, my_inputs, my_packages, my_image)

    assert return_values == ["hihi", "hihi"]
    # _wait_until_node_svc_not_busy(hostname)
    # _assert_node_service_left_proper_containers_running()


def test_UDF_error(hostname):
    """this is an e2e integration test that relies on live infrastructure"""
    my_image = None
    my_inputs = ["hi", "hi"]
    my_packages = []

    def my_function(my_input):
        print(1 / 0)
        return my_input * 2

    with pytest.raises(ZeroDivisionError):
        _execute_job(hostname, my_function, my_inputs, my_packages, my_image)

    _wait_until_node_svc_not_busy(hostname)
    _assert_node_service_left_proper_containers_running()


def test_everything_with_packages(hostname):
    """this is an e2e integration test that relies on live infrastructure"""
    my_image = None
    my_inputs = [2, 3]
    my_packages = [{"name": "pandas", "version": "2.0.3"}, {"name": "matplotlib"}]

    def my_function(my_input):
        import pandas as pd

        df = pd.DataFrame({"hello": my_input * ["world"]})
        return len(df)

    return_values = _execute_job(hostname, my_function, my_inputs, my_packages, my_image)
    assert 2 in return_values
    assert 3 in return_values

    _wait_until_node_svc_not_busy(hostname)
    _assert_node_service_left_proper_containers_running()


def test_BUSY_error(hostname):
    """this is an e2e integration test that relies on live infrastructure"""
    from node_service import N_CPUS  # <- see note near import statements at top.

    my_image = None
    my_inputs = ["hi", "hi"]
    my_packages = []

    def my_function(my_input):
        sleep(2)
        return my_input * 2

    JOB_ID = str(uuid4()) + "-test"
    JOB_2_ID = str(uuid4()) + "-test"
    SUBJOB_IDS = list(range(len(my_inputs)))
    DEFAULT_IMAGE = "us-docker.pkg.dev/burla-test/burla-job-environments/default-image:latest"
    image = my_image if my_image else DEFAULT_IMAGE
    _upload_function_to_gcs(JOB_ID, my_function)
    _upload_inputs_to_gcs(JOB_ID, my_inputs)
    _create_job_document_in_database(JOB_ID, SUBJOB_IDS, image, my_packages)
    if my_packages:
        start_building_environment(JOB_ID, image=my_image if my_image else DEFAULT_IMAGE)

    # request job execution
    payload = {"parallelism": 1}
    response = requests.post(f"{hostname}/jobs/{JOB_ID}", json=payload)
    response.raise_for_status()

    response = requests.post(f"{hostname}/jobs/{JOB_2_ID}", json=payload)
    assert response.status_code == 409

    # restart node service before job is done
    response = requests.post(f"{hostname}/reboot", json=CONTAINERS)
    response.raise_for_status()
