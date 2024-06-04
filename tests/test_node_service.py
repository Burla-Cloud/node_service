import sys
import pickle
import pytest
import requests
from uuid import uuid4
from time import sleep, time
from six import reraise

import cloudpickle
import docker
from tblib import Traceback
from google.cloud import firestore
from google.cloud.storage import Client, Blob

from env_builder import start_building_environment

"""
If node_service is imported anywhere here the containers will be started 
then deleted by `delete_containers` in conftest before testing starts!!
DO NOT import node_service here.
"""

GCS_CLIENT = Client()


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


def _create_job_document_in_database(job_id, subjob_ids, image, dependencies):
    db = firestore.Client(project="burla-test")
    job_ref = db.collection("jobs").document(job_id)
    job_ref.set(
        {
            "test": True,
            "function_uri": f"gs://burla-jobs/12345/{job_id}/function.pkl",
            "python_version": f"3.{sys.version_info.minor}",
            "env": {
                "is_copied_from_client": bool(dependencies),
                "image": image,
                "packages": dependencies,
            },
        }
    )
    sub_jobs_collection = job_ref.collection("sub_jobs")
    for subjob_id in subjob_ids:
        sub_jobs_collection.document(str(subjob_id)).set({"claimed": False})


def _retrieve_and_raise_errors(job_id, subjob_ids):
    db = firestore.Client(project="burla-test")
    job_ref = db.collection("jobs").document(job_id)

    env_install_error = job_ref.get().to_dict()["env"].get("install_error")
    if env_install_error:
        raise Exception(env_install_error)

    for subjob_id in subjob_ids:
        subjob_doc = job_ref.collection("sub_jobs").document(str(subjob_id)).get()

        if not subjob_doc.exists:
            raise Exception("subjob document missing in database!")

        subjob = subjob_doc.to_dict()
        if subjob.get("udf_error"):
            exception_info = pickle.loads(bytes.fromhex(subjob["udf_error"]))
            reraise(
                tp=exception_info["exception_type"],
                value=exception_info["exception"],
                tb=Traceback.from_dict(exception_info["traceback_dict"]).as_traceback(),
            )


def _print_udf_response_times(job_requested_at, job_id, subjob_ids):
    db = firestore.Client(project="burla-test")
    job_ref = db.collection("jobs").document(job_id)
    for subjob_id in subjob_ids:
        subjob = job_ref.collection("sub_jobs").document(str(subjob_id)).get().to_dict()
        response_time = subjob["udf_started_at"] - job_requested_at
        print(f"UDF started running {response_time} seconds after request!")


def _download_return_values_from_gcs(job_id, subjob_ids):
    return_values = []
    for subjob_id in subjob_ids:
        output_uri = f"gs://burla-jobs/{job_id}/outputs/{subjob_id}.pkl"
        blob = Blob.from_string(output_uri, GCS_CLIENT)

        # wait for blob to appear, this takes a while sometimes idk why
        checks = 0
        while not blob.exists():
            sleep(5)
            if checks == 30:
                raise Exception("Return value GCS blob should exist by now, but doesn't")
            else:
                checks += 1

        return_values.append(cloudpickle.loads(blob.download_as_bytes()))
    return return_values


def _wait_until_node_svc_not_busy(node_svc_hostname, attempt=0):
    response = requests.get(node_svc_hostname, timeout=60)
    response.raise_for_status()

    if response.json()["status"] != "READY":
        sleep(2)
        return _wait_until_node_svc_not_busy(node_svc_hostname, attempt=attempt + 1)


def _assert_node_service_left_proper_containers_running():
    from node_service import N_CPUS  # <- see note near import statements at top.

    db = firestore.Client(project="burla-test")
    config = db.collection("cluster_config").document("cluster_config").get().to_dict()

    client = docker.from_env()
    attempts = 0
    in_standby = False
    while not in_standby:
        containers = client.containers.list(all=True)

        # all container svc running ?
        for container in containers:
            port = int(list(container.attrs["NetworkSettings"]["Ports"].values())[0][0]["HostPort"])
            response = requests.get(f"http://127.0.0.1:{port}")
            response.raise_for_status()
            assert response.json()["status"] == "READY"

        # correct num containers ?
        machine_type = "n1-standard-96"
        for node in config["Nodes"]:
            if node["machine_type"] == machine_type:
                break
        in_standby = len(containers) == len(node["containers"]) * N_CPUS

        sleep(2)
        if attempts == 10:
            raise Exception("standby containers not started ??")
        attempts += 1


def _execute_job(node_svc_hostname, my_function, my_inputs, my_packages, my_image):
    JOB_ID = str(uuid4()) + "-test"
    SUBJOB_IDS = list(range(len(my_inputs)))

    DEFAULT_IMAGE = "us-docker.pkg.dev/burla-test/burla-job-environments/default-image:latest"
    image = my_image if my_image else DEFAULT_IMAGE

    _upload_function_to_gcs(JOB_ID, my_function)
    _upload_inputs_to_gcs(JOB_ID, my_inputs)
    _create_job_document_in_database(JOB_ID, SUBJOB_IDS, image, my_packages)
    if my_packages:
        start_building_environment(JOB_ID, image=my_image if my_image else DEFAULT_IMAGE)

    # request job execution
    job_requested_at = time()
    response = requests.post(f"{node_svc_hostname}/jobs/{JOB_ID}", json={"parallelism": 1})
    response.raise_for_status()

    # loop until job is done
    attempts = 0
    sleep_duration = 5
    while True:
        sleep(sleep_duration)

        response = requests.get(f"{node_svc_hostname}/jobs/{JOB_ID}")
        response.raise_for_status()
        response_json = response.json()

        if response_json["any_failed"] == True:
            # exception in container_service,
            # if this happens tell user their job failed and it was not their fault.
            raise Exception("EXECUTOR FAILED")

        if response_json["all_done"] == True:
            break

        attempts += 1
        if attempts * sleep_duration >= 60 * 3:
            raise Exception("TIMEOUT: Job took > 3 minutes to finish?")

    _retrieve_and_raise_errors(JOB_ID, SUBJOB_IDS)
    # _print_udf_response_times(job_requested_at, JOB_ID, SUBJOB_IDS)
    return_values = _download_return_values_from_gcs(JOB_ID, SUBJOB_IDS)
    return return_values


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
    _wait_until_node_svc_not_busy(hostname)
    _assert_node_service_left_proper_containers_running()


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


def test_input_queue(hostname):
    """this is an e2e integration test that relies on live infrastructure"""
    from node_service import N_CPUS  # <- see note near import statements at top.

    my_image = None
    my_inputs = (N_CPUS * 2) * ["hi"]
    my_packages = []

    def my_function(my_input):
        return my_input * 2

    JOB_ID = str(uuid4()) + "-test"
    SUBJOB_IDS = list(range(len(my_inputs)))
    DEFAULT_IMAGE = "us-docker.pkg.dev/burla-test/burla-job-environments/default-image:latest"
    image = my_image if my_image else DEFAULT_IMAGE

    return_values = _execute_job(hostname, my_function, my_inputs, my_packages, my_image)
    assert return_values == (N_CPUS * 2) * ["hihi"]

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

    response = requests.post(f"{hostname}/jobs/{JOB_ID}", json=payload)
    assert response.status_code == 409

    # restart node service before job is done
    response = requests.post(f"{hostname}/standby", json={})
    response.raise_for_status()
