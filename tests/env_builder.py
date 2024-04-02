
""" Taken from burla_webservice/env_builder.py """

from time import sleep

from google.protobuf.duration_pb2 import Duration
from google.cloud.run_v2 import JobsClient
from google.cloud.run_v2.types import (
    Job,
    ExecutionTemplate,
    TaskTemplate,
    Container,
    EnvVar,
    ResourceRequirements,
)

PROJECT_ID = "burla-test"


def start_building_environment(job_id: str, image: str):
    code = """
    import tarfile
    import subprocess
    import tempfile
    import os
    import sys

    from google.cloud import storage
    from google.cloud import firestore

    PROJECT_ID = os.environ["GCP_PROJECT"]
    JOB_ID = os.environ["BURLA_JOB_ID"]
    BURLA_JOBS_BUCKET = "burla-jobs" if PROJECT_ID == "burla-test" else "burla-jobs-prod"

    job_ref = firestore.Client(project=PROJECT_ID).collection("jobs").document(JOB_ID)

    pkgs_formatted = []
    for pkg in job_ref.get().to_dict()["env"]["packages"]:
        if pkg.get("version"):
            pkgs_formatted.append(f"{pkg['name']}=={pkg['version']}")
        else:
            pkgs_formatted.append(pkg["name"])

    temp_dir = tempfile.mkdtemp()
    command = [
        sys.executable,
        "-m",
        "pip",
        "install",
        *pkgs_formatted,
        "--target",
        temp_dir,
        "--use-deprecated",
        "legacy-resolver"
    ]
    result = subprocess.run(command, stderr=subprocess.PIPE)
    print("DONE EXECUTING PIP INSTALL COMMAND")
    if result.returncode != 0:
        job_ref.update({"env.install_error": result.stderr.decode()})
        sys.exit(0)

    print("TARRING")
    tar_path = tempfile.mktemp(suffix=".tar.gz")
    with tarfile.open(tar_path, "w:gz") as tar:
        tar.add(temp_dir, arcname=os.path.sep)
    print("DONE TARRING")

    print("UPLOADING")
    bucket_name = BURLA_JOBS_BUCKET
    blob_name = f"{JOB_ID}/env.tar.gz"
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(tar_path)
    print("DONE UPLOADING")

    uri = f"gs://{BURLA_JOBS_BUCKET}/{blob_name}"
    job_ref.update({"env.uri": uri})
    print(f"Successfully built environment at: {uri}")
    """
    formatted_code = code.replace('"', r'\"').replace("\n    ", "\n")
    python_command = f'/.pyenv/versions/3.11.*/bin/python3.11 -c "{formatted_code}"'

    container = Container(
        image=image,
        command=["/bin/sh", "-c", python_command],
        env=[
            EnvVar(name="BURLA_JOB_ID", value=job_id),
            EnvVar(name="GCP_PROJECT", value=PROJECT_ID),
        ],
        resources=ResourceRequirements(limits={"memory": "32Gi", "cpu": "8"}),
    )
    timeout = Duration(seconds=3600)  # 1hr
    task_template = TaskTemplate(max_retries=0, containers=[container], timeout=timeout)
    job = Job(template=ExecutionTemplate(template=task_template))
    client = JobsClient()

    cloud_run_job_id = f"env-build-{job_id}"
    parent_resource = f"projects/{PROJECT_ID}/locations/us-central1"
    cloud_run_job_name = f"{parent_resource}/jobs/{cloud_run_job_id}"
    client.create_job(parent=parent_resource, job=job, job_id=cloud_run_job_id)

    while client.get_job(name=cloud_run_job_name).reconciling:
        sleep(1)
    client.run_job(name=cloud_run_job_name)

