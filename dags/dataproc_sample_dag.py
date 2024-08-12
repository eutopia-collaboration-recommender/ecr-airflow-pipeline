from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocStartClusterOperator,
    DataprocStopClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

# Define your DAG
default_args = {
    "project_id": "collaboration-recommender",  # Replace with your GCP project ID
    "region": "us-central1",  # Replace with your Dataproc region
    "cluster_name": "cluster-b878",  # Name for the Dataproc cluster
    "bucket_name": "us-central1-eutopia-compose-39e53e1a-bucket",  # Your GCS bucket name
    "start_date": days_ago(1),
}

with DAG(
    "dataproc_workflow",
    default_args=default_args,
    schedule_interval=None,  # Set to None for manual triggering or customize the schedule
    catchup=False,
) as dag:

    # Task 1: Start Dataproc Cluster
    start_cluster = DataprocStartClusterOperator(
        task_id="start_cluster",
        project_id=default_args["project_id"],
        region=default_args["region"],
        cluster_name=default_args["cluster_name"],
    )

    # Task 2: Submit PySpark Job
    submit_pyspark_job = DataprocSubmitJobOperator(
        task_id="submit_pyspark_job",
        project_id=default_args["project_id"],
        region=default_args["region"],
        job={
            "placement": {"cluster_name": default_args["cluster_name"]},
            "pyspark_job": {
                "main_python_file_uri": f"gs://{default_args['bucket_name']}/spark/author_matching_via_orcid.py"
            },
        },
    )
    # Task 3: Stop Dataproc Cluster
    stop_cluster = DataprocStopClusterOperator(
        task_id="stop_cluster",
        project_id=default_args["project_id"],
        region=default_args["region"],
        cluster_name=default_args["cluster_name"],
        trigger_rule=TriggerRule.ALL_DONE,  # Ensures cluster is stopped even if the job fails
    )

    # Set task dependencies
    start_cluster.set_downstream(submit_pyspark_job)
    submit_pyspark_job.set_downstream(stop_cluster)
