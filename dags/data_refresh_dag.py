####################################################################################################
# Import section
####################################################################################################
import logging
from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocStartClusterOperator,
    DataprocStopClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.utils.trigger_rule import TriggerRule
from google.cloud import bigquery

from util.common import offload_df_to_bigquery, offload_df_to_bigquery_via_stage
from util.orcid import (
    fetch_access_token,
    fetch_orcid_by_doi,
    fetch_orcid_id,
    query_dois,
    query_orcid_ids,
)

####################################################################################################
# Global variables
####################################################################################################
PROJECT_ID = "collaboration-recommender"
INGESTION_SCHEMA = "DATALAKE"

# Configuration for the Dataproc cluster
DATAPROC_CLUSTER_CONFIG = {
    "name": "cluster-b878",
    "region": "us-central1",
    "bucket_name": "us-central1-eutopia-compose-39e53e1a-bucket",
}

# Task configuration for the ORCID API data ingestion from Crossref DOIs
ORCID_IDS_FROM_CROSSREF_DOIS_TASK = {
    "source_table_id": f"{PROJECT_ID}.{INGESTION_SCHEMA}.CROSSREF_HISTORIC_ARTICLE_EUTOPIA",
    "target_table_id": f"{PROJECT_ID}.{INGESTION_SCHEMA}.ORCID_API_AUTHOR_FROM_CROSSREF_DOIS",
    "n_max_iterations": None,
    "n_max_iterations_to_offload": 10000,
}

# Task configuration for the ORCID API data ingestion from ORCID IDs
ORCID_RECORDS_FROM_IDS_TASK = {
    "source_table_id": f"{PROJECT_ID}.{INGESTION_SCHEMA}.ORCID_API_AUTHOR_FROM_CROSSREF_DOIS",
    "target_table_id": f"{PROJECT_ID}.{INGESTION_SCHEMA}.ORCID_API_AUTHOR_FROM_ORCID_IDS",
    "stage_table_id": f"{PROJECT_ID}.{INGESTION_SCHEMA}.ORCID_API_AUTHOR_STAGE",
    "n_max_iterations": None,
    "n_max_iterations_to_offload": 100,
}


####################################################################################################
# DAG definition
####################################################################################################

default_args = {
    "owner": "Luka",
    "start_date": datetime(2024, 8, 1),
    "retries": 0,
}

dag = DAG(
    "data_refresh_dag",
    default_args=default_args,
    description="Refresh the entire data pipeline.",
    schedule_interval="@once",
    catchup=False,
)

####################################################################################################
# Task Python functions
####################################################################################################


def orcid_ids_from_crossref_dois():
    # Initialize the BigQuery client
    bq_client = bigquery.Client(project=PROJECT_ID)
    # Fetch the ORCID access token
    orcid_access_token = fetch_access_token(project_id=PROJECT_ID)

    logging.info("Starting the process of Orcid API data ingestion...")

    # Initialize for iteration
    lst_dois = query_dois(
        bq_client=bq_client,
        source_table_id=ORCID_IDS_FROM_CROSSREF_DOIS_TASK["source_table_id"],
        target_table_id=ORCID_IDS_FROM_CROSSREF_DOIS_TASK["target_table_id"],
    )
    lst_doi_authors = list()
    request_limit_queue = list()

    # Iterate over the DOIs
    for ix_iteration, doi in enumerate(lst_dois):
        # If the iteration index exceeds the maximum number of iterations to offload, offload the data to BigQuery
        if (
            ix_iteration > 0
            and ix_iteration
            % ORCID_IDS_FROM_CROSSREF_DOIS_TASK["n_max_iterations_to_offload"]
            == 0
        ):
            offload_df_to_bigquery(
                df=pd.DataFrame(lst_doi_authors),
                table_id=ORCID_IDS_FROM_CROSSREF_DOIS_TASK["target_table_id"],
                client=bq_client,
            )
            lst_doi_authors = list()

        # Process the tarfile file
        doi_list_orcid_ids = fetch_orcid_by_doi(
            doi=doi,
            access_token=orcid_access_token,
            request_limit_queue=request_limit_queue,
        )

        # Append the list of DOI authors to the list
        lst_doi_authors.append(doi_list_orcid_ids)

        # Break the loop if the maximum number of records is exceeded
        if (
            ORCID_IDS_FROM_CROSSREF_DOIS_TASK["n_max_iterations"] is not None
            and ix_iteration >= ORCID_IDS_FROM_CROSSREF_DOIS_TASK["n_max_iterations"]
        ):
            break

    # Offload the remaining articles to BigQuery
    if len(lst_doi_authors) > 0:
        offload_df_to_bigquery(
            df=pd.DataFrame(lst_doi_authors),
            table_id=ORCID_IDS_FROM_CROSSREF_DOIS_TASK["target_table_id"],
            client=bq_client,
        )

    logging.info("Orcid API data ingestion completed.")


def orcid_records_from_ids():
    def fetch_orcid_authors():
        bq_client = bigquery.Client(project=PROJECT_ID)

        # Initialize the list of articles
        lst_authors = list()

        logging.info("Starting the process of Orcid API data ingestion...")

        # Fetch all the authors that are neither in target table, historic offload table but are in the source table
        orcid_ids = query_orcid_ids(
            source_table_id=ORCID_RECORDS_FROM_IDS_TASK["source_table_id"],
            target_table_id=ORCID_RECORDS_FROM_IDS_TASK["target_table_id"],
            bq_client=bq_client,
        )

        logging.info(f"Number of ORCID IDs to be ingested: {len(orcid_ids)}")
        # Fetch the ORCID access token
        orcid_access_token = fetch_access_token(project_id=PROJECT_ID)
        request_limit_queue = list()

        for ix_iteration, orcid_id in enumerate(orcid_ids):
            # If the iteration index exceeds the maximum number of iterations to offload, offload the data to BigQuery
            if (
                ix_iteration > 0
                and ix_iteration
                % ORCID_RECORDS_FROM_IDS_TASK["n_max_iterations_to_offload"]
                == 0
            ):
                # First offload the data to the stage table
                offload_df_to_bigquery_via_stage(
                    df=pd.DataFrame(lst_authors),
                    table_id=ORCID_RECORDS_FROM_IDS_TASK["target_table_id"],
                    stage_table_id=ORCID_RECORDS_FROM_IDS_TASK["stage_table_id"],
                    client=bq_client,
                )
                lst_authors = list()
                print(f"Iteration: {ix_iteration} of {len(orcid_ids)}")

            # Process the tarfile file
            author = fetch_orcid_id(
                orcid_id=orcid_id,
                access_token=orcid_access_token,
                request_limit_queue=request_limit_queue,
            )
            lst_authors.append(author)

            # Break the loop if the maximum number of records is exceeded
            if (
                ORCID_RECORDS_FROM_IDS_TASK["n_max_iterations"] is not None
                and ix_iteration >= ORCID_RECORDS_FROM_IDS_TASK["n_max_iterations"]
            ):
                break

        # Offload the remaining articles to BigQuery
        if len(lst_authors) > 0:
            offload_df_to_bigquery_via_stage(
                df=pd.DataFrame(lst_authors),
                table_id=ORCID_RECORDS_FROM_IDS_TASK["target_table_id"],
                stage_table_id=ORCID_RECORDS_FROM_IDS_TASK["stage_table_id"],
                client=bq_client,
            )

        logging.info("Orcid API data ingestion completed.")


####################################################################################################
# Task definitions
####################################################################################################
with dag:

    # Task to start Dataproc cluster
    start_cluster_task = DataprocStartClusterOperator(
        task_id="start_cluster_task",
        project_id=PROJECT_ID,
        region=DATAPROC_CLUSTER_CONFIG["region"],
        cluster_name=DATAPROC_CLUSTER_CONFIG["name"],
    )

    # Task to fetch ORCID IDs from Crossref DOIs
    orcid_ids_from_crossref_dois_task = PythonOperator(
        task_id="orcid_ids_from_crossref_dois_task",
        python_callable=orcid_ids_from_crossref_dois,
        dag=dag,
    )

    # Task to fetch ORCID records from ORCID IDs
    orcid_records_from_ids_task = PythonOperator(
        task_id="orcid_records_from_ids_task",
        python_callable=orcid_records_from_ids,
        dag=dag,
    )

    # Task to build base dbt model
    dbt_build_task = BashOperator(
        task_id="dbt_build_base_task",
        bash_command="dbt build --project-dir /home/airflow/gcs/dags --profiles-dir /home/airflow/gcs/data --select tag:base",
        dag=dag,
    )

    # Task submit PySpark job to Dataproc: Author matching via ORCID
    author_matching_via_orcid_task = DataprocSubmitJobOperator(
        task_id="author_matching_via_orcid_task",
        project_id=PROJECT_ID,
        region=DATAPROC_CLUSTER_CONFIG["region"],
        job={
            "placement": {"cluster_name": DATAPROC_CLUSTER_CONFIG["name"]},
            "pyspark_job": {
                "main_python_file_uri": f"gs://{DATAPROC_CLUSTER_CONFIG['bucket_name']}/spark/author_matching_via_orcid.py"
            },
        },
    )

    # Task to stop Dataproc cluster
    stop_cluster_task = DataprocStopClusterOperator(
        task_id="stop_cluster",
        project_id=PROJECT_ID,
        region=DATAPROC_CLUSTER_CONFIG["region"],
        cluster_name=DATAPROC_CLUSTER_CONFIG["name"],
        trigger_rule=TriggerRule.ALL_DONE,  # Ensures cluster is stopped even if the job fails
    )
