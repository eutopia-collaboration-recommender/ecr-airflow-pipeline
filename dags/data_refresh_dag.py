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

from util.embedding import (
    classify_research_area_for_article_batch,
    query_article_count_for_missing_research_area,
    query_research_area_embeddings,
)
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
INGESTION_SCHEMA = "AIRFLOW"
ANALYTICS_SCHEMA = "ANALYTICS"
DBT_TARGET_SCHEMA = "PROD"

# Configuration for the Dataproc cluster
DATAPROC_CLUSTER_CONFIG = {
    "name": "ecr-composer-cluster",
    "region": "us-central1",
    "bucket_name": "ecr-composer-bucket-main",
}

# Task configuration for the ORCID API data ingestion from Crossref DOIs
ORCID_IDS_FROM_CROSSREF_DOIS_TASK = {
    "source_table_id": f"{PROJECT_ID}.{INGESTION_SCHEMA}.CROSSREF_ARTICLE_EUTOPIA",
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


# Task configuration for the dbt build base
DBT_BUILD_BASE_TASK = {
    "project_dir": "/home/airflow/gcs/dags",
    "profiles_dir": "/home/airflow/gcs/data",
    "select": "tag:base",
}

# Task configuration for author matching via ORCID
AUTHOR_MATCHING_VIA_ORCID_TASK = {
    "pyspark_job": "author_matching_via_orcid.py",
}

# Task configuration for new text embedding article Gecko
TEXT_EMBEDDING_ARTICLE_GECKO_TASK = {
    "procedure": f"{PROJECT_ID}.{ANALYTICS_SCHEMA}.SP_FILL_TEXT_EMBEDDING_ARTICLE_GECKO",
}

# Task configuration for the research area classification
RESEARCH_AREA_CLASSIFICATION_TASK = {
    "SOURCE_TABLE_ID_ARTICLE_EMBEDDING": f"{PROJECT_ID}.{ANALYTICS_SCHEMA}.TEXT_EMBEDDING_ARTICLE_GECKO",
    "SOURCE_TABLE_ID_RESEARCH_AREA_EMBEDDING": f"{PROJECT_ID}.{ANALYTICS_SCHEMA}.TEXT_EMBEDDING_RESEARCH_AREA_TOP_N_ARTICLES_GECKO",
    "TARGET_TABLE_ID": "ARTICLE_RESEARCH_AREA",
    "BATCH_SIZE": 10000,
}


####################################################################################################
# DAG definition
####################################################################################################

DAG_NAME = "data_refresh_dag"
DAG_DEFAULT_ARGS = {
    "owner": "Luka",
    "start_date": datetime(2024, 8, 1),
    "retries": 0,
}

dag = DAG(
    "data_refresh_dag",
    default_args=DAG_DEFAULT_ARGS,
    description="Refresh the entire data pipeline.",
    schedule_interval="@once",
    catchup=False,
)


####################################################################################################
# Task Python functions
####################################################################################################


def materialize_crossref_article_eutopia_task():
    # Initialize the BigQuery client
    bq_client = bigquery.Client(project=PROJECT_ID)

    # Query to materialize the CROSSREF_ARTICLE_EUTOPIA table
    query = f"""
    CREATE OR REPLACE TABLE {INGESTION_SCHEMA}.CROSSREF_ARTICLE_EUTOPIA AS
    SELECT DISTINCT DOI
    FROM {INGESTION_SCHEMA}.CROSSREF_HISTORIC_ARTICLE
             LEFT JOIN UNNEST(JSON_EXTRACT_ARRAY(JSON, '$.author')) AS AUTHOR_JSON ON TRUE
             LEFT JOIN UNNEST(JSON_EXTRACT_ARRAY(AUTHOR_JSON, '$.affiliation')) AS AFFILIATION_JSON ON TRUE
    WHERE {INGESTION_SCHEMA}.UDF_IS_EUTOPIA_AFFILIATED_STRING(JSON_EXTRACT_SCALAR(AFFILIATION_JSON, '$.name'))
    """

    # Execute the query
    bq_client.query(query).result()

    logging.info("Materialization of CROSSREF_ARTICLE_EUTOPIA completed.")


def orcid_ids_from_crossref_dois_task():
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


def orcid_records_from_ids_task():
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


def text_embedding_article_gecko_task():
    # Initialize a BigQuery client
    client = bigquery.Client(project=PROJECT_ID)

    # Count rows to be processed
    query = f"""
    SELECT COUNT(1) AS ROWS_TO_PROCESS_COUNT
    FROM {DBT_TARGET_SCHEMA}.EMBEDDING_ARTICLE A
    LEFT JOIN {ANALYTICS_SCHEMA}.TEXT_EMBEDDING_ARTICLE_GECKO TE
                       ON A.ARTICLE_DOI = TE.DOI
    """

    # Run the query
    query_job = client.query(query)

    # Fetch the result
    rows_to_process_count = query_job.result().to_dataframe()["ROWS_TO_PROCESS_COUNT"][
        0
    ]

    logging.info(f"Number of rows to process: {rows_to_process_count}")

    # Define the SQL query to call the stored procedure
    query = """
    CALL ANALYTICS.SP_FILL_TEXT_EMBEDDING_ARTICLE_GECKO()
    """

    # Run the query
    query_job = client.query(query)

    # Fetch the result (the procedure will have completed at this point)
    new_rows_count = query_job.result().to_dataframe()["NEW_ROWS_COUNT"][0]

    # Print the output
    print(f"Number of new rows inserted: {new_rows_count}")


def research_area_classification_task():

    # Create a BigQuery client
    bq_client = bigquery.Client(project=PROJECT_ID)

    logging.info("Fetching all research area embeddings...")

    # Fetch all research area embeddings
    research_area_embeddings_df = query_research_area_embeddings(
        bq_client=bq_client,
        source_table_id=RESEARCH_AREA_CLASSIFICATION_TASK[
            "SOURCE_TABLE_ID_RESEARCH_AREA_EMBEDDING"
        ],
    )

    # Extract the research area embeddings
    research_area_embedding_values = research_area_embeddings_df[
        "EMBEDDING_TENSOR_DATA"
    ].values.tolist()

    logging.info("Fetching article embeddings batch...")

    # Fetch all article embeddings that are not yet in the target table
    while (
        query_article_count_for_missing_research_area(
            bq_client=bq_client,
            source_table_id=RESEARCH_AREA_CLASSIFICATION_TASK[
                "SOURCE_TABLE_ID_ARTICLE_EMBEDDING"
            ],
            target_table_id=RESEARCH_AREA_CLASSIFICATION_TASK["TARGET_TABLE_ID"],
        )
        > 0
    ):

        # Process the article embedding batch
        article_research_area_df = classify_research_area_for_article_batch(
            bq_client=bq_client,
            research_area_embeddings_df=research_area_embeddings_df,
            research_area_embedding_values=research_area_embedding_values,
            source_table_id=RESEARCH_AREA_CLASSIFICATION_TASK[
                "SOURCE_TABLE_ID_ARTICLE_EMBEDDING"
            ],
            target_table_id=RESEARCH_AREA_CLASSIFICATION_TASK["TARGET_TABLE_ID"],
            batch_size=RESEARCH_AREA_CLASSIFICATION_TASK["BATCH_SIZE"],
        )

        # Offload the article research area to BigQuery
        offload_df_to_bigquery(
            df=article_research_area_df,
            table_id=RESEARCH_AREA_CLASSIFICATION_TASK["TARGET_TABLE_ID"],
            client=bq_client,
        )


with dag:
    ####################################################################################################
    # Task definitions
    ####################################################################################################

    # Task to materialize CROSSREF_ARTICLE_EUTOPIA
    materialize_crossref_article_eutopia_task = PythonOperator(
        task_id="materialize_crossref_article_eutopia_task",
        python_callable=materialize_crossref_article_eutopia_task,
        dag=dag,
    )

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
        python_callable=orcid_ids_from_crossref_dois_task,
        dag=dag,
    )

    # Task to fetch ORCID records from ORCID IDs
    orcid_records_from_ids_task = PythonOperator(
        task_id="orcid_records_from_ids_task",
        python_callable=orcid_records_from_ids_task,
        dag=dag,
    )

    # Task to build base dbt model
    dbt_build_base_task = BashOperator(
        task_id="dbt_build_base_task",
        bash_command=f"dbt build --project-dir {DBT_BUILD_BASE_TASK['project_dir']} --profiles-dir {DBT_BUILD_BASE_TASK['profiles_dir']} --select {DBT_BUILD_BASE_TASK['select']}",
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
                "main_python_file_uri": f"gs://{DATAPROC_CLUSTER_CONFIG['bucket_name']}/spark/{AUTHOR_MATCHING_VIA_ORCID_TASK['pyspark_job']}"
            },
        },
    )

    # Task to embed article contents with Gecko
    text_embedding_article_gecko_task = PythonOperator(
        task_id="text_embedding_article_gecko_task",
        python_callable=text_embedding_article_gecko_task,
        dag=dag,
    )

    # Task to classify research areas for articles
    research_area_classification_task = PythonOperator(
        task_id="research_area_classification_task",
        python_callable=research_area_classification_task,
        dag=dag,
    )

    # Task to stop Dataproc cluster
    stop_cluster_task = DataprocStopClusterOperator(
        task_id="stop_cluster_task",
        project_id=PROJECT_ID,
        region=DATAPROC_CLUSTER_CONFIG["region"],
        cluster_name=DATAPROC_CLUSTER_CONFIG["name"],
        trigger_rule=TriggerRule.ALL_DONE,  # Ensures cluster is stopped even if the job fails
    )

    ####################################################################################################
    # Define task dependencies
    ####################################################################################################

    # Materialize CROSSREF_ARTICLE_EUTOPIA task
    # -> ORCID IDs from Crossref DOIs
    # -> ORCID records from IDs
    # -> dbt build base
    materialize_crossref_article_eutopia_task.set_downstream(
        orcid_ids_from_crossref_dois_task
    )
    orcid_ids_from_crossref_dois_task.set_downstream(orcid_records_from_ids_task)
    orcid_records_from_ids_task.set_downstream(dbt_build_base_task)

    # Branch 1 from dbt build base:
    # ... dbt build base
    # -> Start Dataproc cluster
    # -> Author matching via ORCID
    # -> Stop Dataproc cluster
    dbt_build_base_task.set_downstream(start_cluster_task)
    start_cluster_task.set_downstream(author_matching_via_orcid_task)
    author_matching_via_orcid_task.set_downstream(stop_cluster_task)

    # Branch 2 from dbt build base:
    # ... dbt build base
    # -> Embed articles using Gecko
    dbt_build_base_task.set_downstream(text_embedding_article_gecko_task)
