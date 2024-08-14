####################################################################################################
# Import section
####################################################################################################
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocStartClusterOperator,
    DataprocStopClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.utils.trigger_rule import TriggerRule

from util.common.common import get_sheet_data
from util.data_refresh_tasks import (
    article_language_classification_task_wrapper,
    article_research_area_classification_task_wrapper,
    author_match_connected_component_by_neighborhood_task_wrapper,
    materialize_crossref_article_eutopia_task_wrapper,
    orcid_ids_from_crossref_dois_task_wrapper,
    orcid_records_from_ids_task_wrapper,
    text_embedding_article_gecko_task_wrapper,
)

####################################################################################################
# Global variables
####################################################################################################
PROJECT_ID = "collaboration-recommender"
INGESTION_SCHEMA = "AIRFLOW"
ANALYTICS_SCHEMA = "ANALYTICS"
DBT_TARGET_SCHEMA = "PROD"
MDM_CONFIG = {
    "spreadsheet_id": "1xyDk6odE7wmRz-X0nQysTPEBEAjkoHa6q7vE-_akoIg",
    "sheet_id": "COMPOSER_ACTIVE_TASKS",
    "range": "A1:B1000",
}

ACTIVE_TASK_DICT = (
    get_sheet_data(
        spreadsheet_id=MDM_CONFIG["spreadsheet_id"],
        sheet_id=MDM_CONFIG["sheet_id"],
        data_range=MDM_CONFIG["range"],
    )
    .set_index("TASK")
    .to_dict()["IS_TASK_ACTIVE"]
)


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
    "target_table_id": f"{PROJECT_ID}.{INGESTION_SCHEMA}.ORCID_API_AUTHOR",
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

# Task configuration for author matching via ORCID
AUTHOR_MATCHING_VIA_NEIGHBORHOOD_TASK = {
    "pyspark_job": "author_matching_via_neighborhood.py",
}

# Task configuration for new text embedding article Gecko
TEXT_EMBEDDING_ARTICLE_GECKO_TASK = {
    "procedure_id": f"{PROJECT_ID}.{ANALYTICS_SCHEMA}.SP_FILL_TEXT_EMBEDDING_ARTICLE_GECKO",
    "source_table_id": f"{PROJECT_ID}.{DBT_TARGET_SCHEMA}.STG_EMBEDDING_ARTICLE",
    "target_table_id": f"{PROJECT_ID}.{ANALYTICS_SCHEMA}.TEXT_EMBEDDING_ARTICLE_GECKO",
}

# Task configuration for the research area classification
ARTICLE_RESEARCH_AREA_CLASSIFICATION_TASK = {
    "source_table_id_article_embedding": f"{PROJECT_ID}.{ANALYTICS_SCHEMA}.TEXT_EMBEDDING_ARTICLE_GECKO",
    "source_table_id_research_area_embedding": f"{PROJECT_ID}.{ANALYTICS_SCHEMA}.TEXT_EMBEDDING_RESEARCH_AREA_TOP_N_ARTICLES_GECKO",
    "target_table_id": f"{PROJECT_ID}.{ANALYTICS_SCHEMA}.ARTICLE_RESEARCH_AREA",
    "batch_size": 10000,
}

# Task configuration for the article language classification
ARTICLE_LANGUAGE_CLASSIFICATION_TASK = {
    "source_table_id": f"{PROJECT_ID}.{DBT_TARGET_SCHEMA}.STG_EMBEDDING_ARTICLE",
    "target_table_id": f"{PROJECT_ID}.{ANALYTICS_SCHEMA}.ARTICLE_LANGUAGE",
    "n_max_iterations": None,
    "n_max_iterations_to_offload": 100,
}

# Task author match connected component by neighborhood
AUTHOR_MATCH_CONNECTED_COMPONENT_BY_NEIGHBORHOOD_TASK = {
    "source_table_id": f"{PROJECT_ID}.{ANALYTICS_SCHEMA}.AUTHOR_MATCH_CANDIDATE_PAIR_BY_NEIGHBORHOOD",
    "target_table_id": f"{PROJECT_ID}.{ANALYTICS_SCHEMA}.AUTHOR_MATCH_CONNECTED_COMPONENT_BY_NEIGHBORHOOD",
}

####################################################################################################
# DAG definition
####################################################################################################

DAG_NAME = "data_refresh_with_mdm_control_dag"
DAG_DEFAULT_ARGS = {
    "owner": "Luka",
    "start_date": datetime(2024, 8, 1),
    "retries": 0,
}

dag = DAG(
    DAG_NAME,
    default_args=DAG_DEFAULT_ARGS,
    description="Refresh the entire data pipeline.",
    schedule_interval="@once",
    catchup=False,
)


####################################################################################################
# Task Python functions
####################################################################################################


def materialize_crossref_article_eutopia_task():
    # Call wrapper function
    materialize_crossref_article_eutopia_task_wrapper(
        project_id=PROJECT_ID,
        ingestion_schema=INGESTION_SCHEMA,
    )


def orcid_ids_from_crossref_dois_task():
    # Call wrapper function
    orcid_ids_from_crossref_dois_task_wrapper(
        project_id=PROJECT_ID,
        task_config=ORCID_IDS_FROM_CROSSREF_DOIS_TASK,
    )


def orcid_records_from_ids_task():
    # Call wrapper function
    orcid_records_from_ids_task_wrapper(
        project_id=PROJECT_ID,
        task_config=ORCID_RECORDS_FROM_IDS_TASK,
    )


def text_embedding_article_gecko_task():
    # Call wrapper function
    text_embedding_article_gecko_task_wrapper(
        project_id=PROJECT_ID,
        task_config=TEXT_EMBEDDING_ARTICLE_GECKO_TASK,
    )


def article_research_area_classification_task():
    # Call wrapper function
    article_research_area_classification_task_wrapper(
        project_id=PROJECT_ID,
        task_config=ARTICLE_RESEARCH_AREA_CLASSIFICATION_TASK,
    )


def article_language_classification_task():
    # Call wrapper function
    article_language_classification_task_wrapper(
        project_id=PROJECT_ID,
        task_config=ARTICLE_LANGUAGE_CLASSIFICATION_TASK,
    )


def author_match_connected_component_by_neighborhood_task():
    # Call wrapper function
    author_match_connected_component_by_neighborhood_task_wrapper(
        project_id=PROJECT_ID,
        task_config=AUTHOR_MATCH_CONNECTED_COMPONENT_BY_NEIGHBORHOOD_TASK,
    )


with dag:
    ####################################################################################################
    # Task definitions
    ####################################################################################################

    # Task to materialize CROSSREF_ARTICLE_EUTOPIA
    if ACTIVE_TASK_DICT["materialize_crossref_article_eutopia_task"] == "TRUE":
        materialize_crossref_article_eutopia_task = PythonOperator(
            task_id="materialize_crossref_article_eutopia_task",
            python_callable=materialize_crossref_article_eutopia_task,
            dag=dag,
        )
    else:
        materialize_crossref_article_eutopia_task = BashOperator(
            task_id="materialize_crossref_article_eutopia_task",
            bash_command='echo "Task `materialize_crossref_article_eutopia_task` is not active and will not be executed."',
            dag=dag,
        )

    # Task to start Dataproc cluster
    if ACTIVE_TASK_DICT["start_cluster_task"] == "TRUE":
        start_cluster_task = DataprocStartClusterOperator(
            task_id="start_cluster_task",
            project_id=PROJECT_ID,
            region=DATAPROC_CLUSTER_CONFIG["region"],
            cluster_name=DATAPROC_CLUSTER_CONFIG["name"],
        )
    else:
        start_cluster_task = BashOperator(
            task_id="start_cluster_task",
            bash_command='echo "Task `start_cluster_task` is not active and will not be executed."',
            dag=dag,
        )

    # Task to fetch ORCID IDs from Crossref DOIs
    if ACTIVE_TASK_DICT["orcid_ids_from_crossref_dois_task"] == "TRUE":
        orcid_ids_from_crossref_dois_task = PythonOperator(
            task_id="orcid_ids_from_crossref_dois_task",
            python_callable=orcid_ids_from_crossref_dois_task,
            dag=dag,
        )
    else:
        orcid_ids_from_crossref_dois_task = BashOperator(
            task_id="orcid_ids_from_crossref_dois_task",
            bash_command='echo "Task `orcid_ids_from_crossref_dois_task` is not active and will not be executed."',
            dag=dag,
        )

    # Task to fetch ORCID records from ORCID IDs
    if ACTIVE_TASK_DICT["orcid_records_from_ids_task"] == "TRUE":
        orcid_records_from_ids_task = PythonOperator(
            task_id="orcid_records_from_ids_task",
            python_callable=orcid_records_from_ids_task,
            dag=dag,
        )
    else:
        orcid_records_from_ids_task = BashOperator(
            task_id="orcid_records_from_ids_task",
            bash_command='echo "Task `orcid_records_from_ids_task` is not active and will not be executed."',
            dag=dag,
        )

    # Task to build base dbt model
    if ACTIVE_TASK_DICT["dbt_build_base_task"] == "TRUE":
        dbt_build_base_task = BashOperator(
            task_id="dbt_build_base_task",
            bash_command=f"dbt build --project-dir {DBT_BUILD_BASE_TASK['project_dir']} --profiles-dir {DBT_BUILD_BASE_TASK['profiles_dir']} --select {DBT_BUILD_BASE_TASK['select']}",
            dag=dag,
        )
    else:
        dbt_build_base_task = BashOperator(
            task_id="dbt_build_base_task",
            bash_command='echo "Task `dbt_build_base_task` is not active and will not be executed."',
            dag=dag,
        )

    # Task submit PySpark job to Dataproc: Author matching via ORCID
    if ACTIVE_TASK_DICT["author_matching_via_orcid_task"] == "TRUE":
        author_matching_via_orcid_task = DataprocSubmitJobOperator(
            task_id="author_matching_via_orcid_task",
            project_id=PROJECT_ID,
            region=DATAPROC_CLUSTER_CONFIG["region"],
            job={
                "placement": {"cluster_name": DATAPROC_CLUSTER_CONFIG["name"]},
                "pyspark_job": {
                    "main_python_file_uri": f"gs://{DATAPROC_CLUSTER_CONFIG['bucket_name']}/spark/{AUTHOR_MATCHING_VIA_ORCID_TASK['pyspark_job']}",
                    "properties": {
                        "spark.jars.packages": "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.24.2",
                    },
                },
            },
        )
    else:
        author_matching_via_orcid_task = BashOperator(
            task_id="author_matching_via_orcid_task",
            bash_command='echo "Task `author_matching_via_orcid_task` is not active and will not be executed."',
            dag=dag,
        )

    # Task to author match by neighborhood
    if ACTIVE_TASK_DICT["author_matching_via_neighborhood_task"] == "TRUE":
        author_matching_via_neighborhood_task = DataprocSubmitJobOperator(
            task_id="author_matching_via_neighborhood_task",
            project_id=PROJECT_ID,
            region=DATAPROC_CLUSTER_CONFIG["region"],
            job={
                "placement": {"cluster_name": DATAPROC_CLUSTER_CONFIG["name"]},
                "pyspark_job": {
                    "main_python_file_uri": f"gs://{DATAPROC_CLUSTER_CONFIG['bucket_name']}/spark/{AUTHOR_MATCHING_VIA_NEIGHBORHOOD_TASK['pyspark_job']}",
                    "properties": {
                        "spark.jars.packages": "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.24.2",
                    },
                },
            },
        )
    else:
        author_matching_via_neighborhood_task = BashOperator(
            task_id="author_matching_via_neighborhood_task",
            bash_command='echo "Task `author_matching_via_neighborhood_task` is not active and will not be executed."',
            dag=dag,
        )

    # Task to author match connected component by neighborhood
    if (
        ACTIVE_TASK_DICT["author_match_connected_component_by_neighborhood_task"]
        == "TRUE"
    ):
        author_match_connected_component_by_neighborhood_task = PythonOperator(
            task_id="author_match_connected_component_by_neighborhood_task",
            python_callable=author_match_connected_component_by_neighborhood_task,
            dag=dag,
        )
    else:
        author_match_connected_component_by_neighborhood_task = BashOperator(
            task_id="author_match_connected_component_by_neighborhood_task",
            bash_command='echo "Task `author_match_connected_component_by_neighborhood_task` is not active and will not be executed."',
            dag=dag,
        )

    # Task to embed article contents with Gecko
    if ACTIVE_TASK_DICT["text_embedding_article_gecko_task"] == "TRUE":
        text_embedding_article_gecko_task = PythonOperator(
            task_id="text_embedding_article_gecko_task",
            python_callable=text_embedding_article_gecko_task,
            dag=dag,
        )
    else:
        text_embedding_article_gecko_task = BashOperator(
            task_id="text_embedding_article_gecko_task",
            bash_command='echo "Task `text_embedding_article_gecko_task` is not active and will not be executed."',
            dag=dag,
        )

    # Task to classify research areas for articles
    if ACTIVE_TASK_DICT["article_research_area_classification_task"] == "TRUE":
        article_research_area_classification_task = PythonOperator(
            task_id="article_research_area_classification_task",
            python_callable=article_research_area_classification_task,
            dag=dag,
        )
    else:
        article_research_area_classification_task = BashOperator(
            task_id="article_research_area_classification_task",
            bash_command='echo "Task `article_research_area_classification_task` is not active and will not be executed."',
            dag=dag,
        )

    # Task to classify article languages
    if ACTIVE_TASK_DICT["article_language_classification_task"] == "TRUE":
        article_language_classification_task = PythonOperator(
            task_id="article_language_classification_task",
            python_callable=article_language_classification_task,
            dag=dag,
        )
    else:
        article_language_classification_task = BashOperator(
            task_id="article_language_classification_task",
            bash_command='echo "Task `article_language_classification_task` is not active and will not be executed."',
            dag=dag,
        )

    # Task to stop Dataproc cluster
    if ACTIVE_TASK_DICT["stop_cluster_task"] == "TRUE":
        stop_cluster_task = DataprocStopClusterOperator(
            task_id="stop_cluster_task",
            project_id=PROJECT_ID,
            region=DATAPROC_CLUSTER_CONFIG["region"],
            cluster_name=DATAPROC_CLUSTER_CONFIG["name"],
            trigger_rule=TriggerRule.ALL_DONE,  # Ensures cluster is stopped even if the job fails
        )
    else:
        stop_cluster_task = BashOperator(
            task_id="stop_cluster_task",
            bash_command='echo "Task `stop_cluster_task` is not active and will not be executed."',
            dag=dag,
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
    # -> Author matching via neighborhood
    # -> Author matching connected component by neighborhood
    # -> Stop Dataproc cluster
    dbt_build_base_task.set_downstream(start_cluster_task)
    start_cluster_task.set_downstream(author_matching_via_orcid_task)
    author_matching_via_orcid_task.set_downstream(author_matching_via_neighborhood_task)
    author_matching_via_neighborhood_task.set_downstream(
        author_match_connected_component_by_neighborhood_task
    )
    author_match_connected_component_by_neighborhood_task.set_downstream(
        stop_cluster_task
    )

    # Branch 2 from dbt build base:
    # ... dbt build base
    # -> Embed articles using Gecko
    # -> Classify research areas for articles
    # -> Classify article languages
    dbt_build_base_task.set_downstream(text_embedding_article_gecko_task)
    text_embedding_article_gecko_task.set_downstream(
        article_research_area_classification_task
    )
    article_research_area_classification_task.set_downstream(
        article_language_classification_task
    )
