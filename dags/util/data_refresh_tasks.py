import logging
import os
import sys

import networkx as nx
import pandas as pd
from google.cloud import bigquery, secretmanager
from langdetect import detect, LangDetectException

# Add the dags folder to PYTHONPATH
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from common.common import (
    get_secret,
    offload_df_to_bigquery,
    offload_df_to_bigquery_via_stage,
)
from common.embedding import (
    classify_research_area_for_article_batch,
    query_article_count_for_missing_research_area,
    query_research_area_embeddings,
)
from common.orcid import (
    fetch_access_token,
    fetch_orcid_by_doi,
    fetch_orcid_id,
    query_dois,
    query_orcid_ids,
)


def materialize_crossref_article_eutopia_task_wrapper(
    project_id: str, ingestion_schema: str
):
    # Initialize the BigQuery client
    bq_client = bigquery.Client(project=project_id)

    # Query to materialize the CROSSREF_ARTICLE_EUTOPIA table
    query = f"""
    CREATE OR REPLACE TABLE {ingestion_schema}.CROSSREF_ARTICLE_EUTOPIA AS
    SELECT DISTINCT DOI
    FROM {ingestion_schema}.CROSSREF_HISTORIC_ARTICLE
             LEFT JOIN UNNEST(JSON_EXTRACT_ARRAY(JSON, '$.author')) AS AUTHOR_JSON ON TRUE
             LEFT JOIN UNNEST(JSON_EXTRACT_ARRAY(AUTHOR_JSON, '$.affiliation')) AS AFFILIATION_JSON ON TRUE
    WHERE {ingestion_schema}.UDF_IS_EUTOPIA_AFFILIATED_STRING(JSON_EXTRACT_SCALAR(AFFILIATION_JSON, '$.name'))
    """

    # Execute the query
    bq_client.query(query).result()

    logging.info("Materialization of CROSSREF_ARTICLE_EUTOPIA completed.")


def orcid_ids_from_crossref_dois_task_wrapper(project_id: str, task_config: dict):
    # Initialize the BigQuery client
    bq_client = bigquery.Client(project=project_id)

    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()
    client_id = get_secret(
        project_id=project_id, name="ORCID_CLIENT_ID", version="1", client=client
    )
    client_secret = get_secret(
        project_id=project_id, name="ORCID_CLIENT_SECRET", version="1", client=client
    )

    # Fetch the ORCID access token
    orcid_access_token = fetch_access_token(
        project_id=project_id, client_id=client_id, client_secret=client_secret
    )

    logging.info("Starting the process of Orcid API data ingestion...")

    # Initialize for iteration
    lst_dois = query_dois(
        bq_client=bq_client,
        source_table_id=task_config["source_table_id"],
        target_table_id=task_config["target_table_id"],
    )
    lst_doi_authors = list()
    request_limit_queue = list()

    # Iterate over the DOIs
    for ix_iteration, doi in enumerate(lst_dois):
        # If the iteration index exceeds the maximum number of iterations to offload, offload the data to BigQuery
        if (
            ix_iteration > 0
            and ix_iteration % task_config["n_max_iterations_to_offload"] == 0
        ):
            offload_df_to_bigquery(
                df=pd.DataFrame(lst_doi_authors),
                table_id=task_config["target_table_id"],
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
            task_config["n_max_iterations"] is not None
            and ix_iteration >= task_config["n_max_iterations"]
        ):
            break

    # Offload the remaining articles to BigQuery
    if len(lst_doi_authors) > 0:
        offload_df_to_bigquery(
            df=pd.DataFrame(lst_doi_authors),
            table_id=task_config["target_table_id"],
            client=bq_client,
        )

    logging.info("Orcid API data ingestion completed.")


def orcid_records_from_ids_task_wrapper(project_id: str, task_config: dict):
    bq_client = bigquery.Client(project=project_id)

    # Initialize the list of articles
    lst_authors = list()

    logging.info("Starting the process of Orcid API data ingestion...")

    # Fetch all the authors that are neither in target table, historic offload table but are in the source table
    orcid_ids = query_orcid_ids(
        source_table_id=task_config["source_table_id"],
        target_table_id=task_config["target_table_id"],
        bq_client=bq_client,
    )

    logging.info(f"Number of ORCID IDs to be ingested: {len(orcid_ids)}")
    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()
    client_id = get_secret(
        project_id=project_id, name="ORCID_CLIENT_ID", version="1", client=client
    )
    client_secret = get_secret(
        project_id=project_id, name="ORCID_CLIENT_SECRET", version="1", client=client
    )

    # Fetch the ORCID access token
    orcid_access_token = fetch_access_token(
        project_id=project_id, client_id=client_id, client_secret=client_secret
    )
    request_limit_queue = list()

    for ix_iteration, orcid_id in enumerate(orcid_ids):
        # If the iteration index exceeds the maximum number of iterations to offload, offload the data to BigQuery
        if (
            ix_iteration > 0
            and ix_iteration % task_config["n_max_iterations_to_offload"] == 0
        ):
            # First offload the data to the stage table
            offload_df_to_bigquery_via_stage(
                df=pd.DataFrame(lst_authors),
                table_id=task_config["target_table_id"],
                stage_table_id=task_config["stage_table_id"],
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
            task_config["n_max_iterations"] is not None
            and ix_iteration >= task_config["n_max_iterations"]
        ):
            break

    # Offload the remaining articles to BigQuery
    if len(lst_authors) > 0:
        offload_df_to_bigquery_via_stage(
            df=pd.DataFrame(lst_authors),
            table_id=task_config["target_table_id"],
            stage_table_id=task_config["stage_table_id"],
            client=bq_client,
        )

    logging.info("Orcid API data ingestion completed.")


def text_embedding_article_gecko_task_wrapper(project_id: str, task_config: dict):
    # Initialize a BigQuery client
    client = bigquery.Client(project=project_id)

    # Count rows to be processed
    query = f"""
    SELECT COUNT(1) AS ROWS_TO_PROCESS_COUNT
    FROM {task_config['source_table_id']} A
    LEFT JOIN {task_config['target_table_id']} TE
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
    query = f"""
    CALL `{task_config['procedure_id']}`()
    """

    # Run the query
    query_job = client.query(query)

    # Fetch the result (the procedure will have completed at this point)
    new_rows_count = query_job.result().to_dataframe()["NEW_ROWS_COUNT"][0]

    # Print the output
    print(f"Number of new rows inserted: {new_rows_count}")


def article_research_area_classification_task_wrapper(
    project_id: str, task_config: dict
):
    # Create a BigQuery client
    bq_client = bigquery.Client(project=project_id)

    logging.info("Fetching all research area embeddings...")

    # Fetch all research area embeddings
    research_area_embeddings_df = query_research_area_embeddings(
        bq_client=bq_client,
        source_table_id=task_config["source_table_id_research_area_embedding"],
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
            source_table_id=task_config["source_table_id_article_embedding"],
            target_table_id=task_config["target_table_id"],
        )
        > 0
    ):

        # Process the article embedding batch
        article_research_area_df = classify_research_area_for_article_batch(
            bq_client=bq_client,
            research_area_embeddings_df=research_area_embeddings_df,
            research_area_embedding_values=research_area_embedding_values,
            source_table_id=task_config["source_table_id_article_embedding"],
            target_table_id=task_config["target_table_id"],
            batch_size=task_config["batch_size"],
        )

        # Offload the article research area to BigQuery
        offload_df_to_bigquery(
            df=article_research_area_df,
            table_id=task_config["target_table_id"],
            client=bq_client,
        )


def article_language_classification_task_wrapper(project_id: str, task_config: dict):
    # Create a BigQuery client
    bq_client = bigquery.Client(project=project_id)

    logging.info("Fetching all relevant articles to query...")

    # Query all the articles
    articles = (
        bq_client.query(
            f"""
    SELECT * 
    FROM {task_config['source_table_id']} S
    LEFT JOIN {task_config['target_table_id']} T USING(ARTICLE_DOI)
    WHERE T.ARTICLE_DOI IS NULL
    """
        )
        .result()
        .to_dataframe()
    )

    logging.info(f"Fetching article languages for {len(articles)}...")

    lst_article_language = list()

    for ix_iteration, item in enumerate(articles.to_dict("records")):

        # If the iteration index exceeds the maximum number of iterations to offload, offload the data to BigQuery
        if (
            ix_iteration > 0
            and ix_iteration % task_config["n_max_iterations_to_offload"] == 0
        ):
            # First offload the data to the stage table
            offload_df_to_bigquery(
                df=pd.DataFrame(lst_article_language),
                table_id=task_config["target_table_id"],
                client=bq_client,
            )
            lst_article_language = list()
            print(f"Iteration: {ix_iteration} of {len(articles)}")

        language_input = item["LANGUAGE_INPUT"]
        article_doi = item["ARTICLE_DOI"]
        try:
            language = detect(language_input)
        except LangDetectException:
            language = "n/a"

        lst_article_language.append(
            dict(ARTICLE_DOI=article_doi, ARTICLE_LANGUAGE=language)
        )

        # Break the loop if the maximum number of records is exceeded
        if (
            task_config["n_max_iterations"] is not None
            and ix_iteration >= task_config["n_max_iterations"]
        ):
            break

    # Offload the remaining articles to BigQuery
    if len(lst_article_language) > 0:
        offload_df_to_bigquery(
            df=pd.DataFrame(lst_article_language),
            table_id=task_config["target_table_id"],
            client=bq_client,
        )


def author_match_connected_component_by_neighborhood_task_wrapper(
    project_id: str, task_config: dict
):
    # Initialize the BigQuery client
    bq_client = bigquery.Client(project=project_id)

    # Query to materialize the CROSSREF_ARTICLE_EUTOPIA table
    query = f"""
    WITH SRC_AUTHOR_MATCH_CANDIDATE_PAIR AS (SELECT *
                                             FROM {task_config["source_table_id"]}),
         FILTERED AS (SELECT DISTINCT AM.INITIAL_AUTHOR_SID,
                                      AM.NEIGHBOR_OF_NEIGHBOR_SID,
                                      AM.INITIAL_AUTHOR_FULL_NAME,
                                      AM.NEIGHBOR_OF_NEIGHBOR_FULL_NAME,
                                      AM.LEVENSHTEIN_DISTANCE
                      FROM SRC_AUTHOR_MATCH_CANDIDATE_PAIR AM
                      WHERE 
    -- The ratio of the Levenshtein distance to the length of the bigger string should be small enough
                        AM.LEVENSHTEIN_DISTANCE / GREATEST(LENGTH(AM.NEIGHBOR_OF_NEIGHBOR_FULL_NAME)
                                                          , LENGTH(LOWER(AM.INITIAL_AUTHOR_FULL_NAME)))
                          <= 1 / 5 -- We say that if the word is 5 characters long, it can have a distance of 1 character.
    -- Check if the initials of one author is a subset of initials of the other author.
                        AND AIRFLOW.IS_AUTHOR_INITIALS_SUBSET(AM.INITIAL_AUTHOR_FULL_NAME,
                                                              AM.NEIGHBOR_OF_NEIGHBOR_FULL_NAME))
    SELECT INITIAL_AUTHOR_SID,
           NEIGHBOR_OF_NEIGHBOR_SID,
           INITIAL_AUTHOR_FULL_NAME,
           NEIGHBOR_OF_NEIGHBOR_FULL_NAME,
           LEVENSHTEIN_DISTANCE
    FROM FILTERED
    """

    # Execute the query
    df = bq_client.query(query).result().to_dataframe()

    G = nx.Graph()

    for ix, row in df.iterrows():
        G.add_edge(row["INITIAL_AUTHOR_SID"], row["NEIGHBOR_OF_NEIGHBOR_SID"])

    # For each author, no matter if it is the initial author or the neighbor of the neighbor, we will have a node
    # in the graph. For each author create a mapping between the author SID and the connected component it belongs to
    components = nx.connected_components(G)

    author_component_lst = list()
    for ix_component, component in enumerate(components):
        for author in component:
            author_component_lst.append(
                dict(AUTHOR_SID=author, CONNECTED_COMPONENT_SID=ix_component)
            )

    # Create a new table with the author SID and the connected component it belongs to
    author_component_df = pd.DataFrame(author_component_lst)

    offload_df_to_bigquery(
        df=author_component_df,
        table_id=task_config["target_table_id"],
        client=bq_client,
        mode=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
