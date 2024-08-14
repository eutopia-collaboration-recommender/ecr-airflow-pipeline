from collections import Counter

import numpy as np
import pandas as pd
from google.cloud import bigquery


def cosine_similarity(vector: np.ndarray, matrix: np.ndarray) -> float:
    """
    Calculate the cosine similarity between a vector and a matrix.
    :param vector: Numpy vector
    :param matrix: Numpy matrix
    :return: Cosine similarity between the vector and the matrix
    """

    # Calculate the dot product between the vector and each row of the matrix
    dot_product = np.dot(matrix, vector.T).flatten()

    # Calculate the norm of the matrix and the vector
    matrix_norms = np.linalg.norm(matrix, axis=1)
    vector_norm = np.linalg.norm(vector)

    # Calculate the cosine similarity
    cosine_similarities = dot_product / (matrix_norms * vector_norm)

    return cosine_similarities


def query_research_area_embeddings(
    bq_client: bigquery.Client, source_table_id: str
) -> pd.DataFrame:
    """
    Query the embeddings for the top N articles for all research areas.
    :param bq_client: The BigQuery client.
    :param source_table_id: Source table ID.
    :return: The research area embeddings.
    """
    return (
        bq_client.query(
            f"""
            SELECT  RESEARCH_AREA_CODE,
                    EMBEDDING_TENSOR_DATA
            FROM `{source_table_id}`
        """
        )
        .result()
        .to_dataframe()
    )


def query_article_count_for_missing_research_area(
    bq_client: bigquery.Client, source_table_id: str, target_table_id: str
) -> int:
    """
    Query the number of articles for which the research area is missing.
    :param bq_client: The BigQuery client.
    :param source_table_id: Source table ID.
    :param target_table_id: Target table ID.
    :return: The number of articles for which the research area is missing.
    """
    return int(
        bq_client.query(
            f"""SELECT COUNT(1)
                FROM `{source_table_id}` A
                LEFT JOIN `{target_table_id}` T USING (DOI)
                WHERE T.DOI IS NULL"""
        )
        .result()
        .to_dataframe()
        .values[0][0]
    )


def query_article_embeddings_batch(
    bq_client: bigquery.Client,
    source_table_id: str,
    target_table_id: str,
    batch_size: int,
) -> pd.DataFrame:
    """
    Query the number of articles for which the research area is missing.
    :param batch_size: Batch size.
    :param bq_client: The BigQuery client.
    :param source_table_id: Source table ID.
    :param target_table_id: Target table ID.
    :return: The number of articles for which the research area is missing.
    """
    return (
        bq_client.query(
            f"""
        SELECT  A.DOI,
                A.EMBEDDING_TENSOR_DATA
        FROM `{source_table_id}` A
        LEFT JOIN `{target_table_id}` T USING (DOI)
        WHERE T.DOI IS NULL
        LIMIT {batch_size}
        """
        )
        .result()
        .to_dataframe()
    )


def classify_research_area_for_article_batch(
    research_area_embeddings_df: pd.DataFrame,
    research_area_embedding_values: np.ndarray,
    batch_size: int,
    bq_client: bigquery.Client,
    source_table_id: str,
    target_table_id: str,
) -> pd.DataFrame:
    """
    Process the article embeddings and calculate the cosine similarity between the research areas and the articles.
    :param research_area_embedding_values: The research area embeddings as a list of numpy arrays.
    :param research_area_embeddings_df: The research area embeddings.
    :param bq_client: The BigQuery client.
    :param target_table_id: The target table ID.
    :param batch_size: The batch size.
    :param source_table_id: The source table ID.
    :return: The article-research area mapping.
    """

    # Fetch all article embeddings that are not yet in the target table
    article_embeddings_df = query_article_embeddings_batch(
        bq_client=bq_client,
        source_table_id=source_table_id,
        target_table_id=target_table_id,
        batch_size=batch_size,
    )

    # Initialize the article-research area mapping
    article_research_area_mapping = []

    for i, article_embedding in enumerate(
        article_embeddings_df["EMBEDDING_TENSOR_DATA"].values.tolist()
    ):
        # Calculate the cosine similarity between the research areas and the article
        similarities = cosine_similarity(
            article_embedding, research_area_embedding_values
        )

        # Find the indices of the top 5 most similar research areas
        top_5_research_area_indices = similarities.argsort()[-5:][::-1]

        # Get the corresponding research area codes
        top_5_research_area_codes = research_area_embeddings_df[
            "RESEARCH_AREA_CODE"
        ].iloc[top_5_research_area_indices]

        # Determine the most frequent research area code among the top 5
        most_common_research_area_code = Counter(top_5_research_area_codes).most_common(
            1
        )[0][0]

        # Add the article-research area mapping
        article_research_area = {
            "DOI": article_embeddings_df["DOI"].iloc[i],
            "RESEARCH_AREA_CODE": most_common_research_area_code,
        }

        # Append to the article-research area mapping list
        article_research_area_mapping.append(article_research_area)

    return pd.DataFrame(article_research_area_mapping)
