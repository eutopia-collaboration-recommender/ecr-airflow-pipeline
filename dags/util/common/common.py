import logging

import backoff
import pandas as pd
import requests
from google.auth import default
from google.cloud import bigquery, secretmanager
from googleapiclient.discovery import build

MAILTO_EMAIL = "luka.zontar.consulting@gmail.com"


# ------------------------------ make_request ------------------------------
# Exponential backoff on HTTP errors (status code >= 500) and RequestException


@backoff.on_exception(
    backoff.expo,
    (requests.exceptions.RequestException, requests.exceptions.HTTPError),
    max_tries=8,
    giveup=lambda e: e.response is not None and e.response.status_code < 500,
)
def make_request(url: str, params: dict) -> dict:
    """
    Make a request to the given URL with the given parameters
    :param url: URL to make the request to.
    :param params: Parameters to include in the request.
    :return: JSON response from the request.
    """
    response = requests.get(url, params=params)
    response.raise_for_status()  # Raise an exception for HTTP errors

    return response.json()


# ------------------------------ offload_df_to_bigquery_via_stage -----------------------
def offload_df_to_bigquery_via_stage(
    df: pd.DataFrame,
    table_id: str,
    stage_table_id: str,
    client: bigquery.Client,
    data_schema: list = None,
    verbose: bool = True,
    mode: bigquery.WriteDisposition = bigquery.WriteDisposition.WRITE_APPEND,
) -> None:
    """
    Offloads a batch of records to BigQuery via stage table.
    :param mode: Write disposition mode.
    :param data_schema: The schema of the data to offload.
    :param client: BigQuery client.
    :param df: The DataFrame to offload.
    :param table_id: The ID of the destination table in BigQuery.
    :param stage_table_id: The ID of the stage table in BigQuery.
    :param verbose: If True, print an info message on success.
    """

    # Offload the DataFrame to the stage table
    offload_df_to_bigquery(
        df=df,
        table_id=stage_table_id,
        client=client,
        data_schema=data_schema,
        verbose=False,
        mode=mode,
    )

    # Merge the stage table with the target table
    query = f"""
    INSERT INTO {table_id}
    SELECT * FROM {stage_table_id}
    """

    # Execute the query
    client.query(query).result()

    # Truncate the stage table
    query = f"""
    TRUNCATE TABLE {stage_table_id}
    """

    # Execute the query
    client.query(query).result()
    # Print info message on success
    if verbose:
        logging.info(
            f"Offloaded a batch of {len(df)} items to final table {table_id} via stage table {stage_table_id}."
        )


# ------------------------------ offload_df_to_bigquery ---------------------------------
def offload_df_to_bigquery(
    df: pd.DataFrame,
    table_id: str,
    client: bigquery.Client,
    data_schema: list = None,
    verbose: bool = True,
    mode: bigquery.WriteDisposition = bigquery.WriteDisposition.WRITE_APPEND,
) -> None:
    """
    Offloads a batch of records to BigQuery.
    :param mode: Write disposition mode.
    :param data_schema: The schema of the data to offload.
    :param client: BigQuery client.
    :param df: The DataFrame to offload.
    :param table_id: The ID of the destination table in BigQuery.
    :param verbose: If True, print an info message on success.
    """

    # Configure the load job to append data to an existing table
    if data_schema is not None:
        job_config = bigquery.LoadJobConfig(write_disposition=mode, schema=data_schema)
    else:
        job_config = bigquery.LoadJobConfig(
            write_disposition=mode,
        )

    # Offload the DataFrame to BigQuery, appending it to the existing table
    job = client.load_table_from_dataframe(
        dataframe=df, destination=table_id, job_config=job_config
    )

    # Wait for the load job to complete
    job.result()

    # Print info message on success
    if verbose:
        logging.info(f"Offloaded a batch of {len(df)} items to BigQuery.")


def get_secret(
    project_id: str,
    name: str,
    client: secretmanager.SecretManagerServiceClient,
    version: str = "1",
) -> str:
    """
    Fetch the secret from Secret Manager
    :rtype: object
    :param project_id: Project ID
    :param name: Secret name
    :param version: Secret version
    :param client: Secret Manager client
    :return: Secret
    """
    return client.access_secret_version(
        name=f"projects/{project_id}/secrets/{name}/versions/{version}"
    ).payload.data.decode("UTF-8")


def get_sheet_data(spreadsheet_id: str, sheet_id: str, data_range: str) -> pd.DataFrame:
    """
    Get data from a Google Sheet.
    :param spreadsheet_id: The ID of the Google Sheet
    :param sheet_id: The name of the sheet
    :param data_range: The range of data to retrieve
    :return: DataFrame with the data
    """
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    # Get credentials and project from the environment
    creds, project = default(scopes=scopes)
    # Build the service to interact with Google Sheets
    service = build("sheets", "v4", credentials=creds)

    # Define the range of data to retrieve (e.g., entire sheet)
    range_name = f"{sheet_id}!{data_range}"

    # Call the Sheets API to fetch the data
    result = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range_name)
        .execute()
    )

    # Extract the data
    values = result.get("values")
    columns = values.pop(0)
    return pd.DataFrame(values, columns=columns)
