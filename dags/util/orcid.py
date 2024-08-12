import json
import time

import requests
from google.cloud import bigquery, secretmanager
from requests.exceptions import ChunkedEncodingError
from util.common import get_secret


def query_dois(
    bq_client: bigquery.Client, source_table_id: str, target_table_id: str
) -> list:
    """
    Query DOIs to be ingested (DOIs that are not in the target table)
    :param bq_client: BigQuery client
    :param source_table_id: Source table ID in BigQuery
    :param target_table_id: Target table ID in BigQuery
    :return: List of DOIs
    """
    query = f"""
    SELECT DISTINCT S.DOI
    FROM {source_table_id} S
        LEFT JOIN {target_table_id} T
    ON S.DOI = T.DOI
    WHERE T.DOI IS NULL 
    """

    # Execute the query and covert the result to a list
    df = bq_client.query(query).result().to_dataframe()
    return df["DOI"].tolist()


def query_orcid_ids(
    source_table_id: str, target_table_id: str, bq_client: bigquery.Client
) -> list:
    """
    Query ORCID IDs to be ingested (ORCID IDs that are queried by DOIs and are not in the target table)
    :param source_table_id: Source table ID in BigQuery
    :param target_table_id: Target table ID in BigQuery
    :param bq_client: BigQuery client
    :return: List of ORCID IDs
    """
    query = f"""
    SELECT DISTINCT JSON_EXTRACT_SCALAR(ID) AS ORCID_ID
    FROM {source_table_id} C,
         UNNEST(JSON_EXTRACT_ARRAY(ORCID_ID_ARRAY, '$.')) AS ID
             LEFT JOIN {target_table_id} A
                       ON JSON_EXTRACT_SCALAR(ID) = A.ORCID_ID
    WHERE A.ORCID_ID IS NULL;
    """

    # Execute the query and covert the result to a list
    df = bq_client.query(query).result().to_dataframe()
    return df["ORCID_ID"].tolist()


def request_orcid(url: str, access_token: str, request_limit_queue: list) -> dict:
    """
    Request call on ORCID API
    :param url: URL
    :param access_token: ORCID access token
    :param request_limit_queue: Request limit queue to avoid exceeding the limit on ORCID, which is 24 requests per second
    :return: JSON response
    """
    # Define headers
    headers_record: dict = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }
    # Pop the first element from the queue (if queue is full)
    if len(request_limit_queue) >= 24:
        # Delete the first element
        request_limit_queue.pop(0)

    # Put the current time in the queue
    request_limit_queue.append(time.time())
    try:
        # Fetch data from ORCID API
        response = requests.get(url=url, headers=headers_record)
    except ChunkedEncodingError:
        print(f"ChunkedEncodingError occurred. URL returned an error: {url}.")
        return dict(error="ChunkedEncodingError")

    # Check if the number of requests is at the limit and check if the first request that went into queue happened
    # less than a second ago
    if (
        len(request_limit_queue) >= 24
        and request_limit_queue[0] > request_limit_queue[-1] - 1
    ):
        # Difference between the last and first request
        diff = request_limit_queue[-1] - request_limit_queue[0]
        # Sleep for the difference to avoid exceeding the limit
        time.sleep(1 - diff)

    return response.json()


def fetch_orcid_by_doi(doi: str, access_token: str, request_limit_queue: list) -> dict:
    """
    Fetch the ORCID IDs by DOI
    :param doi: DOI
    :param access_token: ORCID access token
    :param request_limit_queue: Request limit queue to avoid exceeding the limit on ORCID, which is 24 requests per second
    :return: Record of DOI and corresponding ORCID IDs
    """
    # Fetch the ORCID IDs by DOI
    response = request_orcid(
        url=f"https://pub.orcid.org/v3.0/search?q=doi-self:{doi}",
        access_token=access_token,
        request_limit_queue=request_limit_queue,
    )

    # Check if the response is empty
    if "num-found" not in response.keys() or response["num-found"] == 0:
        list_of_orcid_ids = []
    else:
        list_of_orcid_ids = [
            res["orcid-identifier"]["path"] for res in response["result"]
        ]

    # Define the record
    record = dict(DOI=doi, ORCID_ID_ARRAY=json.dumps(list_of_orcid_ids))
    # Return the record of DOI and corresponding ORCID IDs
    return record


def fetch_orcid_id(orcid_id: str, access_token: str, request_limit_queue: list) -> dict:
    """
    Fetch the ORCID record by ORCID ID
    :param orcid_id: ORCID identifier
    :param access_token: ORCID access token
    :param request_limit_queue: Request limit queue to avoid exceeding the limit on ORCID, which is 24 requests per second
    :return: ORCID record
    """
    # Fetch the ORCID record
    response = request_orcid(
        url=f"https://pub.orcid.org/v3.0/{orcid_id}/record",
        access_token=access_token,
        request_limit_queue=request_limit_queue,
    )

    # Define the record
    record = dict(ORCID_ID=orcid_id, JSON=json.dumps(response))

    # Return the record from ORCID
    return record


def fetch_access_token(project_id) -> str:
    """
    Fetch the access token from ORCID (based on the client ID and client secret that are stored in Secret Manager)
    :param project_id: Project ID
    :return: Access token
    """
    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()
    client_id = get_secret(
        project_id=project_id, name="ORCID_CLIENT_ID", version="1", client=client
    )
    client_secret = get_secret(
        project_id=project_id, name="ORCID_CLIENT_SECRET", version="1", client=client
    )

    # Define headers
    headers_token: dict = {"Accept": "application/json"}

    # Define URL
    url_token: str = "https://orcid.org/oauth/token"

    # Define data
    data_token: dict = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
        "scope": "/read-public",
    }

    # POST request
    response_token: requests.Response = requests.post(
        url=url_token, headers=headers_token, data=data_token
    )
    # Get access from response
    access_token = response_token.json().get("access_token")

    # Return the access token
    return access_token
