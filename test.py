from google.cloud import bigquery

# Initialize a BigQuery client
client = bigquery.Client(project="collaboration-recommender")

# Define the SQL query to call the stored procedure
query = """
CALL ANALYTICS.SP_FILL_TEXT_EMBEDDING_ARTICLE_GECKO()
"""

# Run the query
query_job = client.query(query)

# Fetch the result (the procedure will have completed at this point)
new_rows = query_job.result().to_dataframe()["NEW_ROWS"][0]

# Print the output
print(f"Number of new rows inserted: {new_rows}")
