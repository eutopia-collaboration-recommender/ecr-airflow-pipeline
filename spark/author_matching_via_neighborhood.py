from pyspark.sql import SparkSession
from pyspark.sql.functions import col, levenshtein


# Define the job configuration
JOB_CONFIG = {
    "APP_NAME": "AuthorMatchingViaNeighborhood",
    "DATASET": "PROD",
    "TARGET_TABLE": f"AUTHOR_MATCH_CANDIDATE_PAIR_BY_NEIGHBORHOOD",
    "TEMPORARY_GCS_BUCKET": "ecr-dataproc-cluster-1",
    "SAVE_MODE": "append",
}

# Initialize Spark session
spark = SparkSession.builder.appName("AuthorMatchingViaNeighborhood").getOrCreate()


# Load initial data from BigQuery with sampling
author_neighborhood_df = (
    spark.read.format("bigquery")
    .option("table", "PROD.STG_AUTHOR_NEIGHBORHOOD")
    .load()
    .select(
        col("INITIAL_AUTHOR_SID"),
        col("NEIGHBOR_AUTHOR_SID"),
        col("INITIAL_AUTHOR_FULL_NAME"),
        col("NEIGHBOR_AUTHOR_FULL_NAME"),
    )
)

# Find neighbors of neighbors and their names
neighbors_of_neighbors_df = (
    author_neighborhood_df.alias("na1")
    .join(
        author_neighborhood_df.alias("na2"),
        # We connect neighborhoods of two different authors,
        # where we exclude all cases where neighbor of neighbors are the same
        (col("na1.NEIGHBOR_AUTHOR_SID") == col("na2.INITIAL_AUTHOR_SID"))
        & (col("na1.INITIAL_AUTHOR_SID") != col("na2.NEIGHBOR_AUTHOR_SID")),
        "inner",
    )
    .select(
        col("na1.INITIAL_AUTHOR_SID").alias("INITIAL_AUTHOR_SID"),
        col("na2.NEIGHBOR_AUTHOR_SID").alias("NEIGHBOR_OF_NEIGHBOR_SID"),
        col("na1.INITIAL_AUTHOR_FULL_NAME").alias("INITIAL_AUTHOR_FULL_NAME"),
        col("na2.NEIGHBOR_AUTHOR_FULL_NAME").alias("NEIGHBOR_OF_NEIGHBOR_FULL_NAME"),
    )
    .repartition(32)
)

distinct_neighbors_of_neighbors_df = neighbors_of_neighbors_df.distinct()

# Calculate Levenshtein distance using PySpark function
result_df = distinct_neighbors_of_neighbors_df.withColumn(
    "LEVENSHTEIN_DISTANCE",
    levenshtein(col("INITIAL_AUTHOR_FULL_NAME"), col("NEIGHBOR_OF_NEIGHBOR_FULL_NAME")),
)

# Filter results
author_match_df = result_df.filter(col("LEVENSHTEIN_DISTANCE") < 5)

# Save to BigQuery table as the final result, which is saved to the dataset
# and table specified in the JOB_CONFIG dictionary
author_match_df.write.format("bigquery").mode(JOB_CONFIG["SAVE_MODE"]).option(
    "table", f'{JOB_CONFIG["DATASET"]}.{JOB_CONFIG["TARGET_TABLE"]}'
).option("temporaryGcsBucket", JOB_CONFIG["TEMPORARY_GCS_BUCKET"]).save()
