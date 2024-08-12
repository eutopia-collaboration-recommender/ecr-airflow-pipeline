from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    countDistinct,
    levenshtein,
    lower,
    min as spark_min,
    rank,
)
from pyspark.sql.window import Window


# Define the job configuration
JOB_CONFIG = {
    "APP_NAME": "AuthorMatchingViaORCID",
    "SOURCE_DATASET": "PROD",
    "TARGET_DATASET": "DATALAKE",
    "TARGET_TABLE": f"AUTHOR_MATCH_CANDIDATE_PAIR_BY_ORCID",
    "TEMPORARY_GCS_BUCKET": "ecr-dataproc-cluster-1",
    "SAVE_MODE": "append",
}

# Initialize Spark session
spark = SparkSession.builder.appName(JOB_CONFIG["APP_NAME"]).getOrCreate()

# Load tables from the PROD database into Spark DataFrames
stg_crossref_collaboration_df = (
    spark.read.format("bigquery")
    .option(
        "table", f"{JOB_CONFIG['SOURCE_DATASET']}.STG_CROSSREF_HISTORIC_ARTICLE_FINAL"
    )
    .load()
    .select(
        "ARTICLE_DOI",
        "AUTHOR_FULL_NAME",
        "AUTHOR_ORCID_ID",
        "AUTHOR_SID",
        "IS_AUTHOR_ORCID_AUTHENTICATED",
    )
    .filter((col("AUTHOR_FULL_NAME").isNotNull()) & (col("AUTHOR_FULL_NAME") != ""))
    .distinct()
)
stg_orcid_article_df = (
    spark.read.format("bigquery")
    .option("table", f"{JOB_CONFIG['SOURCE_DATASET']}.STG_ORCID_ARTICLE")
    .load()
    .select("MEMBER_ORCID_ID", "ARTICLE_DOI")
    .distinct()
)
stg_orcid_author_df = (
    spark.read.format("bigquery")
    .option("table", f"{JOB_CONFIG['SOURCE_DATASET']}.STG_ORCID_AUTHOR")
    .load()
    .select("MEMBER_ORCID_ID", "FULL_NAME")
    .distinct()
)

# Fetch author specifications from ORCID data and calculate the Levenshtein distance between the author names
# from the Crossref and ORCID data
levenshtein_distance_by_author_pair_df = (
    stg_crossref_collaboration_df.alias("F")
    .join(
        stg_orcid_article_df.alias("A"),
        col("A.ARTICLE_DOI") == col("F.ARTICLE_DOI"),
        "inner",
    )
    .join(
        stg_orcid_author_df.alias("AU"),
        col("A.MEMBER_ORCID_ID") == col("AU.MEMBER_ORCID_ID"),
        "inner",
    )
    .select(
        col("F.ARTICLE_DOI"),
        col("F.AUTHOR_SID").alias("CROSSREF_AUTHOR_SID"),
        col("F.AUTHOR_ORCID_ID").alias("CROSSREF_ORCID_ID"),
        lower(col("F.AUTHOR_FULL_NAME")).alias("CROSSREF_LOWER_AUTHOR_FULL_NAME"),
        col("AU.MEMBER_ORCID_ID").alias("ORCID_ID"),
        lower(col("AU.FULL_NAME")).alias("ORCID_LOWER_AUTHOR_FULL_NAME"),
        levenshtein(lower(col("F.AUTHOR_FULL_NAME")), lower(col("AU.FULL_NAME"))).alias(
            "LEVENSHTEIN_DISTANCE"
        ),
        col("F.IS_AUTHOR_ORCID_AUTHENTICATED").alias(
            "IS_CROSSREF_AUTHOR_ORCID_AUTHENTICATED"
        ),
    )
)

# Rank the author pairs by Levenshtein distance within each article and select the top-ranked pairs
# both by ORCID and Crossref author IDs
window_crossref_df = Window.partitionBy("ARTICLE_DOI", "CROSSREF_AUTHOR_SID").orderBy(
    "LEVENSHTEIN_DISTANCE"
)
window_orcid_df = Window.partitionBy("ARTICLE_DOI", "ORCID_ID").orderBy(
    "LEVENSHTEIN_DISTANCE"
)

ranked_author_pairs_df = levenshtein_distance_by_author_pair_df.withColumn(
    "RANK_BY_CROSSREF_AUTHOR", rank().over(window_crossref_df)
).withColumn("RANK_BY_ORCID_AUTHOR", rank().over(window_orcid_df))

# Select the top-ranked author pairs by Levenshtein distance within each article
author_match_top_rank_by_distance_within_article_df = ranked_author_pairs_df.filter(
    (col("RANK_BY_CROSSREF_AUTHOR") == 1) & (col("RANK_BY_ORCID_AUTHOR") == 1)
)

# Calculate the minimum Levenshtein distance for each Crossref author ID
min_levenshtein_by_crossref_author_df = (
    author_match_top_rank_by_distance_within_article_df.groupBy(
        "CROSSREF_AUTHOR_SID"
    ).agg(spark_min("LEVENSHTEIN_DISTANCE").alias("MIN_LEVENSHTEIN_DISTANCE"))
)

# Count the number of articles associated with each author pair
article_count_by_crossref_author_df = (
    author_match_top_rank_by_distance_within_article_df.groupBy(
        "CROSSREF_AUTHOR_SID", "ORCID_ID"
    ).agg(countDistinct("ARTICLE_DOI").alias("ARTICLE_COUNT"))
)

# Join the tables to get the final result of author match candidates
author_match_df = (
    author_match_top_rank_by_distance_within_article_df.alias("A")
    .join(
        min_levenshtein_by_crossref_author_df.alias("MIN"),
        col("A.CROSSREF_AUTHOR_SID") == col("MIN.CROSSREF_AUTHOR_SID"),
        "left",
    )
    .join(
        article_count_by_crossref_author_df.alias("CNT"),
        (col("A.CROSSREF_AUTHOR_SID") == col("CNT.CROSSREF_AUTHOR_SID"))
        & (col("A.ORCID_ID") == col("CNT.ORCID_ID")),
        "left",
    )
    .filter(col("A.LEVENSHTEIN_DISTANCE") == col("MIN.MIN_LEVENSHTEIN_DISTANCE"))
    .select(
        "A.ARTICLE_DOI",
        "A.CROSSREF_AUTHOR_SID",
        "A.CROSSREF_ORCID_ID",
        "A.ORCID_ID",
        "A.LEVENSHTEIN_DISTANCE",
        "CNT.ARTICLE_COUNT",
    )
    .distinct()
)

# Save to BigQuery table as the final result, which is saved to the dataset
# and table specified in the JOB_CONFIG dictionary
author_match_df.write.format("bigquery").mode(JOB_CONFIG["SAVE_MODE"]).option(
    "table", f'{JOB_CONFIG["TARGET_DATASET"]}.{JOB_CONFIG["TARGET_TABLE"]}'
).option("temporaryGcsBucket", JOB_CONFIG["TEMPORARY_GCS_BUCKET"]).save()
