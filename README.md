# Cloud Composer

# Dataproc

```bash
gcloud dataproc jobs submit pyspark --cluster=ecr-composer-cluster --region=us-central1 --project=collaboration-recommender --properties spark.jars.packages=com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.24.2 gs://ecr-composer-bucket-main/spark/author_matching_via_orcid.py
gcloud dataproc jobs submit pyspark --cluster=ecr-composer-cluster --region=us-central1 --project=collaboration-recommender --properties spark.jars.packages=com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.24.2 gs://ecr-composer-bucket-main/spark/author_matching_via_neighborhood.py
```

