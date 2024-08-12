-- Create a connection to the Vertex AI in the project: https://cloud.google.com/bigquery/docs/create-cloud-resource-connection#console

--Create a remote model that connects to the AI Platform model
CREATE
MODEL `collaboration-recommender`.ANALYTICS.MODEL_TEXT_EMBEDDING_GECKO
REMOTE WITH CONNECTION `collaboration-recommender.us.ai-connection`
OPTIONS(remote_service_type = 'CLOUD_AI_TEXT_EMBEDDING_MODEL_V1');
