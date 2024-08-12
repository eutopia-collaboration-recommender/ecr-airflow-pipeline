CREATE OR REPLACE MODEL `collaboration-recommender`.ANALYTICS.MODEL_TEXT_GENERATION_PALM_BISON_2
REMOTE WITH CONNECTION `collaboration-recommender.us.ai-connection`
OPTIONS (ENDPOINT = 'text-bison@002');
