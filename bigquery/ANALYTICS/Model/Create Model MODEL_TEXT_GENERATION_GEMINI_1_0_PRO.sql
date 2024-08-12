CREATE OR REPLACE MODEL `collaboration-recommender`.ANALYTICS.MODEL_TEXT_GENERATION_GEMINI_1_0_PRO
REMOTE WITH CONNECTION `collaboration-recommender.us.ai-connection`
OPTIONS (ENDPOINT = 'gemini-1.0-pro');