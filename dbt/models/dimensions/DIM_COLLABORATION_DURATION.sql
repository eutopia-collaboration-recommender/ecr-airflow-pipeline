SELECT AIRFLOW.UDF_MD5_HASH(['New partnership']) AS COLLABORATION_DURATION_SID,
       'New partnership'                         AS COLLABORATION_DURATION_NAME,
       1                                         AS COLLABORATION_DURATION_SORT_INDEX
UNION ALL
SELECT AIRFLOW.UDF_MD5_HASH(['Extended collaboration']) AS COLLABORATION_DURATION_SID,
       'Extended collaboration'                         AS COLLABORATION_DURATION_NAME,
       2                                                AS COLLABORATION_DURATION_SORT_INDEX
UNION ALL
SELECT AIRFLOW.UDF_MD5_HASH(['Long-term collaboration']) AS COLLABORATION_DURATION_SID,
       'Long-term collaboration'                         AS COLLABORATION_DURATION_NAME,
       3                                                 AS COLLABORATION_DURATION_SORT_INDEX