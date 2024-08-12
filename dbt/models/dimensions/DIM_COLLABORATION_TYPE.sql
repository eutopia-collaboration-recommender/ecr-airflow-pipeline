SELECT AIRFLOW.UDF_MD5_HASH(['Sole-author publication']) AS COLLABORATION_TYPE_SID,
       'Sole-author publication'                         AS COLLABORATION_TYPE_NAME,
       'Sole-author'                                     AS COLLABORATION_TYPE_CATEGORY,
       1                                                 AS COLLABORATION_TYPE_SORT_INDEX
UNION ALL
SELECT AIRFLOW.UDF_MD5_HASH(['Internal collaboration']) AS COLLABORATION_TYPE_SID,
       'Internal collaboration'                         AS COLLABORATION_TYPE_NAME,
       'Collaboration'                                  AS COLLABORATION_TYPE_CATEGORY,
       2                                                AS COLLABORATION_TYPE_SORT_INDEX
UNION ALL
SELECT AIRFLOW.UDF_MD5_HASH(['External collaboration']) AS COLLABORATION_TYPE_SID,
       'External collaboration'                         AS COLLABORATION_TYPE_NAME,
       'Collaboration'                                  AS COLLABORATION_TYPE_CATEGORY,
       3                                                AS COLLABORATION_TYPE_SORT_INDEX