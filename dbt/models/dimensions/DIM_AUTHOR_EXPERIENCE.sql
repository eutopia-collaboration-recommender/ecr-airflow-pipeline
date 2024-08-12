SELECT AIRFLOW.UDF_MD5_HASH(['Early-career author']) AS AUTHOR_EXPERIENCE_SID,
       'Early-career author'                         AS AUTHOR_EXPERIENCE_NAME,
       1                                             AS AUTHOR_EXPERIENCE_SORT_INDEX
UNION ALL
SELECT AIRFLOW.UDF_MD5_HASH(['Mid-career author']) AS AUTHOR_EXPERIENCE_SID,
       'Mid-career author'                         AS AUTHOR_EXPERIENCE_NAME,
       2                                           AS AUTHOR_EXPERIENCE_SORT_INDEX
UNION ALL
SELECT AIRFLOW.UDF_MD5_HASH(['Experienced author']) AS AUTHOR_EXPERIENCE_SID,
       'Experienced author'                         AS AUTHOR_EXPERIENCE_NAME,
       3                                            AS AUTHOR_EXPERIENCE_SORT_INDEX