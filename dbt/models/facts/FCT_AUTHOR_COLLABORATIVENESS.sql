WITH REF_AUTHOR_COLLABORATIVENESS AS (SELECT *
                                      FROM {{ ref('INT_AUTHOR_COLLABORATIVENESS') }})
SELECT AUTHOR_SID,
       AUTHOR_PUBLICATIONS                AS PUBLICATION_COUNT,
       AUTHOR_EXTERNAL_COLLABORATIONS     AS EXTERNAL_COLLABORATION_COUNT,
       AUTHOR_COLLABORATION_RATE,
       AUTHOR_COLLABORATION_RATE_PERCENTILE,
       IS_AUTHOR_MORE_COLLABORATIVE,
       AIRFLOW.UDF_MD5_HASH([AUTHOR_SID]) AS PK_AUTHOR_COLLABORATIVENESS
FROM REF_AUTHOR_COLLABORATIVENESS