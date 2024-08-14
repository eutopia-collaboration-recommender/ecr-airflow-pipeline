WITH SRC_AUTHOR_MATCH_CONNECTED_COMPONENT_BY_NEIGHBORHOOD AS (SELECT AUTHOR_SID,
                                                                     CONNECTED_COMPONENT_SID
                                                              FROM {{ source('ANALYTICS', 'AUTHOR_MATCH_CONNECTED_COMPONENT_BY_NEIGHBORHOOD') }})
   , REF_STG_CROSSREF_AUTHOR AS (SELECT *
                                 FROM {{ ref('STG_CROSSREF_AUTHOR') }})
   , REF_ER_AUTHOR_MATCH_BY_ORCID AS (SELECT *
                                      FROM {{ ref('ER_AUTHOR_MATCH_BY_ORCID') }})
   , AUTHOR_MATCH_BASE AS (SELECT CC.AUTHOR_SID,
                                  CC.CONNECTED_COMPONENT_SID,
                                  A.AUTHOR_FULL_NAME,
                                  COALESCE(AMO.ORCID_ID, A.AUTHOR_ORCID_ID) AS AUTHOR_ORCID_ID,
                           FROM SRC_AUTHOR_MATCH_CONNECTED_COMPONENT_BY_NEIGHBORHOOD CC
                                    INNER JOIN REF_STG_CROSSREF_AUTHOR A
                                               ON CC.AUTHOR_SID = A.AUTHOR_SID
                                    LEFT JOIN REF_ER_AUTHOR_MATCH_BY_ORCID AMO
                                              ON A.AUTHOR_SID = AMO.OLD_AUTHOR_SID
                           GROUP BY ALL)
   , AUTHOR_COUNT_BY_ORCID_ID AS (SELECT CONNECTED_COMPONENT_SID,
                                         AUTHOR_ORCID_ID,
                                         COUNT(DISTINCT AUTHOR_SID) AS ORCID_ID_AUTHOR_COUNT
                                  FROM AUTHOR_MATCH_BASE
                                  GROUP BY CONNECTED_COMPONENT_SID, AUTHOR_ORCID_ID)
   , AUTHOR_MATCH_WITH_ORCID_ID_FREQUENCY
    AS (SELECT B.AUTHOR_SID               AS OLD_AUTHOR_SID,
               CASE
                   WHEN ACOI.AUTHOR_ORCID_ID IS NOT NULL THEN
                       AIRFLOW.UDF_MD5_HASH([ACOI.AUTHOR_ORCID_ID])
                   ELSE
                       AIRFLOW.UDF_MD5_HASH([CAST(B.CONNECTED_COMPONENT_SID AS STRING)])
                   END                    AS NEW_AUTHOR_SID,
               B.AUTHOR_ORCID_ID,
               B.AUTHOR_FULL_NAME,
               LENGTH(B.AUTHOR_FULL_NAME) AS AUTHOR_FULL_NAME_LENGTH,
               ACOI.ORCID_ID_AUTHOR_COUNT
        FROM AUTHOR_MATCH_BASE B
                 LEFT JOIN AUTHOR_COUNT_BY_ORCID_ID ACOI
                           ON B.CONNECTED_COMPONENT_SID = ACOI.CONNECTED_COMPONENT_SID
                               AND B.AUTHOR_ORCID_ID = ACOI.AUTHOR_ORCID_ID)
SELECT DISTINCT OLD_AUTHOR_SID,
                NEW_AUTHOR_SID,
                AUTHOR_ORCID_ID,
                AUTHOR_FULL_NAME,
                AUTHOR_FULL_NAME_LENGTH,
                ORCID_ID_AUTHOR_COUNT
FROM AUTHOR_MATCH_WITH_ORCID_ID_FREQUENCY
-- If there are multiple new author SIDs for a single old author SID, we take:
--  - The one with the most frequent ORCID ID within the neighborhood
--  - The one with the highest ORCID ID (majority gets filtered before, this is just to ensure uniqueness)
QUALIFY RANK() OVER (PARTITION BY OLD_AUTHOR_SID ORDER BY ORCID_ID_AUTHOR_COUNT DESC, AUTHOR_ORCID_ID DESC ) = 1