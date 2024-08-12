WITH SOURCE_TABLE
    AS (SELECT ORCID_ID,
               JSON
        FROM {{ source('AIRFLOW', 'ORCID_HISTORIC_AUTHOR') }})
   , ARTICLE_JSON
    AS (SELECT JSON_EXTRACT_SCALAR(JSON, '$.record:record.common:orcid-identifier.common:path') AS ORCID_ID
             , IFNULL(WORK_SUMMARY, JSON_EXTRACT(WORK, '$.work:work-summary'))                  AS WORK_SUMMARY_JSON
             , IFNULL(EXTERNAL_ID,
                      JSON_EXTRACT(WORK, '$.common:external-ids.common:external-id'))           AS EXTERNAL_ID_JSON
        FROM SOURCE_TABLE
                 LEFT JOIN
             UNNEST(
                     JSON_EXTRACT_ARRAY(
                             JSON_EXTRACT(JSON,
                                          '$.record:record.activities:activities-summary.activities:works.activities:group')
                     )
             ) WORK ON TRUE
                 LEFT JOIN UNNEST(
                JSON_EXTRACT_ARRAY(JSON_EXTRACT(work, '$.common:external-ids.common:external-id'))
                           ) EXTERNAL_ID ON TRUE
                 LEFT JOIN UNNEST(
                JSON_EXTRACT_ARRAY(JSON_EXTRACT(work, '$.work:work-summary'))
                           ) WORK_SUMMARY ON TRUE)
   , SUMMARY_BY_ARTICLE
    AS (SELECT ORCID_ID                                                                                               AS MEMBER_ORCID_ID
             , CAST(CAST(JSON_EXTRACT_SCALAR(WORK_SUMMARY_JSON, '$.common:last-modified-date') AS TIMESTAMP) AS DATE) AS ARTICLE_LAST_MODIFIED_DT
             , LOWER(JSON_EXTRACT_SCALAR(EXTERNAL_ID_JSON,
                                         '$.common:external-id-value'))                                               AS ARTICLE_DOI
             , IFNULL(JSON_EXTRACT_SCALAR(WORK_SUMMARY_JSON, '$.work:title.translated-title'),
                      JSON_EXTRACT_SCALAR(WORK_SUMMARY_JSON, '$.work:title.common:title'))                            AS ARTICLE_TITLE
             , JSON_EXTRACT_SCALAR(WORK_SUMMARY_JSON,
                                   '$.common:publication-date.common:year')                                           AS ARTICLE_PUBLICATION_YEAR
             , JSON_EXTRACT_SCALAR(WORK_SUMMARY_JSON,
                                   '$.common:publication-date.common:month')                                          AS ARTICLE_PUBLICATION_MONTH
             , JSON_EXTRACT_SCALAR(WORK_SUMMARY_JSON,
                                   '$.common:publication-date.common:day')                                            AS ARTICLE_PUBLICATION_DAY
             , JSON_EXTRACT_SCALAR(WORK_SUMMARY_JSON,
                                   '$.work:type')                                                                     AS ARTICLE_TYPE
             , JSON_EXTRACT_SCALAR(WORK_SUMMARY_JSON,
                                   '$.common:url')                                                                    AS ARTICLE_URL
             , JSON_EXTRACT_SCALAR(WORK_SUMMARY_JSON,
                                   '$.work:journal-title')                                                            AS ARTICLE_JOURNAL_TITLE
        FROM ARTICLE_JSON
        WHERE JSON_EXTRACT_SCALAR(EXTERNAL_ID_JSON, '$.common:external-id-type') = 'doi')
SELECT *
FROM SUMMARY_BY_ARTICLE