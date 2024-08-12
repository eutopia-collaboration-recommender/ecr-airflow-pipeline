WITH SOURCE_TABLE
         AS (SELECT ORCID_ID,
                    JSON
             FROM {{ source('AIRFLOW', 'ORCID_API_AUTHOR') }})
        ,
     API_ORCID_ARTICLE_JSON
         AS (SELECT ORCID_ID                                                              AS ORCID_ID
                  , WORK                                                                  AS WORK_JSON
                  , IFNULL(WORK_SUMMARY, JSON_EXTRACT(WORK, '$.work-summary'))            AS WORK_SUMMARY_JSON
                  , IFNULL(EXTERNAL_ID, JSON_EXTRACT(WORK, '$.external-ids.external-id')) AS EXTERNAL_ID_JSON
             FROM SOURCE_TABLE
                      LEFT JOIN UNNEST(JSON_EXTRACT_ARRAY(JSON_EXTRACT(JSON, '$.activities-summary.works.group'))) WORK
                                ON TRUE
                      LEFT JOIN UNNEST(JSON_EXTRACT_ARRAY(JSON_EXTRACT(WORK, '$.external-ids.external-id'))) EXTERNAL_ID
                                ON TRUE
                      LEFT JOIN UNNEST(JSON_EXTRACT_ARRAY(JSON_EXTRACT(WORK, '$.work-summary'))) WORK_SUMMARY ON TRUE)
        ,
     API_ORCID_SUMMARY_BY_ARTICLE
         AS (SELECT ORCID_ID
                  , CAST(TIMESTAMP_MILLIS(CAST(JSON_EXTRACT_SCALAR(WORK_JSON, '$.last-modified-date.value') AS INT64)) AS DATE) AS ARTICLE_LAST_MODIFIED_DT
                  , JSON_EXTRACT_SCALAR(EXTERNAL_ID_JSON, '$.external-id-value')                                                AS ARTICLE_DOI
                  , IFNULL(JSON_EXTRACT_SCALAR(WORK_SUMMARY_JSON, '$.title.translated-title.value'),
                           JSON_EXTRACT_SCALAR(WORK_SUMMARY_JSON, '$.title.title.value'))                                       AS ARTICLE_TITLE
                  , JSON_EXTRACT_SCALAR(WORK_SUMMARY_JSON,
                                        '$.publication-date.year.value')                                                        AS ARTICLE_PUBLICATION_YEAR
                  , JSON_EXTRACT_SCALAR(WORK_SUMMARY_JSON,
                                        '$.publication-date.month.value')                                                       AS ARTICLE_PUBLICATION_MONTH
                  , JSON_EXTRACT_SCALAR(WORK_SUMMARY_JSON, '$.publication-date.day.value')                                      AS ARTICLE_PUBLICATION_DAY
                  , JSON_EXTRACT_SCALAR(WORK_SUMMARY_JSON, '$.type')                                                            AS ARTICLE_TYPE
                  , JSON_EXTRACT_SCALAR(WORK_SUMMARY_JSON, '$.url.value')                                                       AS ARTICLE_URL
                  , JSON_EXTRACT_SCALAR(WORK_SUMMARY_JSON, '$.journal-title.value')                                             AS ARTICLE_JOURNAL_TITLE
             FROM API_ORCID_ARTICLE_JSON
             WHERE JSON_EXTRACT_SCALAR(EXTERNAL_ID_JSON, '$.external-id-type') = 'doi')
SELECT *
FROM API_ORCID_SUMMARY_BY_ARTICLE