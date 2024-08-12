WITH REF_STG_CROSSREF_HISTORIC_ARTICLE AS (SELECT *
                                           FROM {{ ref('STG_CROSSREF_HISTORIC_ARTICLE_UNNESTED') }}),
     PARSED AS (SELECT LOWER(DOI)                                                                       AS ARTICLE_DOI
                     , AUTHOR_INDEX
                     , CONCAT(
                 COALESCE(JSON_EXTRACT_SCALAR(AUTHOR_JSON, '$.given') || ' ', ''),
                 COALESCE(JSON_EXTRACT_SCALAR(AUTHOR_JSON, '$.family'), '')
                       )                                                                                AS AUTHOR_FULL_NAME
                     -- Author ORCID
                     , CASE
                           WHEN JSON_EXTRACT(AUTHOR_JSON, '$.ORCID') IS NOT NULL THEN
                               REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(JSON_EXTRACT(AUTHOR_JSON, '$.ORCID')),
                                              '([0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{4})')
             END                                                                                        AS AUTHOR_ORCID_ID
                     , IFNULL(SAFE_CAST(JSON_EXTRACT_SCALAR(AUTHOR_JSON, '$.authenticated-orcid') AS BOOL),
                              FALSE)                                                                    AS IS_AUTHOR_ORCID_AUTHENTICATED
                     , JSON_EXTRACT(AFFILIATION_JSON, '$.name')                                         AS ORIGINAL_AFFILIATION_NAME
                     , JSON_EXTRACT(SOURCE_JSON, '$.URL')                                               AS ARTICLE_URL
                     , JSON_EXTRACT(SOURCE_JSON, '$.funder')                                            AS ARTICLE_FUNDER
                     , JSON_EXTRACT(SOURCE_JSON, '$.institution')                                       AS ARTICLE_INSTITUTION
                     , JSON_EXTRACT(SOURCE_JSON, '$.publisher')                                         AS ARTICLE_PUBLISHER
                     , JSON_EXTRACT(SOURCE_JSON, '$.title')                                             AS ARTICLE_TITLE
                     , JSON_EXTRACT(SOURCE_JSON, '$.short-title')                                       AS ARTICLE_SHORT_TITLE
                     , JSON_EXTRACT(SOURCE_JSON, '$.subtitle')                                          AS ARTICLE_SUBTITLE
                     , JSON_EXTRACT(SOURCE_JSON, '$.original-title')                                    AS ARTICLE_ORIGINAL_TITLE
                     , JSON_EXTRACT(SOURCE_JSON, '$.container-title')                                   AS ARTICLE_CONTAINER_TITLE
                     , JSON_EXTRACT(SOURCE_JSON, '$.short-container-title')                             AS ARTICLE_SHORT_CONTAINER_TITLE
                     , JSON_EXTRACT(SOURCE_JSON, '$.abstract')                                          AS ARTICLE_ABSTRACT
                     , JSON_EXTRACT(SOURCE_JSON, '$.reference')                                         AS ARTICLE_REFERENCE
                     , SAFE_CAST(JSON_EXTRACT_SCALAR(SOURCE_JSON, '$.is-referenced-by-count') AS INT64) AS IS_REFERENCED_BY_COUNT
                     -- Select minimum date from list
                     , LEAST(
                 AIRFLOW.UDF_TO_DATE_FROM_CROSSREF_DATE_PARTS(
                         PARSE_JSON(JSON_EXTRACT(SOURCE_JSON, '$.posted')))
             , AIRFLOW.UDF_TO_DATE_FROM_CROSSREF_DATE_PARTS(
                         PARSE_JSON(JSON_EXTRACT(SOURCE_JSON, '$.accepted')))
             , AIRFLOW.UDF_TO_DATE_FROM_CROSSREF_DATE_PARTS(
                         PARSE_JSON(JSON_EXTRACT(SOURCE_JSON, '$.submitted')))
             , AIRFLOW.UDF_TO_DATE_FROM_CROSSREF_DATE_PARTS(
                         PARSE_JSON(JSON_EXTRACT(SOURCE_JSON, '$.reviewed')))
             , AIRFLOW.UDF_TO_DATE_FROM_CROSSREF_DATE_PARTS(
                         PARSE_JSON(JSON_EXTRACT(SOURCE_JSON, '$.assertion')))
             , AIRFLOW.UDF_TO_DATE_FROM_CROSSREF_DATE_PARTS(
                         PARSE_JSON(JSON_EXTRACT(SOURCE_JSON, '$.published')))
             , AIRFLOW.UDF_TO_DATE_FROM_CROSSREF_DATE_PARTS(
                         PARSE_JSON(JSON_EXTRACT(SOURCE_JSON, '$.indexed'))))                           AS ARTICLE_EST_PUBLISH_DT
                     , AIRFLOW.UDF_TO_DATE_FROM_CROSSREF_DATE_PARTS(
                 PARSE_JSON(JSON_EXTRACT(SOURCE_JSON, '$.indexed')))                                    AS INDEXED_DT
                FROM REF_STG_CROSSREF_HISTORIC_ARTICLE
                WHERE SOURCE_JSON IS NOT NULL)
SELECT *
     , CONCAT(AUTHOR_FULL_NAME, '_',
              IFNULL(AUTHOR_ORCID_ID, 'n/a ')) AS AUTHOR_ID
FROM PARSED