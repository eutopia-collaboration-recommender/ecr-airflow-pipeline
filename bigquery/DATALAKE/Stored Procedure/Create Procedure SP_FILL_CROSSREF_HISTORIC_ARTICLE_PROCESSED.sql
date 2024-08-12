CREATE OR REPLACE PROCEDURE AIRFLOW.SP_FILL_CROSSREF_HISTORIC_ARTICLE_PROCESSED(year INT64, month INT64)
BEGIN

    -- Delete data from target table for the given year and month
    DELETE
    FROM AIRFLOW.CROSSREF_HISTORIC_ARTICLE_PROCESSED
    WHERE EXTRACT(YEAR FROM INDEXED_DT) = year
      AND EXTRACT(MONTH FROM INDEXED_DT) = month;

    -- Create temporary table for REF_CROSSREF_HISTORIC_ARTICLE
    CREATE OR REPLACE TEMP TABLE TEMP_REF_CROSSREF_HISTORIC_ARTICLE AS
    SELECT DOI,
           JSON
    FROM AIRFLOW.CROSSREF_HISTORIC_ARTICLE
    WHERE EXTRACT(YEAR FROM
                  AIRFLOW.UDF_TO_DATE_FROM_CROSSREF_DATE_PARTS(PARSE_JSON(JSON_EXTRACT(JSON, '$.indexed')))
          ) = year
      AND EXTRACT(MONTH FROM
                  AIRFLOW.UDF_TO_DATE_FROM_CROSSREF_DATE_PARTS(PARSE_JSON(JSON_EXTRACT(JSON, '$.indexed')))
          ) = month;

    -- Create temporary table for CROSSREF_DOI_METADATA
    CREATE OR REPLACE TEMP TABLE TEMP_CROSSREF_DOI_METADATA_PARSED AS
    SELECT LOWER(DOI)                                                                AS ARTICLE_DOI
         , AUTHOR_INDEX
         , CONCAT(
            COALESCE(JSON_EXTRACT_SCALAR(AUTHOR, '$.given') || ' ', ''),
            COALESCE(JSON_EXTRACT_SCALAR(AUTHOR, '$.family'), '')
           )                                                                         AS AUTHOR_FULL_NAME
         -- Author ORCID
         , CASE
               WHEN JSON_EXTRACT(AUTHOR, '$.ORCID') IS NOT NULL THEN
                   REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(JSON_EXTRACT(AUTHOR, '$.ORCID')),
                                  '([0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{4})')
        END                                                                          AS AUTHOR_ORCID_ID
         , JSON_EXTRACT(AFFILIATION, '$.name')                                       AS ORIGINAL_AFFILIATION_NAME
         , JSON_EXTRACT(JSON, '$.URL')                                               AS ARTICLE_URL
         , JSON_EXTRACT(JSON, '$.funder')                                            AS ARTICLE_FUNDER
         , JSON_EXTRACT(JSON, '$.institution')                                       AS ARTICLE_INSTITUTION
         , JSON_EXTRACT(JSON, '$.publisher')                                         AS ARTICLE_PUBLISHER
         , JSON_EXTRACT(JSON, '$.title')                                             AS ARTICLE_TITLE
         , JSON_EXTRACT(JSON, '$.short-title')                                       AS ARTICLE_SHORT_TITLE
         , JSON_EXTRACT(JSON, '$.subtitle')                                          AS ARTICLE_SUBTITLE
         , JSON_EXTRACT(JSON, '$.original-title')                                    AS ARTICLE_ORIGINAL_TITLE
         , JSON_EXTRACT(JSON, '$.container-title')                                   AS ARTICLE_CONTAINER_TITLE
         , JSON_EXTRACT(JSON, '$.short-container-title')                             AS ARTICLE_SHORT_CONTAINER_TITLE
         , JSON_EXTRACT(JSON, '$.abstract')                                          AS ARTICLE_ABSTRACT
         , JSON_EXTRACT(JSON, '$.reference')                                         AS ARTICLE_REFERENCE
         , SAFE_CAST(JSON_EXTRACT_SCALAR(JSON, '$.is-referenced-by-count') AS INT64) AS IS_REFERENCED_BY_COUNT
         -- Select minimum date from list
         , LEAST(
            AIRFLOW.UDF_TO_DATE_FROM_CROSSREF_DATE_PARTS(
                    PARSE_JSON(JSON_EXTRACT(JSON, '$.posted')))
        , AIRFLOW.UDF_TO_DATE_FROM_CROSSREF_DATE_PARTS(
                    PARSE_JSON(JSON_EXTRACT(JSON, '$.accepted')))
        , AIRFLOW.UDF_TO_DATE_FROM_CROSSREF_DATE_PARTS(
                    PARSE_JSON(JSON_EXTRACT(JSON, '$.submitted')))
        , AIRFLOW.UDF_TO_DATE_FROM_CROSSREF_DATE_PARTS(
                    PARSE_JSON(JSON_EXTRACT(JSON, '$.reviewed')))
        , AIRFLOW.UDF_TO_DATE_FROM_CROSSREF_DATE_PARTS(
                    PARSE_JSON(JSON_EXTRACT(JSON, '$.assertion')))
        , AIRFLOW.UDF_TO_DATE_FROM_CROSSREF_DATE_PARTS(
                    PARSE_JSON(JSON_EXTRACT(JSON, '$.published')))
        , AIRFLOW.UDF_TO_DATE_FROM_CROSSREF_DATE_PARTS(
                    PARSE_JSON(JSON_EXTRACT(JSON, '$.indexed'))))                    AS ARTICLE_EST_PUBLISH_DT
         , AIRFLOW.UDF_TO_DATE_FROM_CROSSREF_DATE_PARTS(
            PARSE_JSON(JSON_EXTRACT(JSON, '$.indexed')))                             AS INDEXED_DT
    FROM TEMP_REF_CROSSREF_HISTORIC_ARTICLE
             LEFT JOIN UNNEST(JSON_EXTRACT_ARRAY(JSON, '$.author')) AS AUTHOR WITH OFFSET AS AUTHOR_INDEX ON TRUE
             LEFT JOIN UNNEST(JSON_EXTRACT_ARRAY(AUTHOR, '$.affiliation')) AS AFFILIATION
                       ON TRUE
    WHERE JSON IS NOT NULL;

    CREATE OR REPLACE TEMP TABLE TEMP_CROSSREF_HISTORIC_ARTICLE_PROCESSED AS
    SELECT *,
           -- Add surrogate IDs
           IF(AUTHOR_FULL_NAME = '', 'n/a', AIRFLOW.UDF_MD5_HASH([AUTHOR_FULL_NAME, AUTHOR_ORCID_ID])) AS AUTHOR_SID,
           AIRFLOW.UDF_MD5_HASH([ARTICLE_DOI])                                                         AS ARTICLE_SID,
           AIRFLOW.UDF_GET_EUTOPIA_INSTITUTION_SID(ORIGINAL_AFFILIATION_NAME)                          AS INSTITUTION_SID,
           AIRFLOW.UDF_IS_EUTOPIA_AFFILIATED_STRING(ORIGINAL_AFFILIATION_NAME)                         AS IS_EUTOPIA_AFFILIATED_INSTITUTION
    FROM TEMP_CROSSREF_DOI_METADATA_PARSED;

    -- Insert data into target table
    INSERT INTO AIRFLOW.CROSSREF_HISTORIC_ARTICLE_PROCESSED
    (ARTICLE_DOI,
     AUTHOR_INDEX,
     AUTHOR_FULL_NAME,
     AUTHOR_ORCID_ID,
     ORIGINAL_AFFILIATION_NAME,
     ARTICLE_URL,
     ARTICLE_FUNDER,
     ARTICLE_INSTITUTION,
     ARTICLE_PUBLISHER,
     ARTICLE_TITLE,
     ARTICLE_SHORT_TITLE,
     ARTICLE_SUBTITLE,
     ARTICLE_ORIGINAL_TITLE,
     ARTICLE_CONTAINER_TITLE,
     ARTICLE_SHORT_CONTAINER_TITLE,
     ARTICLE_ABSTRACT,
     ARTICLE_REFERENCE,
     IS_REFERENCED_BY_COUNT,
     ARTICLE_EST_PUBLISH_DT,
     INDEXED_DT,
     AUTHOR_SID,
     ARTICLE_SID,
     INSTITUTION_SID,
     IS_EUTOPIA_AFFILIATED_INSTITUTION)
    SELECT ARTICLE_DOI,
           AUTHOR_INDEX,
           AUTHOR_FULL_NAME,
           AUTHOR_ORCID_ID,
           ORIGINAL_AFFILIATION_NAME,
           ARTICLE_URL,
           ARTICLE_FUNDER,
           ARTICLE_INSTITUTION,
           ARTICLE_PUBLISHER,
           ARTICLE_TITLE,
           ARTICLE_SHORT_TITLE,
           ARTICLE_SUBTITLE,
           ARTICLE_ORIGINAL_TITLE,
           ARTICLE_CONTAINER_TITLE,
           ARTICLE_SHORT_CONTAINER_TITLE,
           ARTICLE_ABSTRACT,
           ARTICLE_REFERENCE,
           IS_REFERENCED_BY_COUNT,
           ARTICLE_EST_PUBLISH_DT,
           INDEXED_DT,
           AUTHOR_SID,
           ARTICLE_SID,
           INSTITUTION_SID,
           IS_EUTOPIA_AFFILIATED_INSTITUTION
    FROM TEMP_CROSSREF_HISTORIC_ARTICLE_PROCESSED;
END;