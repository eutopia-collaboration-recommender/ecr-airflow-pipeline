WITH REF_STG_CROSSREF_HISTORIC_ARTICLE_PARSED AS (SELECT *
                                                  FROM {{ ref('STG_CROSSREF_HISTORIC_ARTICLE_PARSED') }})
SELECT *,
       -- Add surrogate IDs
       CASE
           WHEN AUTHOR_FULL_NAME = '' THEN 'n/a'
           WHEN AUTHOR_ORCID_ID IS NOT NULL AND AUTHOR_ORCID_ID <> 'n/a' THEN AIRFLOW.UDF_MD5_HASH([AUTHOR_ORCID_ID])
           ELSE AIRFLOW.UDF_MD5_HASH([AUTHOR_FULL_NAME])
           END                                                             AS AUTHOR_SID,
       AIRFLOW.UDF_MD5_HASH([ARTICLE_DOI])                                 AS ARTICLE_SID,
       AIRFLOW.UDF_GET_EUTOPIA_INSTITUTION_SID(ORIGINAL_AFFILIATION_NAME)  AS INSTITUTION_SID,
       AIRFLOW.UDF_IS_EUTOPIA_AFFILIATED_STRING(ORIGINAL_AFFILIATION_NAME) AS IS_EUTOPIA_AFFILIATED_INSTITUTION
FROM REF_STG_CROSSREF_HISTORIC_ARTICLE_PARSED