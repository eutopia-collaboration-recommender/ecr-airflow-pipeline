WITH REF_SUMMARY_BY_ARTICLE
         AS (SELECT *
             FROM {{ ref('STG_ORCID_HISTORIC_ARTICLE') }}
             UNION ALL
             SELECT *
             FROM {{ ref('STG_ORCID_API_ARTICLE') }}),
     ARTICLE
         AS (SELECT MEMBER_ORCID_ID
                  -- Lower because Crossref stores DOIs in lower case and DOI is case-insensitive
                  , LOWER({{ extract_doi_from_url('ARTICLE_DOI') }}) AS ARTICLE_DOI
                  , ARTICLE_LAST_MODIFIED_DT                         AS ARTICLE_LAST_MODIFIED_DT
                  , ARTICLE_TITLE                                    AS ARTICLE_TITLE
                  , ARTICLE_PUBLICATION_YEAR                         AS ARTICLE_PUBLICATION_YEAR
                  , ARTICLE_PUBLICATION_MONTH                        AS ARTICLE_PUBLICATION_MONTH
                  , ARTICLE_PUBLICATION_DAY                          AS ARTICLE_PUBLICATION_DAY
                  , ARTICLE_TYPE                                     AS ARTICLE_TYPE
                  , ARTICLE_URL                                      AS ARTICLE_URL
                  , ARTICLE_JOURNAL_TITLE                            AS ARTICLE_JOURNAL_TITLE
             FROM REF_SUMMARY_BY_ARTICLE)
SELECT DISTINCT MEMBER_ORCID_ID                                      AS ORCID_ID
              , ARTICLE_DOI
              , ARTICLE_TITLE
              , AIRFLOW.UDF_TO_DATE_NEAREST(ARTICLE_PUBLICATION_YEAR,
                                            ARTICLE_PUBLICATION_MONTH,
                                            ARTICLE_PUBLICATION_DAY) AS ARTICLE_PUBLICATION_DT
              , ARTICLE_TYPE
              , ARTICLE_URL
              , ARTICLE_JOURNAL_TITLE
              , ARTICLE_LAST_MODIFIED_DT
FROM ARTICLE
WHERE ARTICLE_DOI IS NOT NULL