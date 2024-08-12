WITH REF_STG_CROSSREF_DOI_METADATA AS (SELECT *
                                       FROM {{ ref('STG_CROSSREF_HISTORIC_ARTICLE_FINAL') }})
SELECT DISTINCT ARTICLE_SID
              , ARTICLE_DOI
              , JSON_EXTRACT_SCALAR(PARSE_JSON(ARTICLE_URL), '$.')                       AS ARTICLE_URL
              , JSON_EXTRACT_SCALAR(JSON_EXTRACT(ARTICLE_FUNDER, '$[0]'), '$.name')      AS ARTICLE_FUNDER
              , JSON_EXTRACT_SCALAR(JSON_EXTRACT(ARTICLE_INSTITUTION, '$[0]'), '$.name') AS ARTICLE_INSTITUTION
              , JSON_EXTRACT_SCALAR(PARSE_JSON(ARTICLE_PUBLISHER), '$.')                 AS ARTICLE_PUBLISHER
              , JSON_EXTRACT_SCALAR(JSON_EXTRACT(ARTICLE_TITLE, '$[0]'), '$.')           AS ARTICLE_TITLE
              , JSON_EXTRACT_SCALAR(JSON_EXTRACT(ARTICLE_SHORT_TITLE, '$[0]'), '$.')     AS ARTICLE_SHORT_TITLE
              , JSON_EXTRACT_SCALAR(JSON_EXTRACT(ARTICLE_SUBTITLE, '$[0]'), '$.')        AS ARTICLE_SUBTITLE
              , JSON_EXTRACT_SCALAR(JSON_EXTRACT(ARTICLE_ORIGINAL_TITLE, '$[0]'), '$.')  AS ARTICLE_ORIGINAL_TITLE
              , JSON_EXTRACT_SCALAR(JSON_EXTRACT(ARTICLE_CONTAINER_TITLE, '$[0]'),
                                    '$.')                                                AS ARTICLE_CONTAINER_TITLE
              , JSON_EXTRACT_SCALAR(JSON_EXTRACT(ARTICLE_SHORT_CONTAINER_TITLE, '$[0]'),
                                    '$.')                                                AS ARTICLE_SHORT_CONTAINER_TITLE
              , JSON_EXTRACT_SCALAR(PARSE_JSON(ARTICLE_ABSTRACT), '$.')                  AS ARTICLE_ABSTRACT
              , ARTICLE_REFERENCE
              , ARTICLE_EST_PUBLISH_DT
              , COUNT(DISTINCT IF(IS_EUTOPIA_AFFILIATED_INSTITUTION, AUTHOR_SID, NULL))
                      OVER (PARTITION BY ARTICLE_SID) >
                0                                                                        AS IS_EUTOPIA_AFFILIATED_ARTICLE
FROM REF_STG_CROSSREF_DOI_METADATA
