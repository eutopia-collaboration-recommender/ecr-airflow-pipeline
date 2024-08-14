WITH REF_STG_CROSSREF_ARTICLE AS (SELECT * FROM {{ ref('STG_CROSSREF_ARTICLE') }}),
     REF_ANALYTICS_ARTICLE_LANGUAGE AS (SELECT * FROM {{ source('ANALYTICS', 'ARTICLE_LANGUAGE') }}),
     REF_ANALYTICS_TEXT_EMBEDDING_ARTICLE AS (SELECT * FROM {{ source('ANALYTICS', 'TEXT_EMBEDDING_ARTICLE') }}),
     REF_ER_COLLABORATION AS (SELECT ARTICLE_SID,
                                     LOGICAL_OR(IS_SOLE_AUTHOR_PUBLICATION) AS IS_SOLE_AUTHOR_PUBLICATION,
                                     LOGICAL_OR(IS_INTERNAL_COLLABORATION)  AS IS_INTERNAL_COLLABORATION,
                                     LOGICAL_OR(IS_EXTERNAL_COLLABORATION)  AS IS_EXTERNAL_COLLABORATION,
                                     LOGICAL_OR(IS_EUTOPIAN_COLLABORATION)  AS IS_EUTOPIAN_COLLABORATION,
                                     LOGICAL_OR(IS_EUTOPIAN_PUBLICATION)    AS IS_EUTOPIAN_PUBLICATION
                              FROM {{ ref("ER_COLLABORATION") }}
                              GROUP BY 1),
     COMBINED AS (SELECT DISTINCT A.ARTICLE_SID
                                , A.ARTICLE_DOI
                                , A.ARTICLE_URL
                                , IFNULL(A.ARTICLE_FUNDER, 'n/a')                AS ARTICLE_FUNDER
                                , IFNULL(A.ARTICLE_INSTITUTION, 'n/a')           AS ARTICLE_INSTITUTION
                                , IFNULL(A.ARTICLE_PUBLISHER, 'n/a')             AS ARTICLE_PUBLISHER
                                , IFNULL(A.ARTICLE_TITLE, 'n/a')                 AS ARTICLE_TITLE
                                , IFNULL(A.ARTICLE_SHORT_TITLE, 'n/a')           AS ARTICLE_SHORT_TITLE
                                , IFNULL(A.ARTICLE_SUBTITLE, 'n/a')              AS ARTICLE_SUBTITLE
                                , IFNULL(A.ARTICLE_ORIGINAL_TITLE, 'n/a')        AS ARTICLE_ORIGINAL_TITLE
                                , IFNULL(A.ARTICLE_CONTAINER_TITLE, 'n/a')       AS ARTICLE_CONTAINER_TITLE
                                , IFNULL(A.ARTICLE_SHORT_CONTAINER_TITLE, 'n/a') AS ARTICLE_SHORT_CONTAINER_TITLE
                                , IFNULL(A.ARTICLE_ABSTRACT, 'n/a')              AS ARTICLE_ABSTRACT
                                , IFNULL(A.ARTICLE_REFERENCE, 'n/a')             AS ARTICLE_REFERENCE
                                , IFNULL(L.ARTICLE_LANGUAGE, 'n/a')              AS ARTICLE_LANGUAGE
                                , A.ARTICLE_EST_PUBLISH_DT                       AS ARTICLE_PUBLICATION_DT
                                , IFNULL(L.ARTICLE_LANGUAGE = 'en', FALSE)       AS IS_ARTICLE_ENGLISH
                                , A.ARTICLE_ABSTRACT IS NOT NULL OR
                                  A.ARTICLE_REFERENCE IS NOT NULL                AS HAS_SUFFICIENT_TEXT_FOR_EMBEDDING
                                , E.DOI IS NOT NULL                              AS HAS_TEXT_EMBEDDING
                                , A.ARTICLE_ABSTRACT IS NOT NULL                 AS HAS_ABSTRACT
                                , A.ARTICLE_REFERENCE IS NOT NULL                AS HAS_REFERENCES
                                , C.IS_SOLE_AUTHOR_PUBLICATION
                                , C.IS_INTERNAL_COLLABORATION
                                , C.IS_EXTERNAL_COLLABORATION
                                , C.IS_EUTOPIAN_COLLABORATION
                                , C.IS_EUTOPIAN_PUBLICATION
                  FROM REF_STG_CROSSREF_ARTICLE A
                           LEFT JOIN REF_ANALYTICS_ARTICLE_LANGUAGE L USING (ARTICLE_DOI)
                           LEFT JOIN REF_ANALYTICS_TEXT_EMBEDDING_ARTICLE E ON E.DOI = A.ARTICLE_DOI
                           LEFT JOIN REF_ER_COLLABORATION C ON A.ARTICLE_SID = C.ARTICLE_SID)
SELECT ARTICLE_SID,
       ARTICLE_DOI,
       ARTICLE_URL,
       ARTICLE_TITLE,
       ARTICLE_LANGUAGE,
       ARTICLE_PUBLICATION_DT,
       IS_ARTICLE_ENGLISH,
       HAS_SUFFICIENT_TEXT_FOR_EMBEDDING,
       HAS_TEXT_EMBEDDING,
       HAS_ABSTRACT,
       HAS_REFERENCES,
       IS_SOLE_AUTHOR_PUBLICATION,
       IS_INTERNAL_COLLABORATION,
       IS_EXTERNAL_COLLABORATION,
       IS_EUTOPIAN_COLLABORATION,
       IS_EUTOPIAN_PUBLICATION,
       IS_ARTICLE_ENGLISH
           AND HAS_SUFFICIENT_TEXT_FOR_EMBEDDING
           AND IS_EUTOPIAN_PUBLICATION AS IS_ARTICLE_RELEVANT
FROM COMBINED
WHERE IS_EUTOPIAN_PUBLICATION
