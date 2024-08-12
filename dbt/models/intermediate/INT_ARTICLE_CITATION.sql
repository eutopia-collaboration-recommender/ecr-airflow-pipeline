WITH REF_STG_COLLABORATION AS (SELECT *
                                       FROM {{ ref("STG_COLLABORATION") }}),
     DISTINCT_INSTITUTION_CITATIONS AS (SELECT DISTINCT ARTICLE_SID
                                                      , INSTITUTION_SID
                                                      , FIRST_VALUE(AUTHOR_SID)
                                                                    OVER
                                                                        (PARTITION BY ARTICLE_SID, INSTITUTION_SID ORDER BY IS_REFERENCED_BY_COUNT DESC) AS AUTHOR_SID
                                                      , MAX(IS_REFERENCED_BY_COUNT)
                                                            OVER (PARTITION BY ARTICLE_SID, INSTITUTION_SID ORDER BY IS_REFERENCED_BY_COUNT DESC)        AS INSTITUTION_CITATION_COUNT
                                        FROM REF_STG_COLLABORATION)
SELECT C.AUTHOR_SID,
       C.ARTICLE_SID,
       C.INSTITUTION_SID,
       -- The number of times the article has been cited by other articles. We only fill this
       -- field if the author is the first author of the article so that we do not double count the citations.
       IF(C.IS_FIRST_AUTHOR, C.IS_REFERENCED_BY_COUNT, 0) AS ARTICLE_CITATION_COUNT,
       -- The number of times the institution has been cited by other articles. We only fill
       -- this field once per institution per article so that we do not double count the citations.
       IFNULL(D.INSTITUTION_CITATION_COUNT, 0)            AS INSTITUTION_CITATION_COUNT,
       -- The number of times the author has been cited by other articles. This field is always filled.
       IFNULL(C.IS_REFERENCED_BY_COUNT, 0)                AS AUTHOR_CITATION_COUNT
FROM REF_STG_COLLABORATION C
         LEFT JOIN DISTINCT_INSTITUTION_CITATIONS D
                   ON C.ARTICLE_SID = D.ARTICLE_SID
                       AND C.INSTITUTION_SID = D.INSTITUTION_SID
                       AND C.AUTHOR_SID = D.AUTHOR_SID
WHERE IS_EUTOPIAN_PUBLICATION