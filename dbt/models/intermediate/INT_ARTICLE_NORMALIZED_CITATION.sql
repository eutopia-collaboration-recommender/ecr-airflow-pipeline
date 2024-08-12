WITH REF_INT_ARTICLE_RESEARCH_AREA AS (SELECT RESEARCH_AREA_SID,
                                              ARTICLE_SID
                                       FROM {{ ref('INT_ARTICLE_RESEARCH_AREA')}}),
     REF_INT_ARTICLE_CITATION AS (SELECT ARTICLE_SID,
                                         AUTHOR_SID,
                                         INSTITUTION_SID,
                                         ARTICLE_CITATION_COUNT,
                                         INSTITUTION_CITATION_COUNT,
                                         AUTHOR_CITATION_COUNT
                                  FROM {{ ref('INT_ARTICLE_CITATION')}}),
     ARTICLE_COUNT_BY_TOPIC AS (SELECT RESEARCH_AREA_SID,
                                       COUNT(DISTINCT ARTICLE_SID) AS ARTICLE_COUNT
                                FROM REF_INT_ARTICLE_RESEARCH_AREA
                                GROUP BY RESEARCH_AREA_SID),
     NORMALIZATION_FACTOR_BY_ARTICLE AS (SELECT ARTICLE_SID,
                                                1 / ARTICLE_COUNT AS NORMALIZATION_FACTOR
                                         FROM REF_INT_ARTICLE_RESEARCH_AREA A
                                                  INNER JOIN ARTICLE_COUNT_BY_TOPIC C
                                                             ON A.RESEARCH_AREA_SID = C.RESEARCH_AREA_SID)
SELECT C.ARTICLE_SID,
       C.AUTHOR_SID,
       C.INSTITUTION_SID,
       C.ARTICLE_CITATION_COUNT * N.NORMALIZATION_FACTOR     AS ARTICLE_CITATION_COUNT,
       C.INSTITUTION_CITATION_COUNT * N.NORMALIZATION_FACTOR AS INSTITUTION_CITATION_COUNT,
       C.AUTHOR_CITATION_COUNT * N.NORMALIZATION_FACTOR      AS AUTHOR_CITATION_COUNT
FROM REF_INT_ARTICLE_CITATION C
         INNER JOIN NORMALIZATION_FACTOR_BY_ARTICLE N
                    ON C.ARTICLE_SID = N.ARTICLE_SID
