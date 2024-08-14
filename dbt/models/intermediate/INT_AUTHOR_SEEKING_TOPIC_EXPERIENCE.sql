WITH REF_ER_COLLABORATION AS (SELECT * FROM {{ ref("ER_COLLABORATION") }})
   , REF_INT_ARTICLE_RESEARCH_AREA AS (SELECT * FROM {{ ref("INT_ARTICLE_RESEARCH_AREA") }})
   , ARTICLE_COLLABORATION_WITH_TOPIC AS (SELECT C.ARTICLE_SID,
                                                 C.AUTHOR_SID,
                                                 T.RESEARCH_AREA_SID,
                                                 C.ARTICLE_PUBLICATION_DT
                                          FROM REF_ER_COLLABORATION C
                                                   LEFT JOIN REF_INT_ARTICLE_RESEARCH_AREA T USING (ARTICLE_SID)
                                          WHERE NOT C.IS_SOLE_AUTHOR_PUBLICATION)
   , ARTICLE_RESEARCH_AREA_EXPERIENCE_BY_AUTHOR_AND_ARTICLE
    AS (SELECT C.AUTHOR_SID,
               C.RESEARCH_AREA_SID,
               C.ARTICLE_SID,
               COUNT(DISTINCT C2.ARTICLE_SID) AS ARTICLE_COUNT -- Experience proxy
        FROM ARTICLE_COLLABORATION_WITH_TOPIC C
                 LEFT JOIN ARTICLE_COLLABORATION_WITH_TOPIC C2
                           ON C.AUTHOR_SID = C2.AUTHOR_SID
                               AND C.RESEARCH_AREA_SID = C2.RESEARCH_AREA_SID
                               AND C.ARTICLE_PUBLICATION_DT > C2.ARTICLE_PUBLICATION_DT
        GROUP BY C.AUTHOR_SID, C.RESEARCH_AREA_SID, C.ARTICLE_SID)
   , ARTICLE_RESEARCH_AREA_EXPERIENCE_THRESHOLD AS (SELECT ARTICLE_SID,
                                                           RESEARCH_AREA_SID,
                                                           AVG(ARTICLE_COUNT)    AS AVG_ARTICLE_COUNT,
                                                           STDDEV(ARTICLE_COUNT) AS STD_ARTICLE_COUNT
                                                    FROM ARTICLE_RESEARCH_AREA_EXPERIENCE_BY_AUTHOR_AND_ARTICLE
                                                    GROUP BY ARTICLE_SID, RESEARCH_AREA_SID)
SELECT DISTINCT C.ARTICLE_SID,
                C.AUTHOR_SID,
                A.ARTICLE_COUNT                                                 AS AUTHOR_TOPIC_EXPERIENCE,
                T.AVG_ARTICLE_COUNT                                             AS AVG_AUTHOR_TOPIC_EXPERIENCE,
                T.STD_ARTICLE_COUNT                                             AS STD_AUTHOR_TOPIC_EXPERIENCE,
                A.ARTICLE_COUNT < T.AVG_ARTICLE_COUNT - 2 * T.STD_ARTICLE_COUNT AS IS_AUTHOR_SEEKING_TOPIC_EXPERIENCE
FROM ARTICLE_COLLABORATION_WITH_TOPIC C
         INNER JOIN ARTICLE_RESEARCH_AREA_EXPERIENCE_BY_AUTHOR_AND_ARTICLE A
                    USING (ARTICLE_SID, AUTHOR_SID, RESEARCH_AREA_SID)
         LEFT JOIN ARTICLE_RESEARCH_AREA_EXPERIENCE_THRESHOLD T USING (ARTICLE_SID, RESEARCH_AREA_SID)
