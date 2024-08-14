WITH REF_ER_COLLABORATION AS (SELECT *
                              FROM {{ ref('ER_COLLABORATION')}}),
     REF_INT_COLLABORATION_NOVELTY_AUTHOR_COLLABORATION AS (SELECT DISTINCT ARTICLE_SID,
                                                                            AUTHOR_SID
                                                            FROM {{ ref('INT_COLLABORATION_NOVELTY_AUTHOR_COLLABORATION')}}
                                                            WHERE PRIOR_COLLABORATION_COUNT = 0),
     REF_INT_COLLABORATION_NOVELTY_INSTITUTION_COLLABORATION AS (SELECT DISTINCT ARTICLE_SID,
                                                                                 INSTITUTION_SID
                                                                 FROM {{ ref('INT_COLLABORATION_NOVELTY_INSTITUTION_COLLABORATION')}}
                                                                 WHERE PRIOR_COLLABORATION_COUNT = 0)
SELECT C.ARTICLE_SID,
       C.AUTHOR_SID,
       C.INSTITUTION_SID,
       C.ARTICLE_PUBLICATION_DT,
       A.AUTHOR_SID IS NOT NULL      AS IS_NEW_AUTHOR_COLLABORATION,
       I.INSTITUTION_SID IS NOT NULL AS IS_NEW_INSTITUTION_COLLABORATION
FROM REF_ER_COLLABORATION C
         LEFT JOIN REF_INT_COLLABORATION_NOVELTY_AUTHOR_COLLABORATION A
                   ON C.ARTICLE_SID = A.ARTICLE_SID
                       AND C.AUTHOR_SID = A.AUTHOR_SID
         LEFT JOIN REF_INT_COLLABORATION_NOVELTY_INSTITUTION_COLLABORATION I
                   ON C.ARTICLE_SID = I.ARTICLE_SID
                       AND C.INSTITUTION_SID = I.INSTITUTION_SID