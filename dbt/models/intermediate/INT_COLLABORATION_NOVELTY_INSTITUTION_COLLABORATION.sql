WITH REF_INT_COLLABORATION_NOVELTY_AUTHOR_COLLABORATION AS (SELECT *
                                                            FROM {{ ref('INT_COLLABORATION_NOVELTY_AUTHOR_COLLABORATION') }}),
     REF_ER_COLLABORATION AS (SELECT *
                              FROM {{ ref('ER_COLLABORATION') }}),
     INSTITUTION_COLLABORATION AS (SELECT DISTINCT AC.ARTICLE_SID,
                                                   AC.AUTHOR_SID,
                                                   AC.COLLABORATOR_AUTHOR_SID,
                                                   C.INSTITUTION_SID,
                                                   C2.INSTITUTION_SID AS COLLABORATOR_INSTITUTION_SID,
                                                   AC.PRIOR_COLLABORATION_COUNT
                                   FROM REF_INT_COLLABORATION_NOVELTY_AUTHOR_COLLABORATION AC
                                            INNER JOIN REF_ER_COLLABORATION C
                                                       ON C.ARTICLE_SID = AC.ARTICLE_SID
                                                           AND C.AUTHOR_SID = AC.AUTHOR_SID
                                            INNER JOIN REF_ER_COLLABORATION C2
                                                       ON AC.ARTICLE_SID = C2.ARTICLE_SID
                                                           AND AC.COLLABORATOR_AUTHOR_SID = C2.AUTHOR_SID
                                   WHERE C.INSTITUTION_SID <> C2.INSTITUTION_SID)
SELECT ARTICLE_SID,
       INSTITUTION_SID,
       COLLABORATOR_INSTITUTION_SID,
       SUM(PRIOR_COLLABORATION_COUNT) AS PRIOR_COLLABORATION_COUNT
FROM INSTITUTION_COLLABORATION
GROUP BY ALL