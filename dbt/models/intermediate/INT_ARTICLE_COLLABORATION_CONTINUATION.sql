/*
Collaboration continuation is defined as the maximum number of articles published by the same pair of authors after
the current article. It is used to measure how long after the current article the authors continue to collaborate.
*/
WITH REF_STG_COLLABORATION AS (SELECT ARTICLE_SID,
                                              AUTHOR_SID,
                                              ARTICLE_PUBLICATION_DT
                                       FROM {{ ref("STG_COLLABORATION") }}
                                       WHERE IS_EUTOPIAN_PUBLICATION),
     AUTHOR_COUNT AS (SELECT ARTICLE_SID,
                             COUNT(DISTINCT AUTHOR_SID) AS AUTHOR_COUNT
                      FROM REF_STG_COLLABORATION
                      GROUP BY ARTICLE_SID),
     AUTHOR_PAIRS AS (SELECT A1.ARTICLE_SID,
                             A1.AUTHOR_SID AS AUTHOR_SID_1,
                             A2.AUTHOR_SID AS AUTHOR_SID_2,
                             A1.ARTICLE_PUBLICATION_DT
                      FROM REF_STG_COLLABORATION A1
                               INNER JOIN REF_STG_COLLABORATION A2 ON A1.ARTICLE_SID = A2.ARTICLE_SID
                      WHERE A1.AUTHOR_SID <> A2.AUTHOR_SID),
     COLLABORATION_LENGTH AS (SELECT AP1.AUTHOR_SID_1,
                                     AP1.AUTHOR_SID_2,
                                     AP1.ARTICLE_SID,
                                     COUNT(DISTINCT AP2.ARTICLE_SID) AS PAIRWISE_COLLABORATION_LENGTH
                              FROM AUTHOR_PAIRS AP1
                                       INNER JOIN AUTHOR_PAIRS AP2
                                                  ON AP1.AUTHOR_SID_1 = AP2.AUTHOR_SID_1
                                                      AND AP1.AUTHOR_SID_2 = AP2.AUTHOR_SID_2
                                                      -- Only consider collaborations where the article AP2 was published AFTER AP1
                                                      AND AP2.ARTICLE_PUBLICATION_DT > AP1.ARTICLE_PUBLICATION_DT
                              GROUP BY AP1.AUTHOR_SID_1, AP1.AUTHOR_SID_2, AP1.ARTICLE_SID)
SELECT AP.ARTICLE_SID,
       -- Collaboration continuation is defined as the maximum number of articles published by the same pair of authors
       -- after the current article. With this we measure if any of the authors continue to collaborate after the current
       -- article.
       MAX(CL.PAIRWISE_COLLABORATION_LENGTH) AS PAIRWISE_COLLABORATION_CONTINUATION
FROM AUTHOR_PAIRS AP
         INNER JOIN COLLABORATION_LENGTH CL
                    ON AP.AUTHOR_SID_1 = CL.AUTHOR_SID_1
                        AND AP.AUTHOR_SID_2 = CL.AUTHOR_SID_2
                        AND AP.ARTICLE_SID = CL.ARTICLE_SID
         INNER JOIN AUTHOR_COUNT AC
                    ON AP.ARTICLE_SID = AC.ARTICLE_SID
GROUP BY AP.ARTICLE_SID, AC.AUTHOR_COUNT