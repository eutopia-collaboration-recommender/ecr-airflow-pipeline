WITH REF_DIM_ARTICLE AS (SELECT * FROM {{ref('DIM_ARTICLE')}} WHERE EXTRACT(YEAR FROM ARTICLE_PUBLICATION_DT) >= 2000)
   , REF_STG_COLLABORATION AS (SELECT DISTINCT ARTICLE_SID,
                                               AUTHOR_SID,
                                               INSTITUTION_SID,
                                               ARTICLE_PUBLICATION_DT,
                                               IS_FIRST_AUTHOR,
                                               IS_EUTOPIAN_COLLABORATION,
                                               IS_EXTERNAL_COLLABORATION,
                                               IS_INTERNAL_COLLABORATION,
                                               IS_SOLE_AUTHOR_PUBLICATION
                               FROM {{ref('STG_COLLABORATION')}})
   , REF_STG_COLLABORATION_NOVELTY_INDEX
    AS (SELECT DISTINCT ARTICLE_SID,
                        COLLABORATION_NOVELTY_INDEX
        FROM {{ref('INT_ARTICLE_COLLABORATION_NOVELTY_INDEX')}})
   , REF_INT_ARTICLE_CITATION
    AS (SELECT ARTICLE_SID,
               SUM(ARTICLE_CITATION_COUNT) AS ARTICLE_CITATION_COUNT
        FROM {{ref('INT_ARTICLE_CITATION')}}
        GROUP BY ARTICLE_SID)
   , REF_INT_ARTICLE_NORMALIZED_CITATION
    AS (SELECT ARTICLE_SID,
               SUM(ARTICLE_CITATION_COUNT) AS ARTICLE_CITATION_COUNT
        FROM {{ref('INT_ARTICLE_NORMALIZED_CITATION')}}
        GROUP BY ARTICLE_SID)
   , REF_INT_AUTHOR_CITATION AS (SELECT AUTHOR_SID,
                                        ARTICLE_SID,
                                        SUM(AUTHOR_CITATION_COUNT) AS AUTHOR_CITATION_COUNT
                                 FROM {{ref('INT_ARTICLE_CITATION')}}
                                 GROUP BY AUTHOR_SID, ARTICLE_SID)
   , REF_INT_AUTHOR_NORMALIZED_CITATION AS (SELECT AUTHOR_SID,
                                                   ARTICLE_SID,
                                                   SUM(AUTHOR_CITATION_COUNT) AS AUTHOR_CITATION_COUNT
                                            FROM {{ref('INT_ARTICLE_NORMALIZED_CITATION')}}
                                            GROUP BY AUTHOR_SID, ARTICLE_SID)
   , REF_STG_COLLABORATION_DURATION
    AS (SELECT DISTINCT ARTICLE_SID,
                        PAIRWISE_COLLABORATION_DURATION
        FROM {{ref('INT_ARTICLE_COLLABORATION_DURATION')}})
   , REF_STG_COLLABORATION_CONTINUATION
    AS (SELECT DISTINCT ARTICLE_SID,
                        PAIRWISE_COLLABORATION_CONTINUATION
        FROM {{ref('INT_ARTICLE_COLLABORATION_CONTINUATION')}})
   , REF_INT_ARTICLE_FUTURE_COLLABORATION_IMPACT
    AS (SELECT ARTICLE_SID,
               NEW_COLLABORATION_IMPACT_COUNT
        FROM {{ref('INT_ARTICLE_FUTURE_COLLABORATION_IMPACT')}})
   , REF_INT_AUTHOR_EXPERIENCE
    AS (SELECT AUTHOR_SID,
               ARTICLE_SID,
               PREVIOUS_ARTICLE_COUNT,
               PREVIOUS_EUTOPIA_ARTICLE_COUNT
        FROM {{ref('INT_AUTHOR_EXPERIENCE')}})
   , REF_INT_AUTHOR_COLLABORATIVENESS AS (SELECT AUTHOR_SID,
                                                 AVG(AUTHOR_COLLABORATION_RATE) AS AUTHOR_COLLABORATION_RATE
                                          FROM {{ref('INT_AUTHOR_COLLABORATIVENESS')}}
                                          GROUP BY AUTHOR_SID)
   , REF_STG_COLLABORATION_NOVELTY AS (SELECT DISTINCT ARTICLE_SID,
                                                       AUTHOR_SID,
                                                       INSTITUTION_SID,
                                                       IS_NEW_AUTHOR_COLLABORATION,
                                                       IS_NEW_INSTITUTION_COLLABORATION
                                       FROM {{ref('INT_ARTICLE_COLLABORATION_NOVELTY')}})
   , LEAD_AUTHOR_COLLABORATIVENESS AS (SELECT C.ARTICLE_SID,
                                              AC.AUTHOR_COLLABORATION_RATE
                                       FROM REF_STG_COLLABORATION C
                                                INNER JOIN REF_INT_AUTHOR_COLLABORATIVENESS AC USING (AUTHOR_SID)
                                       WHERE C.IS_FIRST_AUTHOR)
   , LEAD_AUTHOR_EXPERIENCE AS (SELECT C.ARTICLE_SID,
                                       AE.PREVIOUS_ARTICLE_COUNT,
                                       AE.PREVIOUS_EUTOPIA_ARTICLE_COUNT
                                FROM REF_STG_COLLABORATION C
                                         INNER JOIN REF_INT_AUTHOR_EXPERIENCE AE USING (AUTHOR_SID, ARTICLE_SID)
                                WHERE C.IS_FIRST_AUTHOR)
   , AVG_AUTHOR_EXPERIENCE AS (SELECT ARTICLE_SID,
                                      AVG(PREVIOUS_ARTICLE_COUNT)         AS AVG_PREVIOUS_ARTICLE_COUNT,
                                      AVG(PREVIOUS_EUTOPIA_ARTICLE_COUNT) AS AVG_PREVIOUS_EUTOPIA_ARTICLE_COUNT
                               FROM REF_INT_AUTHOR_EXPERIENCE
                               GROUP BY ARTICLE_SID)
   , COLLABORATION_METADATA AS (SELECT DISTINCT ARTICLE_SID,
                                                COUNT(DISTINCT IF(AUTHOR_SID <> 'n/a', AUTHOR_SID, NULL))           AS AUTHOR_COUNT,
                                                COUNT(DISTINCT IF(INSTITUTION_SID <> 'n/a', INSTITUTION_SID, NULL)) AS INSTITUTION_COUNT,
                                                IS_EUTOPIAN_COLLABORATION,
                                                IS_EXTERNAL_COLLABORATION,
                                                IS_INTERNAL_COLLABORATION,
                                                IS_SOLE_AUTHOR_PUBLICATION
                                FROM REF_STG_COLLABORATION
                                GROUP BY ARTICLE_SID, IS_EUTOPIAN_COLLABORATION, IS_EXTERNAL_COLLABORATION,
                                         IS_INTERNAL_COLLABORATION, IS_SOLE_AUTHOR_PUBLICATION)
   , COLLABORATION_NOVELTY AS (SELECT C.ARTICLE_SID
                                    , LOGICAL_OR(IF(C.INSTITUTION_SID <> 'n/a'
                                                        AND C.AUTHOR_SID <> 'n/a'
                                                        AND NOT C.IS_SOLE_AUTHOR_PUBLICATION
                                                        AND EXTRACT(YEAR FROM C.ARTICLE_PUBLICATION_DT) >= 2000,
                                                    CN.IS_NEW_AUTHOR_COLLABORATION,
                                                    FALSE)) AS IS_NEW_AUTHOR_COLLABORATION
                                    , LOGICAL_OR(IF(C.INSTITUTION_SID <> 'n/a'
                                                        AND C.AUTHOR_SID <> 'n/a'
                                                        AND NOT C.IS_SOLE_AUTHOR_PUBLICATION
                                                        AND EXTRACT(YEAR FROM C.ARTICLE_PUBLICATION_DT) >= 2000,
                                                    CN.IS_NEW_INSTITUTION_COLLABORATION,
                                                    FALSE)) AS IS_NEW_INSTITUTION_COLLABORATION
                               FROM REF_STG_COLLABORATION C
                                        LEFT JOIN REF_STG_COLLABORATION_NOVELTY CN
                                                  USING (ARTICLE_SID, AUTHOR_SID, INSTITUTION_SID)
                               GROUP BY ARTICLE_SID)
   , AVG_CITATION_BY_AUTHOR AS (SELECT ARTICLE_SID
                                     , SAFE_DIVIDE(AVG(AUTHOR_CITATION_COUNT), COUNT(DISTINCT AUTHOR_SID)) AS AVG_ARTICLE_CITATION_COUNT
                                FROM REF_INT_AUTHOR_CITATION
                                GROUP BY ARTICLE_SID)
   , AVG_NORMALIZED_CITATION_BY_AUTHOR AS (SELECT ARTICLE_SID
                                                , SAFE_DIVIDE(AVG(AUTHOR_CITATION_COUNT), COUNT(DISTINCT AUTHOR_SID)) AS AVG_ARTICLE_CITATION_COUNT
                                           FROM REF_INT_AUTHOR_NORMALIZED_CITATION
                                           GROUP BY ARTICLE_SID)
SELECT DISTINCT A.ARTICLE_SID
              , A.ARTICLE_PUBLICATION_DT
              ------------------------------------------------------------------------------------------------------
              ------------------- FEATURES -------------------------------------------------------------------------
              ------------------------------------------------------------------------------------------------------
              -- With the following features, we will try to predict the value of the collaboration.
              -- We assume the collaboration value depends on the following factors:
              ------------------------------------------------------------------------------------------------------
              -- 1. Collaboration novelty index based on the number of new author pairs, articles and institutions within the collaboration.
              , IF(NOT A.IS_SOLE_AUTHOR_PUBLICATION AND IACNI.COLLABORATION_NOVELTY_INDEX IS NOT NULL,
                   IACNI.COLLABORATION_NOVELTY_INDEX, 0)            AS COLLABORATION_NOVELTY_INDEX
              -- 2. Pairwise collaboration duration in number of articles published together per pair of authors
              , IFNULL(IACD.PAIRWISE_COLLABORATION_DURATION, 0)     AS PAIRWISE_COLLABORATION_DURATION
              -- 3. Lead author collaborativeness rate
              , IFNULL(LAC.AUTHOR_COLLABORATION_RATE, 0)            AS LEAD_AUTHOR_COLLABORATION_RATE
              -- 4. Lead author experience
              , IFNULL(LAE.PREVIOUS_ARTICLE_COUNT, 0)               AS LEAD_AUTHOR_PREVIOUS_ARTICLE_COUNT
              , IFNULL(LAE.PREVIOUS_EUTOPIA_ARTICLE_COUNT, 0)       AS LEAD_AUTHOR_PREVIOUS_EUTOPIA_ARTICLE_COUNT
              -- 5. Average author experience
              , IFNULL(AAE.AVG_PREVIOUS_ARTICLE_COUNT, 0)           AS AVG_PREVIOUS_ARTICLE_COUNT
              , IFNULL(AAE.AVG_PREVIOUS_EUTOPIA_ARTICLE_COUNT, 0)   AS AVG_PREVIOUS_EUTOPIA_ARTICLE_COUNT
              -- 6. Average citation count by author
              , IFNULL(ACBA.AVG_ARTICLE_CITATION_COUNT, 0)          AS AVG_ARTICLE_CITATION_COUNT
              , IFNULL(ANCBA.AVG_ARTICLE_CITATION_COUNT, 0)         AS AVG_NORMALIZED_ARTICLE_CITATION_COUNT
              -- 7. Add flags whether the collaboration is new in terms of authors and institutions
              , CN.IS_NEW_AUTHOR_COLLABORATION
              , CN.IS_NEW_INSTITUTION_COLLABORATION
              -- 8. Add some common collaboration properties as features
              , CM.AUTHOR_COUNT
              , CM.INSTITUTION_COUNT
              , CM.IS_SOLE_AUTHOR_PUBLICATION
              , CM.IS_INTERNAL_COLLABORATION
              , CM.IS_EXTERNAL_COLLABORATION
              , CM.IS_EUTOPIAN_COLLABORATION
              ------------------------------------------------------------------------------------------------------
              -- TODO: Add the average number of articles published on research topic (per author) as a feature
              -- TODO: Add the keyword trend impact as a feature (Google Trends)
              -- TODO Something similar to the Silhouette score for the collaboration, i.e. how well the authors fit together topic-wise.
              ------------------------------------------------------------------------------------------------------
              ------------------- TARGET VARIABLES -----------------------------------------------------------------
              ------------------------------------------------------------------------------------------------------
              -- We will estimate collaboration value using the following target variables:
              ------------------------------------------------------------------------------------------------------
              -- 1. Article citation count as a publication success metric
              , IFNULL(IAC.ARTICLE_CITATION_COUNT, 0)               AS ARTICLE_CITATION_COUNT
              -- 2. Normalized article citation count as a publication success metric
              , IFNULL(IANC.ARTICLE_CITATION_COUNT, 0)              AS NORMALIZED_ARTICLE_CITATION_COUNT
              -- 3. New collaboration impact proxy based on the articles citing this article, i.e. how many articles are influenced by this article.
              , IFNULL(IAFCI.NEW_COLLABORATION_IMPACT_COUNT, 0)     AS NEW_COLLABORATION_IMPACT_COUNT
              -- 4. Length of the collaboration (from publication onwards) as we want to promote long-term collaborations.
              , IFNULL(IACC.PAIRWISE_COLLABORATION_CONTINUATION, 0) AS PAIRWISE_COLLABORATION_CONTINUATION
              -- TODO: Influence on research direction. Collaborations that significantly influence the contextual direction of the research group are more valuable.
              ------------------------------------------------------------------------------------------------------
              ------------------- PRIMARY KEY ----------------------------------------------------------------------
              ------------------------------------------------------------------------------------------------------
              , AIRFLOW.UDF_MD5_HASH([A.ARTICLE_SID])               AS PK_ARTICLE
FROM REF_DIM_ARTICLE A
         LEFT JOIN REF_STG_COLLABORATION_NOVELTY_INDEX IACNI USING (ARTICLE_SID)
         LEFT JOIN REF_INT_ARTICLE_CITATION IAC USING (ARTICLE_SID)
         LEFT JOIN REF_INT_ARTICLE_NORMALIZED_CITATION IANC USING (ARTICLE_SID)
         LEFT JOIN REF_STG_COLLABORATION_DURATION IACD USING (ARTICLE_SID)
         LEFT JOIN REF_INT_ARTICLE_FUTURE_COLLABORATION_IMPACT IAFCI USING (ARTICLE_SID)
         LEFT JOIN REF_STG_COLLABORATION_CONTINUATION IACC USING (ARTICLE_SID)
         LEFT JOIN COLLABORATION_METADATA CM USING (ARTICLE_SID)
         LEFT JOIN LEAD_AUTHOR_COLLABORATIVENESS LAC USING (ARTICLE_SID)
         LEFT JOIN LEAD_AUTHOR_EXPERIENCE LAE USING (ARTICLE_SID)
         LEFT JOIN AVG_AUTHOR_EXPERIENCE AAE USING (ARTICLE_SID)
         LEFT JOIN AVG_CITATION_BY_AUTHOR ACBA USING (ARTICLE_SID)
         LEFT JOIN AVG_NORMALIZED_CITATION_BY_AUTHOR ANCBA USING (ARTICLE_SID)
         LEFT JOIN COLLABORATION_NOVELTY CN USING (ARTICLE_SID)


