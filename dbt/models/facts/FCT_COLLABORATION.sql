WITH REF_STG_COLLABORATION AS (SELECT * FROM {{ ref("ER_COLLABORATION") }})
   , REF_STG_COLLABORATION_DURATION AS (SELECT * FROM {{ ref("INT_ARTICLE_COLLABORATION_DURATION") }})
   , REF_INT_ARTICLE_CITATION AS (SELECT * FROM {{ ref("INT_ARTICLE_CITATION") }})
   , REF_INT_ARTICLE_NORMALIZED_CITATION AS (SELECT * FROM {{ ref("INT_ARTICLE_NORMALIZED_CITATION") }})
   , REF_INT_ARTICLE_RESEARCH_AREA AS (SELECT * FROM {{ ref("INT_ARTICLE_RESEARCH_AREA") }})
   , REF_STG_COLLABORATION_NOVELTY AS (SELECT * FROM {{ ref("INT_ARTICLE_COLLABORATION_NOVELTY") }})
   , REF_INT_AUTHOR_EXPERIENCE AS (SELECT * FROM {{ ref("INT_AUTHOR_EXPERIENCE") }})
   , REF_INT_AUTHOR_SEEKING_TOPIC_EXPERIENCE AS (SELECT * FROM {{ ref("INT_AUTHOR_SEEKING_TOPIC_EXPERIENCE") }})
   , REF_DIM_ARTICLE AS (SELECT * FROM {{ ref('DIM_ARTICLE') }})
SELECT
     -- Surrogate keys
    DISTINCT C.ARTICLE_SID
     , C.AUTHOR_SID
     , C.INSTITUTION_SID
     , IFNULL(RT.RESEARCH_AREA_SID, 'n/a')                                             AS RESEARCH_AREA_SID
     , CASE
           WHEN IFNULL(P.PREVIOUS_ARTICLE_COUNT, 0) < 3 THEN AIRFLOW.UDF_MD5_HASH(['Early-career author'])
           WHEN P.PREVIOUS_ARTICLE_COUNT < 10 THEN AIRFLOW.UDF_MD5_HASH(['Mid-career author'])
           ELSE AIRFLOW.UDF_MD5_HASH(['Experienced author']) END                       AS AUTHOR_EXPERIENCE_SID
     , CASE
           WHEN C.IS_SOLE_AUTHOR_PUBLICATION THEN AIRFLOW.UDF_MD5_HASH(['Sole-author publication'])
           WHEN C.IS_INTERNAL_COLLABORATION THEN AIRFLOW.UDF_MD5_HASH(['Internal collaboration'])
           ELSE AIRFLOW.UDF_MD5_HASH(['External collaboration']) END                   AS COLLABORATION_TYPE_SID
     , CASE
           WHEN IFNULL(D.PAIRWISE_COLLABORATION_DURATION, 0) < 3
               THEN AIRFLOW.UDF_MD5_HASH(['New partnership'])
           WHEN D.PAIRWISE_COLLABORATION_DURATION < 10
               THEN AIRFLOW.UDF_MD5_HASH(['Extended collaboration'])
           ELSE AIRFLOW.UDF_MD5_HASH(['Long-term collaboration']) END                  AS COLLABORATION_DURATION_SID
     -- Date field
     , C.ARTICLE_PUBLICATION_DT
     -- Metrics
     , IFNULL(CT.
                  ARTICLE_CITATION_COUNT,
              0)                                                                       AS ARTICLE_CITATION_COUNT
     , IFNULL(CT.AUTHOR_CITATION_COUNT, 0)                                             AS AUTHOR_CITATION_COUNT
     , IFNULL(CT.INSTITUTION_CITATION_COUNT, 0)                                        AS INSTITUTION_CITATION_COUNT
     , IFNULL(N.ARTICLE_CITATION_COUNT, 0)                                             AS NORMALIZED_ARTICLE_CITATION_COUNT
     , IFNULL(N.AUTHOR_CITATION_COUNT, 0)                                              AS NORMALIZED_AUTHOR_CITATION_COUNT
     , IFNULL(N.INSTITUTION_CITATION_COUNT, 0)                                         AS NORMALIZED_INSTITUTION_CITATION_COUNT
     -- Flags
     , C.IS_SOLE_AUTHOR_PUBLICATION
     , C.IS_INTERNAL_COLLABORATION
     , C.IS_EXTERNAL_COLLABORATION
     , C.IS_EUTOPIAN_COLLABORATION
     , C.IS_EUTOPIAN_PUBLICATION
     , IF(C.INSTITUTION_SID <> 'n/a'
              AND C.AUTHOR_SID <> 'n/a'
              AND NOT C.IS_SOLE_AUTHOR_PUBLICATION,
          CN.IS_NEW_AUTHOR_COLLABORATION,
          FALSE)                                                                       AS IS_NEW_AUTHOR_COLLABORATION
     , IF(C.INSTITUTION_SID <> 'n/a'
              AND C.AUTHOR_SID <> 'n/a'
              AND C.IS_EXTERNAL_COLLABORATION,
          CN.IS_NEW_INSTITUTION_COLLABORATION,
          FALSE)                                                                       AS IS_NEW_INSTITUTION_COLLABORATION
     , IFNULL(A.IS_ARTICLE_RELEVANT, FALSE)                                            AS IS_ARTICLE_RELEVANT
     , IF(C.IS_FIRST_AUTHOR IS NULL OR C.AUTHOR_SID = 'n/a', FALSE, C.IS_FIRST_AUTHOR) AS IS_FIRST_AUTHOR
     , IFNULL(E.IS_AUTHOR_SEEKING_TOPIC_EXPERIENCE, FALSE)                             AS IS_AUTHOR_SEEKING_TOPIC_EXPERIENCE
     -- Primary key
     , AIRFLOW.UDF_MD5_HASH([C.ARTICLE_SID, C.AUTHOR_SID, C.INSTITUTION_SID])          AS PK_COLLABORATION
FROM REF_STG_COLLABORATION C
         LEFT JOIN REF_DIM_ARTICLE A USING (ARTICLE_SID)
    -- Other intermediate models
         LEFT JOIN REF_STG_COLLABORATION_DURATION D USING (ARTICLE_SID)
         LEFT JOIN REF_INT_ARTICLE_RESEARCH_AREA RT USING (ARTICLE_SID)
         LEFT JOIN REF_INT_AUTHOR_EXPERIENCE P USING (ARTICLE_SID, AUTHOR_SID)
         LEFT JOIN REF_STG_COLLABORATION_NOVELTY CN USING (ARTICLE_SID, AUTHOR_SID, INSTITUTION_SID)
         LEFT JOIN REF_INT_ARTICLE_CITATION CT USING (ARTICLE_SID, AUTHOR_SID, INSTITUTION_SID)
         LEFT JOIN REF_INT_ARTICLE_NORMALIZED_CITATION N USING (ARTICLE_SID, AUTHOR_SID, INSTITUTION_SID)
         LEFT JOIN REF_INT_AUTHOR_SEEKING_TOPIC_EXPERIENCE E USING (ARTICLE_SID, AUTHOR_SID)