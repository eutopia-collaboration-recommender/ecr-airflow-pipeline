WITH REF_COLLABORATION_NOVELTY_INDEX AS (SELECT *
                                         FROM {{ source('ANALYTICS', 'COLLABORATION_NOVELTY_INDEX') }})
   , COLLABORATION AS (SELECT DISTINCT ARTICLE_SID,
                                       INSTITUTION_SID,
                                       IS_SOLE_AUTHOR_PUBLICATION,
                                       ARTICLE_PUBLICATION_DT
                       FROM {{ ref('FCT_COLLABORATION') }})
SELECT CN.ARTICLE_SID,
       CN.COLLABORATION_NOVELTY_INDEX,
       C.INSTITUTION_SID,
       C.ARTICLE_PUBLICATION_DT,
       AIRFLOW.UDF_MD5_HASH([CN.ARTICLE_SID, C.INSTITUTION_SID]) AS PK_COLLABORATION_NOVELTY
FROM REF_COLLABORATION_NOVELTY_INDEX CN
         INNER JOIN COLLABORATION C USING (ARTICLE_SID)
WHERE NOT C.IS_SOLE_AUTHOR_PUBLICATION
GROUP BY 1, 2, 3, 4