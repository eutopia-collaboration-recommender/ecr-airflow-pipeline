WITH REF_DIM_RESEARCH_AREA AS (SELECT * FROM {{ ref('DIM_RESEARCH_AREA') }})
   , REF_DIM_ARTICLE AS (SELECT * FROM {{ ref('DIM_ARTICLE') }})
   , REF_ARTICLE_RESEARCH_AREA AS (SELECT DISTINCT DOI,
                                                   RESEARCH_AREA_CODE
                                   FROM {{ source('ANALYTICS', 'ARTICLE_RESEARCH_AREA') }})
SELECT A.ARTICLE_SID,
       RT.RESEARCH_AREA_SID
FROM REF_ARTICLE_RESEARCH_AREA T
         LEFT JOIN REF_DIM_ARTICLE A
                   ON T.DOI = A.ARTICLE_DOI
         LEFT JOIN REF_DIM_RESEARCH_AREA RT
                   ON T.RESEARCH_AREA_CODE = RT.RESEARCH_AREA_CODE
