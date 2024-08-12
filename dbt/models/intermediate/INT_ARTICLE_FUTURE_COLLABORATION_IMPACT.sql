/*
Future collaboration impact is measured within the EUTOPIA publication subspace and counts the number of articles
within the EUTOPIA publication subspace that have cited the current article. This is used to measure the impact of
the current article on future collaborations within the EUTOPIA publication subspace.
*/
WITH REF_STG_CROSSREF_ARTICLE_REFERENCE_METADATA AS (SELECT ARTICLE_DOI,
                                                            REFERENCE_ARTICLE_DOI,
                                                            REFERENCE_ARTICLE_ARTICLE_TITLE
                                                     FROM {{ ref( 'STG_CROSSREF_ARTICLE_REFERENCE_METADATA' ) }})
   , REF_DIM_ARTICLE AS (SELECT * FROM {{ ref( 'DIM_ARTICLE' ) }} WHERE IS_EUTOPIAN_PUBLICATION)
SELECT A.ARTICLE_SID,
       -- The number of articles within the EUTOPIA publication subspace that have cited the current article.
       COUNT(DISTINCT IFNULL(A_DOI.ARTICLE_SID, A_TITLE.ARTICLE_SID)) AS NEW_COLLABORATION_IMPACT_COUNT
FROM REF_STG_CROSSREF_ARTICLE_REFERENCE_METADATA R
         INNER JOIN REF_DIM_ARTICLE A USING (ARTICLE_DOI)
    -- We join references on both DOI and article title to account for potential missing DOIs
         LEFT JOIN REF_DIM_ARTICLE A_DOI ON R.REFERENCE_ARTICLE_DOI = A_DOI.ARTICLE_DOI
         LEFT JOIN REF_DIM_ARTICLE A_TITLE ON R.REFERENCE_ARTICLE_ARTICLE_TITLE = A_TITLE.ARTICLE_TITLE
GROUP BY A.ARTICLE_SID