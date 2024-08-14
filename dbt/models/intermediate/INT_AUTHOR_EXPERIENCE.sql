/*
Author experience is defined as the number of articles published by the author prior to current publication. Since we
mainly focus on the author's experience in the EUTOPIA project, we will calculate the number of EUTOPIA publications
published by the author prior to the current publication. And then for EUTOPIA authors, we will calculate the number of
total publications published by the author prior to the current publication from the ORCID dataset.

NOTE: author experience for external authors is not calculated properly in this model as we do not have the complete
dataset of all the articles published by external authors that only collaborate with EUTOPIA authors.
*/
WITH REF_STG_CROSSREF_HISTORIC_ARTICLE AS (SELECT DISTINCT AUTHOR_SID, ARTICLE_SID, ARTICLE_PUBLICATION_DT
                                           FROM {{ ref("ER_COLLABORATION") }}),
     REF_STG_CROSSREF_AUTHOR AS (SELECT DISTINCT AUTHOR_SID,
                                                 AUTHOR_ORCID_ID
                                 FROM {{ ref("STG_CROSSREF_AUTHOR") }}),
     REF_STG_ORCID_ARTICLE AS (SELECT A.AUTHOR_SID,
                                      O.ARTICLE_DOI,
                                      O.ARTICLE_PUBLICATION_DT
                               FROM {{ref('STG_ORCID_ARTICLE')}} O
                                        LEFT JOIN REF_STG_CROSSREF_AUTHOR A
                                                  ON O.ORCID_ID = A.AUTHOR_ORCID_ID),
     -- Calculate the number of EUTOPIA articles published by the author prior to the current publication
     INT_AUTHOR_EXPERIENCE_CROSSREF AS (SELECT A.AUTHOR_SID,
                                               A.ARTICLE_SID,
                                               A.ARTICLE_PUBLICATION_DT,
                                               COUNT(A.ARTICLE_SID)
                                                     OVER (PARTITION BY A.AUTHOR_SID ORDER BY A.ARTICLE_PUBLICATION_DT ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS PREVIOUS_EUTOPIA_ARTICLE_COUNT
                                        FROM REF_STG_CROSSREF_HISTORIC_ARTICLE A),
     -- Calculate the number of total articles published by the author prior to the current publication
     -- Mainly focus on EUTOPIA authors
     INT_AUTHOR_EXPERIENCE AS (SELECT AE.AUTHOR_SID,
                                      AE.ARTICLE_SID,
                                      AE.PREVIOUS_EUTOPIA_ARTICLE_COUNT,
                                      COUNT(DISTINCT O.ARTICLE_DOI) AS PREVIOUS_ARTICLE_COUNT
                               FROM INT_AUTHOR_EXPERIENCE_CROSSREF AE
                                        LEFT JOIN REF_STG_ORCID_ARTICLE O
                                                  ON AE.AUTHOR_SID = O.AUTHOR_SID
                                                      AND AE.ARTICLE_PUBLICATION_DT > O.ARTICLE_PUBLICATION_DT
                               GROUP BY AE.AUTHOR_SID, AE.ARTICLE_SID, AE.PREVIOUS_EUTOPIA_ARTICLE_COUNT)
SELECT AUTHOR_SID,
       ARTICLE_SID,
       PREVIOUS_ARTICLE_COUNT,
       PREVIOUS_EUTOPIA_ARTICLE_COUNT
FROM INT_AUTHOR_EXPERIENCE