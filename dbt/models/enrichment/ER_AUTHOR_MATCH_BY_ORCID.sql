WITH REF_STG_COLLABORATION AS (SELECT *
                               FROM {{ ref('STG_COLLABORATION') }})
   , REF_STG_CROSSREF_AUTHOR AS (SELECT *
                                 FROM {{ ref('STG_CROSSREF_AUTHOR') }})
   , REF_STG_ORCID_AUTHOR AS (SELECT *
                              FROM {{ ref('STG_ORCID_AUTHOR') }})
   , SRC_AUTHOR_MATCH_CANDIDATE_PAIR AS (SELECT *
                                         FROM {{ source('ANALYTICS', 'AUTHOR_MATCH_CANDIDATE_PAIR_BY_ORCID') }})
   , AUTHOR_MATCH_BY_ORCID AS (SELECT DISTINCT AM.CROSSREF_AUTHOR_SID,
                                               AIRFLOW.UDF_MD5_HASH([AM.ARTICLE_DOI]) AS ARTICLE_SID,
                                               AM.ORCID_ID,
                                               AM.ARTICLE_COUNT,
                                               AM.LEVENSHTEIN_DISTANCE,
                                               C.AUTHOR_FULL_NAME,
                                               O.ORCID_AUTHOR_FULL_NAME
                               FROM SRC_AUTHOR_MATCH_CANDIDATE_PAIR AM
                                        INNER JOIN REF_STG_CROSSREF_AUTHOR C
                                                   ON AM.CROSSREF_AUTHOR_SID = C.AUTHOR_SID
                                        INNER JOIN REF_STG_ORCID_AUTHOR O
                                                   ON AM.ORCID_ID = O.ORCID_ID
                               WHERE
                                 -- If Crossref ORCID ID and member ID from ORCID are the same,
                                 -- we say that our data is consistent and does not need changes.
                                   (CROSSREF_ORCID_ID IS NULL)
                                 -- Check if the initials of one author is a subset of initials of the other author.
                                 AND AIRFLOW.IS_AUTHOR_INITIALS_SUBSET(O.ORCID_AUTHOR_FULL_NAME,
                                                                       C.AUTHOR_FULL_NAME)
                               QUALIFY
                                   RANK() OVER (PARTITION BY AM.CROSSREF_AUTHOR_SID, AM.ARTICLE_DOI ORDER BY AM.LEVENSHTEIN_DISTANCE ASC, AM.ARTICLE_COUNT DESC, O.ORCID_AUTHOR_LAST_MODIFIED_DT DESC) =
                                   1)
SELECT DISTINCT BASE.ARTICLE_SID                   AS ARTICLE_SID,
                BASE.AUTHOR_SID                    AS OLD_AUTHOR_SID,
                AIRFLOW.UDF_MD5_HASH([O.ORCID_ID]) AS NEW_AUTHOR_SID,
                O.ORCID_ID
FROM REF_STG_COLLABORATION BASE
         LEFT JOIN AUTHOR_MATCH_BY_ORCID O
                   ON BASE.AUTHOR_SID = O.CROSSREF_AUTHOR_SID
                       AND BASE.ARTICLE_SID = O.ARTICLE_SID
