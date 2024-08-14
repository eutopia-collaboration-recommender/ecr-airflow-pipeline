WITH REF_STG_COLLABORATION AS (SELECT *
                               FROM {{ ref('STG_COLLABORATION') }})
   , REF_ER_AUTHOR_MATCH_BY_ORCID AS (SELECT *
                                      FROM {{ ref('ER_AUTHOR_MATCH_BY_ORCID') }})
   , REF_ER_AUTHOR_MATCH_BY_NEIGHBORHOOD AS (SELECT *
                                             FROM {{ ref('ER_AUTHOR_MATCH_BY_NEIGHBORHOOD') }})
   , REF_ER_ORCID_EUTOPIA_AFFILIATION_BY_DATE AS (SELECT *
                                                  FROM {{ ref('ER_ORCID_EUTOPIA_AFFILIATION_BY_DATE') }})
   , ENRICHED_AUTHORS AS (SELECT BASE.ARTICLE_SID,
                                 COALESCE(O.NEW_AUTHOR_SID, N.NEW_AUTHOR_SID, BASE.AUTHOR_SID) AS AUTHOR_SID,
                                 BASE.INSTITUTION_SID,
                                 BASE.ARTICLE_PUBLICATION_DT,
                                 BASE.IS_SOLE_AUTHOR_PUBLICATION,
                                 BASE.IS_INTERNAL_COLLABORATION,
                                 BASE.IS_EXTERNAL_COLLABORATION,
                                 BASE.IS_EUTOPIAN_COLLABORATION,
                                 BASE.IS_EUTOPIAN_PUBLICATION,
                                 BASE.IS_FIRST_AUTHOR,
                                 BASE.IS_REFERENCED_BY_COUNT,
                          FROM REF_STG_COLLABORATION BASE
                                   LEFT JOIN REF_ER_AUTHOR_MATCH_BY_ORCID O
                                             ON BASE.AUTHOR_SID = O.OLD_AUTHOR_SID
                                                 AND BASE.ARTICLE_SID = O.ARTICLE_SID
                                   LEFT JOIN REF_ER_AUTHOR_MATCH_BY_NEIGHBORHOOD N
                                             ON BASE.AUTHOR_SID = N.OLD_AUTHOR_SID)
   , ENRICHED_INSTITUTIONS AS (SELECT BASE.ARTICLE_SID
                                    , BASE.AUTHOR_SID
                                    , IF(
            BASE.INSTITUTION_SID IN ('n/a', 'OTHER') AND O_AD.INSTITUTION_SID IS NOT NULL AND
            O_AD.INSTITUTION_SID NOT IN ('n/a', 'OTHER'), O_AD.INSTITUTION_SID,
            BASE.INSTITUTION_SID) AS INSTITUTION_SID
                                    , BASE.ARTICLE_PUBLICATION_DT
                                    , BASE.IS_SOLE_AUTHOR_PUBLICATION
                                    , BASE.IS_INTERNAL_COLLABORATION
                                    , BASE.IS_EXTERNAL_COLLABORATION
                                    , BASE.IS_EUTOPIAN_COLLABORATION
                                    , BASE.IS_EUTOPIAN_PUBLICATION
                                    , BASE.IS_FIRST_AUTHOR
                                    , BASE.IS_REFERENCED_BY_COUNT
                               FROM ENRICHED_AUTHORS BASE
                                        LEFT JOIN REF_ER_ORCID_EUTOPIA_AFFILIATION_BY_DATE O_AD
                                                  ON O_AD.AUTHOR_SID = BASE.AUTHOR_SID
                                                      AND
                                                     O_AD.MONTH_DT = DATE_TRUNC(BASE.ARTICLE_PUBLICATION_DT, MONTH))
SELECT ARTICLE_SID,
       AUTHOR_SID,
       INSTITUTION_SID,
       ARTICLE_PUBLICATION_DT,
       IS_SOLE_AUTHOR_PUBLICATION,
       IS_INTERNAL_COLLABORATION,
       IS_EXTERNAL_COLLABORATION,
       IS_EUTOPIAN_COLLABORATION,
       IS_EUTOPIAN_PUBLICATION,
       IS_REFERENCED_BY_COUNT,
       -- In some cases, we merge authors that are actually different people and if these people are first authors,
       -- we get errors downstream. Since this is a rare case, we decide to ignore it.
       LOGICAL_OR(IS_FIRST_AUTHOR) AS IS_FIRST_AUTHOR,
FROM ENRICHED_INSTITUTIONS
GROUP BY ALL

