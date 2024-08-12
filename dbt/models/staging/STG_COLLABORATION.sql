WITH REF_STG_CROSSREF_HISTORIC_ARTICLE AS (SELECT *
                                           FROM {{ ref('STG_CROSSREF_HISTORIC_ARTICLE_FINAL') }})
   , REF_INT_ORCID_EUTOPIA_AFFILIATION_BY_DATE AS (SELECT * FROM {{ ref('STG_ORCID_EUTOPIA_AFFILIATION_BY_DATE') }})
   , COLLABORATION_WITH_INSTITUTION AS (SELECT DISTINCT M.ARTICLE_SID
                                                      , M.AUTHOR_SID
                                                      , COALESCE(M.INSTITUTION_SID, O_AD.INSTITUTION_SID,
                                                                 IF(M.ORIGINAL_AFFILIATION_NAME IS NULL, 'n/a', 'OTHER')) AS
                                                                                                                             INSTITUTION_SID
                                                      , M.ARTICLE_EST_PUBLISH_DT                                          AS ARTICLE_PUBLICATION_DT
                                                      , M.IS_EUTOPIA_AFFILIATED_INSTITUTION
                                                      , M.AUTHOR_INDEX
                                                      , M.IS_REFERENCED_BY_COUNT
                                        FROM REF_STG_CROSSREF_HISTORIC_ARTICLE M
                                                 LEFT JOIN REF_INT_ORCID_EUTOPIA_AFFILIATION_BY_DATE O_AD
                                                           ON O_AD.AUTHOR_SID = M.AUTHOR_SID
                                                               AND O_AD.MONTH_DT =
                                                                   DATE_TRUNC(M.ARTICLE_EST_PUBLISH_DT, MONTH))
   , COLLABORATION_WITH_FLAGS
    AS (SELECT DISTINCT M.ARTICLE_SID
                      , M.AUTHOR_SID
                      , M.INSTITUTION_SID
                      , M.ARTICLE_PUBLICATION_DT
                      -- We define a sole author publication as an article with only one author
                      , COUNT(DISTINCT M.AUTHOR_SID) OVER (PARTITION BY M.ARTICLE_SID) = 1
                                                                                                     AS IS_SOLE_AUTHOR_PUBLICATION
                      -- We define an internal collaboration as an article where all authors are from the same institution
                      , COUNT(DISTINCT M.INSTITUTION_SID)
                              OVER (PARTITION BY M.ARTICLE_SID) = 1 AND
                        COUNT(DISTINCT M.AUTHOR_SID)
                              OVER (PARTITION BY M.ARTICLE_SID) >
                        1                                                                            AS IS_INTERNAL_COLLABORATION
                      -- We define an external collaboration as an article where authors are from at least two different institutions
                      , COUNT(DISTINCT M.INSTITUTION_SID)
                              OVER (PARTITION BY M.ARTICLE_SID) > 1 AND
                        COUNT(DISTINCT M.AUTHOR_SID)
                              OVER (PARTITION BY M.ARTICLE_SID) >
                        1                                                                            AS IS_EXTERNAL_COLLABORATION
                      -- We define a publication a EUTOPIA collaboration, when authors from two or more EUTOPIA institutions collaborate.
                      , COUNT(DISTINCT IF(M.IS_EUTOPIA_AFFILIATED_INSTITUTION, M.INSTITUTION_SID, NULL))
                              OVER (PARTITION BY M.ARTICLE_SID) > 1 AND
                        COUNT(DISTINCT M.AUTHOR_SID)
                              OVER (PARTITION BY M.ARTICLE_SID) >
                        1                                                                            AS IS_EUTOPIAN_COLLABORATION
                      -- We define an EUTOPIA-an article as an article with at least one author affiliated with a EUTOPIA university
                      , COUNT(DISTINCT IF(M.IS_EUTOPIA_AFFILIATED_INSTITUTION, M.AUTHOR_SID, NULL))
                              OVER (PARTITION BY M.ARTICLE_SID) >
                        0                                                                            AS IS_EUTOPIAN_PUBLICATION
                      , COUNT(DISTINCT IF(M.AUTHOR_INDEX = 0, M.AUTHOR_SID, NULL))
                              OVER (PARTITION BY M.ARTICLE_SID, M.INSTITUTION_SID, M.AUTHOR_SID) > 0 AS IS_FIRST_AUTHOR
                      -- We define the number of previously published articles by the author to determine author experience
                      -- We only consider articles that were published before the current article
                      , M.IS_REFERENCED_BY_COUNT                                                     AS IS_REFERENCED_BY_COUNT
        FROM COLLABORATION_WITH_INSTITUTION M)
SELECT ARTICLE_SID,
       AUTHOR_SID,
       INSTITUTION_SID,
       ARTICLE_PUBLICATION_DT,
       IS_SOLE_AUTHOR_PUBLICATION,
       IS_INTERNAL_COLLABORATION,
       IS_EXTERNAL_COLLABORATION,
       IS_EUTOPIAN_COLLABORATION,
       IS_EUTOPIAN_PUBLICATION,
       IS_FIRST_AUTHOR,
       IS_REFERENCED_BY_COUNT
FROM COLLABORATION_WITH_FLAGS
WHERE IS_EUTOPIAN_PUBLICATION

