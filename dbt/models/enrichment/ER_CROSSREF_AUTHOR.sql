WITH REF_STG_CROSSREF_AUTHOR AS (SELECT *
                                 FROM {{ ref('STG_CROSSREF_AUTHOR') }})
   , REF_ER_AUTHOR_MATCH_BY_ORCID AS (SELECT *
                                      FROM {{ ref('ER_AUTHOR_MATCH_BY_ORCID') }})
   , REF_ER_AUTHOR_MATCH_BY_NEIGHBORHOOD AS (SELECT *
                                             FROM {{ ref('ER_AUTHOR_MATCH_BY_NEIGHBORHOOD') }})
   , ENRICHED_AUTHORS AS (SELECT COALESCE(O.NEW_AUTHOR_SID, N.NEW_AUTHOR_SID, BASE.AUTHOR_SID) AS AUTHOR_SID,
                                 COALESCE(O.ORCID_ID, N.AUTHOR_ORCID_ID, BASE.AUTHOR_ORCID_ID) AS AUTHOR_ORCID_ID,
                                 ANY_VALUE(BASE.AUTHOR_FULL_NAME)                              AS AUTHOR_FULL_NAME,
                          FROM REF_STG_CROSSREF_AUTHOR BASE
                                   LEFT JOIN REF_ER_AUTHOR_MATCH_BY_ORCID O
                                             ON BASE.AUTHOR_SID = O.OLD_AUTHOR_SID
                                   LEFT JOIN REF_ER_AUTHOR_MATCH_BY_NEIGHBORHOOD N
                                             ON BASE.AUTHOR_SID = N.OLD_AUTHOR_SID
                          GROUP BY 1, 2)
SELECT AUTHOR_SID,
       AUTHOR_FULL_NAME,
       AUTHOR_ORCID_ID,
FROM ENRICHED_AUTHORS

