WITH REF_STG_CROSSREF_AUTHOR AS (SELECT * FROM {{ ref('ER_CROSSREF_AUTHOR') }}),
     REF_ANALYTICS_AUTHOR_COLLABORATIVENESS AS (SELECT * FROM {{ ref('INT_AUTHOR_COLLABORATIVENESS') }}),
     EUTOPIA_AUTHOR AS (SELECT DISTINCT AUTHOR_SID
                        FROM {{ ref('ER_COLLABORATION') }}
                        WHERE INSTITUTION_SID NOT IN ('n/a', 'OTHER'))
SELECT DISTINCT A.AUTHOR_SID
              , IFNULL(A.AUTHOR_ORCID_ID, 'n/a')                                  AS AUTHOR_ORCID_ID
              , IFNULL(AC.IS_AUTHOR_MORE_COLLABORATIVE, FALSE)                    AS IS_AUTHOR_MORE_COLLABORATIVE
              , IFNULL(EA.AUTHOR_SID IS NOT NULL, FALSE)                          AS IS_EUTOPIA_AUTHOR
              , ANY_VALUE(IF(A.AUTHOR_FULL_NAME = '', 'n/a', A.AUTHOR_FULL_NAME)) AS AUTHOR_FULL_NAME
FROM REF_STG_CROSSREF_AUTHOR A
         LEFT JOIN REF_ANALYTICS_AUTHOR_COLLABORATIVENESS AC USING (AUTHOR_SID)
         LEFT JOIN EUTOPIA_AUTHOR EA USING (AUTHOR_SID)
GROUP BY 1, 2, 3, 4