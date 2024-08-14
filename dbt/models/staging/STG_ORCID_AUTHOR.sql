WITH REF_MEMBER AS (SELECT *
                    FROM {{ ref("STG_ORCID_HISTORIC_AUTHOR") }}
                    UNION ALL
                    SELECT *
                    FROM {{ ref("STG_ORCID_API_AUTHOR") }})
SELECT DISTINCT MEMBER_ORCID_ID                                    AS ORCID_ID,
                MEMBER_GIVEN_NAME                                  AS ORCID_AUTHOR_GIVEN_NAME,
                MEMBER_FAMILY_NAME                                 AS ORCID_AUTHOR_FAMILY_NAME,
                CONCAT(MEMBER_GIVEN_NAME, ' ', MEMBER_FAMILY_NAME) AS ORCID_AUTHOR_FULL_NAME,
                MEMBER_URL                                         AS ORCID_AUTHOR_URL,
                MEMBER_LOCALE                                      AS ORCID_AUTHOR_LOCALE,
                MEMBER_LAST_MODIFIED_DT                            AS ORCID_AUTHOR_LAST_MODIFIED_DT,
                MEMBER_CREATION_METHOD                             AS ORCID_AUTHOR_CREATION_METHOD,
                IS_MEMBER_VERIFIED                                 AS IS_ORCID_AUTHOR_VERIFIED,
                MEMBER_KEYWORDS                                    AS ORCID_AUTHOR_KEYWORDS
FROM REF_MEMBER