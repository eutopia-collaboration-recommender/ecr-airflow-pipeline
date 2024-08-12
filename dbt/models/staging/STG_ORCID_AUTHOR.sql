WITH REF_MEMBER AS (SELECT *
                    FROM {{ ref("STG_ORCID_HISTORIC_AUTHOR") }}
                    UNION ALL
                    SELECT *
                    FROM {{ ref("STG_ORCID_API_AUTHOR") }})
SELECT DISTINCT MEMBER_ORCID_ID,
                MEMBER_GIVEN_NAME,
                MEMBER_FAMILY_NAME,
                CONCAT(MEMBER_GIVEN_NAME, ' ', MEMBER_FAMILY_NAME) AS FULL_NAME,
                MEMBER_URL,
                MEMBER_LOCALE,
                MEMBER_LAST_MODIFIED_DT,
                MEMBER_CREATION_METHOD,
                IS_MEMBER_VERIFIED,
                MEMBER_KEYWORDS
FROM REF_MEMBER