WITH SOURCE_TABLE
         AS (SELECT ORCID_ID,
                    JSON
             FROM {{ source('DATALAKE', 'ORCID_API_AUTHOR') }})
SELECT ORCID_ID                                                   AS MEMBER_ORCID_ID
     , JSON_EXTRACT_SCALAR(
        JSON, '$.person.name.given-names.value'
       )                                                          AS MEMBER_GIVEN_NAME
     , JSON_EXTRACT_SCALAR(
        JSON, '$.person.name.family-name.value'
       )                                                          AS MEMBER_FAMILY_NAME
     , JSON_EXTRACT_SCALAR(JSON, '$.orcid-identifier.uri')        AS MEMBER_URL
     , JSON_EXTRACT_SCALAR(JSON, '$.preferences.locale')          AS MEMBER_LOCALE
     , CAST(
        TIMESTAMP_MILLIS(
                CAST(
                        JSON_EXTRACT_SCALAR(JSON, '$.history.last-modified-date.value') AS INT64
                )
        ) AS DATE
       )                                                          AS MEMBER_LAST_MODIFIED_DT
     , JSON_EXTRACT_SCALAR(
        JSON, '$.history.creation-method'
       )                                                          AS MEMBER_CREATION_METHOD
     , JSON_EXTRACT_SCALAR(JSON, '$.history.verified-email')      AS IS_MEMBER_VERIFIED
     , STRING_AGG(JSON_EXTRACT_SCALAR(KEYWORD, '$.content'), ',') AS MEMBER_KEYWORDS
FROM SOURCE_TABLE
         LEFT JOIN
     UNNEST(JSON_EXTRACT_ARRAY(JSON_EXTRACT(JSON, '$.person.keywords.keyword'))) KEYWORD ON TRUE
GROUP BY MEMBER_ORCID_ID,
         MEMBER_GIVEN_NAME,
         MEMBER_FAMILY_NAME,
         MEMBER_URL,
         MEMBER_LOCALE,
         MEMBER_LAST_MODIFIED_DT,
         MEMBER_CREATION_METHOD,
         IS_MEMBER_VERIFIED
