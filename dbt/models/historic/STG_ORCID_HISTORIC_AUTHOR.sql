WITH SOURCE_TABLE
         AS (SELECT FILEPATH
                  , JSON
             FROM {{ source('DATALAKE', 'ORCID_HISTORIC_AUTHOR') }})
SELECT JSON_EXTRACT_SCALAR(
        JSON, '$.record:record.common:orcid-identifier.common:path'
       )                                                   AS MEMBER_ORCID_ID
     , JSON_EXTRACT_SCALAR(
        JSON, '$.record:record.person:person.person:name.personal-details:given-names'
       )                                                   AS MEMBER_GIVEN_NAME
     , JSON_EXTRACT_SCALAR(
        JSON, '$.record:record.person:person.person:name.personal-details:family-name'
       )                                                   AS MEMBER_FAMILY_NAME
     , JSON_EXTRACT_SCALAR(
        JSON, '$.record:record.common:orcid-identifier.common:uri'
       )                                                   AS MEMBER_URL
     , JSON_EXTRACT_SCALAR(
        JSON, '$.record:record.preferences:preferences.preferences:locale'
       )                                                   AS MEMBER_LOCALE
     , CAST(
        CAST(
                JSON_EXTRACT_SCALAR(
                        JSON, '$.record:record.history:history.common:last-modified-date'
                ) AS TIMESTAMP
        ) AS DATE
       )                                                   AS MEMBER_LAST_MODIFIED_DT
     , JSON_EXTRACT_SCALAR(
        JSON, '$.record:record.history:history.history:creation-method'
       )                                                   AS MEMBER_CREATION_METHOD
     , JSON_EXTRACT_SCALAR(
        JSON, '$.record:record.history:history.history:verified-email'
       )                                                   AS IS_MEMBER_VERIFIED
     , STRING_AGG(JSON_EXTRACT(KEYWORD, '$.content'), ',') AS MEMBER_KEYWORDS
FROM SOURCE_TABLE
         LEFT JOIN
     UNNEST(
             JSON_EXTRACT_ARRAY(
                     JSON_EXTRACT(JSON, '$.record:record.person:person.keyword:keywords'))
     ) KEYWORD ON TRUE
GROUP BY MEMBER_ORCID_ID,
         MEMBER_GIVEN_NAME,
         MEMBER_FAMILY_NAME,
         MEMBER_URL,
         MEMBER_LOCALE,
         MEMBER_LAST_MODIFIED_DT,
         MEMBER_CREATION_METHOD,
         IS_MEMBER_VERIFIED
