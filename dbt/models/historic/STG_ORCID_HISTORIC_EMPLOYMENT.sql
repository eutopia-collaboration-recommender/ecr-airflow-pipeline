WITH SOURCE_TABLE
    AS (SELECT ORCID_ID
             , JSON
        FROM {{ source('AIRFLOW', 'ORCID_HISTORIC_AUTHOR') }})
   , EMPLOYMENT_JSON
    AS (SELECT JSON_EXTRACT_SCALAR(JSON, '$.record:record.common:orcid-identifier.common:path') AS ORCID_MEMBER_ID
             , JSON
             , IFNULL(
                AFFILIATION,
                JSON_EXTRACT(
                        JSON,
                        '$.record:record.activities:activities-summary.activities:employments.activities:affiliation-group'
                )
               )                                                                                AS AFFILIATION_JSON
             , IFNULL(
                JSON_EXTRACT(AFFILIATION, '$.employment:employment-summary'),
                JSON_EXTRACT(
                        JSON,
                        '$.record:record.activities:activities-summary.activities:employments.activities:affiliation-group.employment:employment-summary'
                )
               )                                                                                AS AFFILIATION_SUMMARY_JSON
        FROM SOURCE_TABLE
                 LEFT JOIN
             UNNEST(
                     JSON_EXTRACT_ARRAY(
                             JSON_EXTRACT(JSON
                                 ,
                                          '$.record:record.activities:activities-summary.activities:employments.activities:affiliation-group'
                             )
                     )
             ) AFFILIATION ON TRUE)
   , SUMMARY_BY_EMPLOYMENT
    AS (SELECT JSON_EXTRACT_SCALAR(JSON, '$.record:record.common:orcid-identifier.common:path') AS MEMBER_ORCID_ID
             , JSON_EXTRACT_SCALAR(AFFILIATION_SUMMARY_JSON,
                                   '$.common:department-name')                                  AS EMPLOYMENT_DEPARTMENT_NAME
             , JSON_EXTRACT_SCALAR(AFFILIATION_SUMMARY_JSON, '$.common:role-title')             AS EMPLOYMENT_ROLE_TITLE
             , JSON_EXTRACT_SCALAR(AFFILIATION_SUMMARY_JSON,
                                   '$.common:start-date.common:year')                           AS EMPLOYMENT_START_YEAR
             , JSON_EXTRACT_SCALAR(AFFILIATION_SUMMARY_JSON,
                                   '$.common:start-date.common:month')                          AS EMPLOYMENT_START_MONTH
             , JSON_EXTRACT_SCALAR(AFFILIATION_SUMMARY_JSON,
                                   '$.common:start-date.common:day')                            AS EMPLOYMENT_START_DAY
             , JSON_EXTRACT_SCALAR(AFFILIATION_SUMMARY_JSON, '$.common:end-date.common:year')   AS EMPLOYMENT_END_YEAR
             , JSON_EXTRACT_SCALAR(AFFILIATION_SUMMARY_JSON,
                                   '$.common:end-date.common:month')                            AS EMPLOYMENT_END_MONTH
             , JSON_EXTRACT_SCALAR(AFFILIATION_SUMMARY_JSON, '$.common:end-date.common:day')    AS EMPLOYMENT_END_DAY
             , JSON_EXTRACT_SCALAR(AFFILIATION_SUMMARY_JSON,
                                   '$.common:organization.common:name')                         AS EMPLOYMENT_ORGANIZATION_NAME
             , JSON_EXTRACT_SCALAR(AFFILIATION_SUMMARY_JSON, '$.common:organization.common:address.common:city'
               )                                                                                AS EMPLOYMENT_ORGANIZATION_CITY
             , JSON_EXTRACT_SCALAR(AFFILIATION_SUMMARY_JSON, '$.common:organization.common:address.common:region'
               )                                                                                AS EMPLOYMENT_ORGANIZATION_REGION
             , JSON_EXTRACT_SCALAR(AFFILIATION_SUMMARY_JSON, '$.common:organization.common:address.common:country'
               )                                                                                AS EMPLOYMENT_ORGANIZATION_COUNTRY
             , JSON_EXTRACT_SCALAR(AFFILIATION_SUMMARY_JSON, '$.common:url')                    AS EMPLOYMENT_ORGANIZATION_URL
        FROM EMPLOYMENT_JSON)
SELECT MEMBER_ORCID_ID,
       EMPLOYMENT_DEPARTMENT_NAME,
       EMPLOYMENT_ROLE_TITLE,
       EMPLOYMENT_START_YEAR,
       EMPLOYMENT_START_MONTH,
       EMPLOYMENT_START_DAY,
       EMPLOYMENT_END_YEAR,
       EMPLOYMENT_END_MONTH,
       EMPLOYMENT_END_DAY,
       EMPLOYMENT_ORGANIZATION_NAME,
       EMPLOYMENT_ORGANIZATION_CITY,
       EMPLOYMENT_ORGANIZATION_REGION,
       EMPLOYMENT_ORGANIZATION_COUNTRY,
       EMPLOYMENT_ORGANIZATION_URL
FROM SUMMARY_BY_EMPLOYMENT
