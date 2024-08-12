WITH SOURCE_TABLE
    AS (SELECT ORCID_ID,
               JSON
        FROM {{ source('AIRFLOW', 'ORCID_API_AUTHOR') }})
   , API_ORCID_EMPLOYMENT_JSON AS (SELECT ORCID_ID            AS ORCID_ID
                                        , JSON
                                        , IFNULL(AFFILIATION, JSON_EXTRACT(JSON,
                                                                           '$.activities-summary.employments.affiliation-group')
                                          )                   AS AFFILIATION_JSON
                                        , AFFILIATION_SUMMARY AS AFFILIATION_SUMMARY_JSON
                                   FROM SOURCE_TABLE
                                            LEFT JOIN
                                        UNNEST(
                                                JSON_EXTRACT_ARRAY(JSON_EXTRACT(JSON,
                                                                                '$.activities-summary.employments.affiliation-group'))
                                        ) AFFILIATION ON TRUE
                                            LEFT JOIN
                                        UNNEST(
                                                JSON_EXTRACT_ARRAY(JSON_EXTRACT(AFFILIATION, '$.summaries'))
                                        ) AFFILIATION_SUMMARY ON TRUE)
   , API_ORCID_SUMMARY_BY_EMPLOYMENT
    AS (SELECT ORCID_ID
             , JSON_EXTRACT_SCALAR(AFFILIATION_SUMMARY_JSON,
                                   '$.employment-summary.department-name')              AS EMPLOYMENT_DEPARTMENT_NAME
             , JSON_EXTRACT_SCALAR(AFFILIATION_SUMMARY_JSON,
                                   '$.employment-summary.role-title')                   AS EMPLOYMENT_ROLE_TITLE
             , JSON_EXTRACT_SCALAR(AFFILIATION_SUMMARY_JSON,
                                   '$.employment-summary.start-date.year.value')        AS EMPLOYMENT_START_YEAR
             , JSON_EXTRACT_SCALAR(AFFILIATION_SUMMARY_JSON,
                                   '$.employment-summary.start-date.month.value')       AS EMPLOYMENT_START_MONTH
             , JSON_EXTRACT_SCALAR(AFFILIATION_SUMMARY_JSON,
                                   '$.employment-summary.start-date.day.value')         AS EMPLOYMENT_START_DAY
             , JSON_EXTRACT_SCALAR(AFFILIATION_SUMMARY_JSON,
                                   '$.employment-summary.end-date.year.value')          AS EMPLOYMENT_END_YEAR
             , JSON_EXTRACT_SCALAR(AFFILIATION_SUMMARY_JSON,
                                   '$.employment-summary.end-date.month.value')         AS EMPLOYMENT_END_MONTH
             , JSON_EXTRACT_SCALAR(AFFILIATION_SUMMARY_JSON,
                                   '$.employment-summary.end-date.day.value')           AS EMPLOYMENT_END_DAY
             , JSON_EXTRACT_SCALAR(AFFILIATION_SUMMARY_JSON,
                                   '$.employment-summary.organization.name')            AS EMPLOYMENT_ORGANIZATION_NAME
             , JSON_EXTRACT_SCALAR(AFFILIATION_SUMMARY_JSON,
                                   '$.employment-summary.organization.address.city')    AS EMPLOYMENT_ORGANIZATION_CITY
             , JSON_EXTRACT_SCALAR(AFFILIATION_SUMMARY_JSON,
                                   '$.employment-summary.organization.address.region')  AS EMPLOYMENT_ORGANIZATION_REGION
             , JSON_EXTRACT_SCALAR(AFFILIATION_SUMMARY_JSON,
                                   '$.employment-summary.organization.address.country') AS EMPLOYMENT_ORGANIZATION_COUNTRY
             , JSON_EXTRACT_SCALAR(AFFILIATION_SUMMARY_JSON,
                                   '$.employment-summary.url.value')                    AS EMPLOYMENT_URL
        FROM API_ORCID_EMPLOYMENT_JSON)
SELECT *
FROM API_ORCID_SUMMARY_BY_EMPLOYMENT
