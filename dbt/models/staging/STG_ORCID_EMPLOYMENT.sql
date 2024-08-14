WITH REF_SUMMARY_BY_EMPLOYMENT AS (SELECT *
                                   FROM {{ ref("STG_ORCID_HISTORIC_EMPLOYMENT") }}
                                   UNION ALL
                                   SELECT *
                                   FROM {{ ref("STG_ORCID_API_EMPLOYMENT") }})
   , ORCID_EMPLOYMENT
    AS (SELECT DISTINCT MEMBER_ORCID_ID
                      , AIRFLOW.UDF_MD5_HASH([EMPLOYMENT_ORGANIZATION_NAME, EMPLOYMENT_DEPARTMENT_NAME, EMPLOYMENT_ROLE_TITLE, EMPLOYMENT_ORGANIZATION_CITY, EMPLOYMENT_ORGANIZATION_REGION, EMPLOYMENT_ORGANIZATION_COUNTRY, EMPLOYMENT_ORGANIZATION_URL]) AS EMPLOYMENT_SID
                      , EMPLOYMENT_DEPARTMENT_NAME
                      , EMPLOYMENT_ROLE_TITLE
                      , AIRFLOW.UDF_TO_DATE_NEAREST(EMPLOYMENT_START_YEAR,
                                                    EMPLOYMENT_START_MONTH,
                                                    EMPLOYMENT_START_DAY)                                                                                                                                                                                   AS EMPLOYMENT_START_DT
                      , AIRFLOW.UDF_TO_DATE_NEAREST(EMPLOYMENT_END_YEAR,
                                                    EMPLOYMENT_END_MONTH,
                                                    EMPLOYMENT_END_DAY)                                                                                                                                                                                     AS EMPLOYMENT_END_DT
                      , EMPLOYMENT_ORGANIZATION_NAME
                      , EMPLOYMENT_ORGANIZATION_CITY
                      , EMPLOYMENT_ORGANIZATION_REGION
                      , EMPLOYMENT_ORGANIZATION_COUNTRY
                      , EMPLOYMENT_ORGANIZATION_URL
        FROM REF_SUMMARY_BY_EMPLOYMENT)
   , ORCID_EMPLOYMENT_WITH_SETTING AS (SELECT E.MEMBER_ORCID_ID
                                            , E.EMPLOYMENT_SID
                                            , E.EMPLOYMENT_ORGANIZATION_NAME
                                            , E.EMPLOYMENT_DEPARTMENT_NAME
                                            , E.EMPLOYMENT_ROLE_TITLE
                                            , E.EMPLOYMENT_ORGANIZATION_CITY
                                            , E.EMPLOYMENT_ORGANIZATION_REGION
                                            , E.EMPLOYMENT_START_DT
                                            , E.EMPLOYMENT_END_DT
                                            , FORMAT('%s,%s', IFNULL(E.EMPLOYMENT_ORGANIZATION_NAME, ''),
                                                     IFNULL(E.EMPLOYMENT_DEPARTMENT_NAME, '')
                                              ) AS EMPLOYMENT_ORGANIZATION_SETTING
                                       FROM ORCID_EMPLOYMENT E
                                       WHERE EMPLOYMENT_SID <> TO_HEX(MD5('')))
   , ORCID_EMPLOYMENT_WITH_PROCESSED_UNIVERSITY AS (SELECT E.MEMBER_ORCID_ID
                                                         , E.EMPLOYMENT_SID
                                                         , E.EMPLOYMENT_ORGANIZATION_NAME
                                                         , E.EMPLOYMENT_DEPARTMENT_NAME
                                                         , E.EMPLOYMENT_ROLE_TITLE
                                                         , E.EMPLOYMENT_ORGANIZATION_CITY
                                                         , E.EMPLOYMENT_ORGANIZATION_REGION
                                                         , E.EMPLOYMENT_START_DT
                                                         , E.EMPLOYMENT_END_DT
                                                         , AIRFLOW.UDF_GET_EUTOPIA_INSTITUTION_SID(E.EMPLOYMENT_ORGANIZATION_SETTING)  AS INSTITUTION_SID
                                                         , AIRFLOW.UDF_IS_EUTOPIA_AFFILIATED_STRING(E.EMPLOYMENT_ORGANIZATION_SETTING) AS IS_EUTOPIA_AFFILIATED_INSTITUTION
                                                    FROM ORCID_EMPLOYMENT_WITH_SETTING E)
SELECT DISTINCT E.MEMBER_ORCID_ID AS ORCID_ID
              , E.EMPLOYMENT_SID
              , E.INSTITUTION_SID
              , E.EMPLOYMENT_ORGANIZATION_NAME
              , E.EMPLOYMENT_DEPARTMENT_NAME
              , E.EMPLOYMENT_ROLE_TITLE
              , E.EMPLOYMENT_ORGANIZATION_REGION
              , E.EMPLOYMENT_ORGANIZATION_CITY
              , E.EMPLOYMENT_START_DT
              , E.EMPLOYMENT_END_DT
              , E.IS_EUTOPIA_AFFILIATED_INSTITUTION
FROM ORCID_EMPLOYMENT_WITH_PROCESSED_UNIVERSITY E