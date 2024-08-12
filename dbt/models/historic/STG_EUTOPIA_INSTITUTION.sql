WITH REF_STG_EUTOPIA_INSTITUTION AS (SELECT *
                                     FROM {{ source('AIRFLOW', 'EUTOPIA_INSTITUTION') }})
SELECT INSTITUTION_SID,
       INSTITUTION_NAME,
       INSTITUTION_PRETTY_NAME,
       INSTITUTION_COUNTRY,
       INSTITUTION_LANGUAGE,
       INSTITUTION_COUNTRY_ISO2
FROM REF_STG_EUTOPIA_INSTITUTION