/*
FCT_AFFILIATION_BY_DATE
This view calculates the affiliation of ORCID_IDs on a monthly basis.

HAS_EUTOPIAN_AFFILIATION:
    Author has to be employed at a EUTOPIA-an affiliation in the same month or employment start date is null.

    -> Assumption: Publication happens in the following 12 months of working at a university.
    This means that the author is not necessarily working at the university at the time of publication, but has to be employed there within the prior 12 months.
    This assumption is used due to the fact that the publication comes with delay and the author might have left the university before the publication date.
    We filter author to still work here or has worked here within the 12 months before publication.

HAS_NON_EUTOPIAN_AFFILIATION:
    Author has to be employed at a non-EUTOPIA-an affiliation in the same month or employment start date is null.

    -> Assumption: Publication happens in the following 12 months of working at a university.
    This means that the author is not necessarily working at the university at the time of publication, but has to be employed there within the prior 12 months.
    This assumption is used due to the fact that the publication comes with delay and the author might have left the university before the publication date.
    We filter author to still work here or has worked here within the 12 months before publication.

*/
-- Generate dates since 1900 to current date in BigQuery
WITH DT AS (SELECT DATE_ADD(DATE '1900-01-01', INTERVAL DT_SEQ DAY) AS DT
            FROM UNNEST(GENERATE_ARRAY(0, DATE_DIFF(CURRENT_DATE(), DATE '1900-01-01', DAY))) AS DT_SEQ)
   , REF_STG_ORCID_EMPLOYMENT AS (SELECT * FROM {{ ref('STG_ORCID_EMPLOYMENT') }})
   , REF_STG_ORCID_ARTICLE AS (SELECT * FROM {{ ref('STG_ORCID_ARTICLE') }})
   , REF_STG_CROSSREF_AUTHOR AS (SELECT * FROM {{ ref('ER_CROSSREF_AUTHOR') }})
   , MIN_PUBLICATION_DT_BY_ORCID_ID AS (SELECT ORCID_ID
                                             , MIN(ARTICLE_PUBLICATION_DT) AS ARTICLE_FIRST_PUBLICATION_DT
                                        FROM REF_STG_ORCID_ARTICLE
                                        GROUP BY ORCID_ID)
-- For every date, we connect it to every ORCID_ID if:
-- 1. The date is greater than the first publication date of the ORCID_ID
-- 2. The date is less than the current date
-- We then join the ORCID_ID to the EMPLOYMENT table if:
-- 1. The date is between the EMPLOYMENT_START_DT and EMPLOYMENT_END_DT plus 12 months (due to assumption)
-- 2. The date is greater than EMPLOYMENT_START_DT and EMPLOYMENT_END_DT is NULL -> still working here
-- 3. The date is less than EMPLOYMENT_END_DT plus 12 months and EMPLOYMENT_START_DT is NULL -> worked here within the last 12 months since date
-- 4. The EMPLOYMENT_START_DT and EMPLOYMENT_END_DT are NULL -> no input, meaning that our best guess is that the author always worked here
SELECT CR_A.AUTHOR_SID
     , E.INSTITUTION_SID
     , DATE_TRUNC(D.DT, MONTH) AS MONTH_DT
FROM MIN_PUBLICATION_DT_BY_ORCID_ID A
         INNER JOIN DT D
                    ON D.DT >= A.ARTICLE_FIRST_PUBLICATION_DT
                        AND D.DT < CURRENT_DATE()
         INNER JOIN REF_STG_ORCID_EMPLOYMENT E
                    ON A.ORCID_ID = E.ORCID_ID
                        AND (
                           -- The date is between the EMPLOYMENT_START_DT and EMPLOYMENT_END_DT plus 12 months (due to assumption)
                           (D.DT BETWEEN E.EMPLOYMENT_START_DT
                                AND DATE_ADD(E.EMPLOYMENT_END_DT, INTERVAL 12 MONTH)
                               -- The date is greater than EMPLOYMENT_START_DT and EMPLOYMENT_END_DT is NULL -> still working here
                               OR (D.DT >= E.EMPLOYMENT_START_DT AND E.EMPLOYMENT_END_DT IS NULL)
                               -- The date is less than EMPLOYMENT_END_DT plus 12 months and EMPLOYMENT_START_DT is NULL -> worked here within the last 12 months since date
                               OR (D.DT <= DATE_ADD(E.EMPLOYMENT_END_DT, INTERVAL 12 MONTH)
                                   AND E.EMPLOYMENT_START_DT IS NULL)
                               -- The EMPLOYMENT_START_DT and EMPLOYMENT_END_DT are NULL -> no input, meaning that our best guess is that the author always worked here
                               OR (E.EMPLOYMENT_START_DT IS NULL AND E.EMPLOYMENT_END_DT IS NULL))
                           )
         INNER JOIN REF_STG_CROSSREF_AUTHOR CR_A
                    ON A.ORCID_ID = CR_A.AUTHOR_ORCID_ID
WHERE E.IS_EUTOPIA_AFFILIATED_INSTITUTION = TRUE
  AND CR_A.AUTHOR_ORCID_ID IS NOT NULL
GROUP BY CR_A.AUTHOR_SID
       , E.INSTITUTION_SID
       , MONTH_DT

