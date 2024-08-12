-- Function accepts an array of (possibly null) strings and returns a string that is the concatenation of all the strings in the array and hashed using MD5 algorithm.
CREATE OR REPLACE FUNCTION AIRFLOW.UDF_MD5_HASH(strings ARRAY <STRING>)
    RETURNS STRING
AS (
    TO_HEX(
            MD5(
                    (SELECT STRING_AGG(s, '')
                     FROM UNNEST(strings) AS s
                     WHERE s IS NOT NULL)
            )
    )
    );