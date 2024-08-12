CREATE OR REPLACE FUNCTION AIRFLOW.UDF_TO_DATE_NEAREST(year STRING, month STRING, day STRING)
    RETURNS DATE
    LANGUAGE js AS """
    function transformDate(year, month, day) {
        if (year == null) {
            return null;
        }
        if (month == null) {
            return new Date(Date.UTC(year));
        }
        if (day == null) {
            return new Date(Date.UTC(year, month - 1));
        }
        return new Date(Date.UTC(year, month - 1, day));
    }
    return transformDate(year, month, day);
""";