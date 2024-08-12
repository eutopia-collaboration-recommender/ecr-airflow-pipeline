CREATE OR REPLACE PROCEDURE AIRFLOW.SP_ITERATE_FILL_CROSSREF_HISTORIC_ARTICLE_PROCESSED(
    start_year INT64,
    start_month INT64,
    end_year INT64,
    end_month INT64
)
BEGIN
    DECLARE current_year INT64;
    DECLARE current_month INT64;
    DECLARE max_months INT64;
    DECLARE total_months INT64;
    DECLARE month_counter INT64;

    -- Calculate the total number of months to iterate over
    SET total_months = (end_year - start_year) * 12 + (end_month - start_month + 1);

    -- Initialize current year and month
    SET current_year = start_year;
    SET current_month = start_month;
    SET month_counter = 0;

    -- Iterate over each month in the range
    WHILE month_counter < total_months DO
            -- Execute the target procedure for the current year and month
            CALL AIRFLOW.SP_FILL_CROSSREF_HISTORIC_ARTICLE_PROCESSED(current_year, current_month);

            -- Move to the next month
            SET current_month = current_month + 1;

            -- Adjust the year and month if we go past December
            IF current_month > 12 THEN
                SET current_month = 1;
                SET current_year = current_year + 1;
            END IF;

            -- Increment the month counter
            SET month_counter = month_counter + 1;
        END WHILE;
END;